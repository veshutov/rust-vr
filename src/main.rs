use std::cell::RefCell;
use std::env;
use std::rc::Rc;

use anyhow::Result;
use futures::StreamExt;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::client::VrClient;
use crate::message::{Message, Request};
use crate::replica::VrReplica;

mod client;
mod message;
mod replica;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    assert!(args.len() > 1, "No app mode arg provided");
    let app_mode = args[1].as_str();
    println!("App mode: {}", app_mode);

    match app_mode {
        "client" => handle_client_mode(args).await,
        "server" => handle_server_mode(args).await,
        _ => panic!("Invalid app mode: {:?}", args[1]),
    }
}

async fn handle_client_mode(args: Vec<String>) -> Result<()> {
    assert!(args.len() > 2, "No client id arg provided");
    let client_id = args[2].as_str();
    println!("Client id: {}", client_id);

    assert!(args.len() > 3, "No replica ports arg provided");
    assert!(args.len() == 4, "Invalid arg number, only 3 args required");
    let replica_urls = args[3]
        .split(",")
        .map(|p| p.parse::<u32>().expect("Invalid replica ports arg"))
        .map(|p| format!("localhost:{}", p))
        .collect();
    println!("Replica urls: {:?}", replica_urls);

    let mut client = VrClient::new(client_id.to_owned(), replica_urls);

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            println!("Client {}: exiting...", client_id);
            return Ok(());
        }

        match client.send(line.clone()).await {
            Ok(response) => println!("Client {}: response: {:?}", client_id, response),
            Err(e) => println!("Client {}: error response: {}", client_id, e),
        }
    }
}

async fn handle_server_mode(args: Vec<String>) -> Result<()> {
    assert!(args.len() > 2, "No replica number arg provided");
    let replica_number = args[2]
        .as_str()
        .parse::<usize>()
        .expect("Invalid replica number arg");
    println!("Replica number: {}", replica_number);

    assert!(args.len() > 3, "No replica ports arg provided");
    assert!(args.len() == 4, "Invalid arg number, only 3 args required");
    let mut replica_urls: Vec<String> = args[3]
        .split(",")
        .map(|p| p.parse::<u32>().expect("Invalid replica ports arg"))
        .map(|p| format!("localhost:{}", p))
        .collect();
    replica_urls.sort();
    println!("Replica urls: {:?}", replica_urls);
    let listener = tokio::net::TcpListener::bind(format!(
        "0.0.0.0:{}",
        replica_urls
            .get(replica_number)
            .expect("Invalid replica number")
            .split(":")
            .last()
            .unwrap()
    ))
    .await?;

    let (message_tx, mut message_rx) = mpsc::unbounded_channel::<Message>();

    let replica = Rc::new(RefCell::new(VrReplica::new(
        replica_number,
        replica::ReplicaStatus::Normal,
        replica_urls.clone(),
    )));
    let replica_worker = replica.clone();

    let local = LocalSet::new();
    local
        .run_until(async move {
            tokio::task::spawn_local(async move {
                println!(
                    "Replica {}: started message handling worker",
                    replica_number
                );
                while let Some(message) = message_rx.recv().await {
                    println!("Replica {}: received {:?}", replica_number, message);
                    replica_worker.borrow_mut().process_message(message).await;
                }
            });

            println!(
                "Replica {}: started connection handling worker",
                replica_number
            );
            loop {
                let replica = replica.clone();
                let (stream, addr) = listener.accept().await.unwrap();
                let (sink, mut stream) = Framed::new(stream, LengthDelimitedCodec::new()).split();
                let sink = Rc::new(RefCell::new(sink));

                println!(
                    "Replica {}: accepted connection from {}",
                    replica_number, addr
                );

                let message_tx = message_tx.clone();
                tokio::task::spawn_local(async move {
                    while let Some(frame) = stream.next().await {
                        let bytes = frame.unwrap();
                        let json = String::from_utf8(bytes.to_vec()).unwrap();
                        let message: Request = serde_json::from_str(&json).unwrap();

                        replica
                            .borrow_mut()
                            .client_connections
                            .insert(message.client_id.clone(), sink.clone());

                        message_tx.send(Message::Request(message)).unwrap();
                    }
                });
            }
        })
        .await;
    Ok(())
}
