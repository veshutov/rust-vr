use std::env;
use std::sync::Arc;

use anyhow::Result;
use axum::{Router, extract::Json, extract::State, routing::post};
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;

use crate::client::VrClient;
use crate::replica::{ClientRequest, PrepareRequest, PrepareResponse, ServerResponse, VrReplica};

mod client;
mod replica;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    assert!(args.len() > 1, "No app mode arg provided");
    let app_mode = args[1].as_str();
    println!("App mode: {}", app_mode);

    match app_mode {
        "client" => {
            assert!(args.len() > 2, "No client id arg provided");
            let client_id = args[2].as_str();
            println!("Client id: {}", client_id);

            assert!(args.len() > 3, "No replica ports arg provided");
            let replica_urls = args[3]
                .split(",")
                .map(|p| p.parse::<u32>().expect("Invalid replica ports arg"))
                .map(|p| format!("http://localhost:{}", p))
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
                    // end of input (Ctrl+D)
                    println!("EOF, exiting...");
                    break;
                }

                match client.send(line.clone()).await {
                    Ok(_) => {}
                    Err(e) => println!("Client {}: error response: {}", client_id, e),
                }
            }
        }
        "server" => {
            assert!(args.len() > 2, "No replica number arg provided");
            let replica_number = args[2]
                .as_str()
                .parse::<usize>()
                .expect("Invalid replica number arg");
            println!("Replica number: {}", replica_number);

            assert!(args.len() > 3, "No replica ports arg provided");
            let mut replica_urls: Vec<String> = args[3]
                .split(",")
                .map(|p| p.parse::<u32>().expect("Invalid replica ports arg"))
                .map(|p| format!("http://localhost:{}", p))
                .collect();
            replica_urls.sort();
            println!("Replica urls: {:?}", replica_urls);

            let replica = replica::VrReplica::new(
                replica_urls.clone(),
                replica_number,
                replica::ReplicaStatus::Normal,
            );

            let app_state = Arc::new(AppState {
                replica: Arc::new(Mutex::new(replica)),
            });
            let app = Router::new()
                .route("/", post(process_client_request))
                .route("/prepare", post(process_prepare_request))
                .with_state(app_state);

            let listener = tokio::net::TcpListener::bind(format!(
                "0.0.0.0:{}",
                replica_urls
                    .get(replica_number)
                    .expect("Invalid replica number")
                    .split(":")
                    .last()
                    .unwrap()
            ))
            .await
            .unwrap();
            axum::serve(listener, app).await.unwrap();
        }
        _ => {
            panic!("Invalid app mode: {:?}", args[1])
        }
    };
    Ok(())
}

struct AppState {
    pub replica: Arc<Mutex<VrReplica>>,
}

async fn process_client_request(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ClientRequest>,
) -> Json<ServerResponse> {
    let mut replica = state.replica.lock().await;
    let response = replica.process_client_request(payload).await;
    Json(response)
}

async fn process_prepare_request(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PrepareRequest>,
) -> Json<PrepareResponse> {
    let mut replica = state.replica.lock().await;
    let response = replica.process_prepare_message(payload).await;
    Json(response)
}
