use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use anyhow::Result;
use bytes::Bytes;
use futures::SinkExt;
use futures::stream::SplitSink;
use tokio::net::TcpStream;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

use crate::message::{Commit, Message, Prepare, PrepareOk, Reply, Request};

pub struct VrReplica {
    // Protocol
    pub replica_number: usize,
    pub view_number: usize,
    pub status: ReplicaStatus,
    pub operation_number: usize,
    pub log: Vec<Request>,
    pub commit_number: usize,
    pub client_table: HashMap<String, ClientState>,
    pub replica_urls: Vec<String>,
    // Communication
    pub client_connections:
        HashMap<String, Rc<RefCell<SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>>>>,
}

#[derive(Clone, Debug)]
pub enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovering,
}

#[derive(Clone, Debug)]
pub struct ClientState {
    pub last_request_number: u64,
    pub result: Option<String>,
}

impl VrReplica {
    pub fn new(
        replica_number: usize,
        status: ReplicaStatus,
        mut replica_urls: Vec<String>,
    ) -> Self {
        replica_urls.sort();
        VrReplica {
            replica_urls,
            replica_number,
            view_number: 0,
            status,
            operation_number: 0,
            log: Vec::new(),
            commit_number: 0,
            client_table: HashMap::new(),
            client_connections: HashMap::new(),
        }
    }

    pub async fn process_message(&mut self, message: Message) {
        match message {
            Message::Request(request) => self.process_client_request(request).await,
            Message::Reply(_) => println!(
                "Replica {}: cannot process Reply message",
                self.replica_number
            ),
            Message::Prepare(prepare) => self.process_prepare_message(prepare).await,
            Message::PrepareOk(_) => todo!(),
            Message::Commit(commit) => self.process_commit_message(commit).await,
        };
    }

    pub async fn process_client_request(&mut self, client_request: Request) {
        let is_primary = self.view_number % self.replica_urls.len() == self.replica_number;
        if !is_primary {
            println!(
                "Replica {}: is not primary, dropping {:?}",
                self.replica_number, client_request
            );
            return;
        }

        let maybe_last_request = self.client_table.get(&client_request.client_id);

        return match maybe_last_request {
            Some(state) => {
                if state.last_request_number < client_request.request_number {
                    self.do_process_client_request(client_request).await
                } else if state.last_request_number == client_request.request_number {
                    println!(
                        "Replica {}: returning cached response for {:?}",
                        self.replica_number, client_request
                    );
                    let reply = Reply {
                        view_number: self.view_number,
                        request_number: state.last_request_number,
                        result: state.result.clone().unwrap(),
                    };
                    self.send_reply_to_client(client_request.client_id, reply)
                        .await
                        .unwrap();
                } else {
                    println!(
                        "Replica {}: already executed {:?}",
                        self.replica_number, client_request
                    );
                }
            }
            None => self.do_process_client_request(client_request).await,
        };
    }

    async fn do_process_client_request(&mut self, client_request: Request) {
        self.operation_number += 1;
        self.log.push(client_request.clone());
        self.client_table.insert(
            client_request.client_id.clone(),
            ClientState {
                last_request_number: client_request.request_number,
                result: None,
            },
        );

        let prepare = Prepare {
            client_request: client_request.clone(),
            view_number: self.view_number,
            operation_number: self.operation_number,
            commit_number: self.commit_number,
        };

        self.send_prepare_message_to_replicas(prepare).await;

        let result = self.execute_operation(&client_request.operation).await;
        self.commit_number += 1;

        self.client_table.insert(
            client_request.client_id.clone(),
            ClientState {
                last_request_number: client_request.request_number,
                result: Some(result.clone()),
            },
        );

        let reply = Reply {
            view_number: self.view_number,
            request_number: client_request.request_number,
            result,
        };
        self.send_reply_to_client(client_request.client_id, reply)
            .await
            .unwrap();
    }

    async fn send_reply_to_client(&self, client_id: String, reply: Reply) -> Result<()> {
        let mut connection = self
            .client_connections
            .get(&client_id)
            .unwrap()
            .borrow_mut();
        let json = serde_json::to_string(&reply)?;
        connection.send(Bytes::from(json)).await?;
        Ok(())
    }

    async fn send_prepare_message_to_replicas(&self, prepare_request: Prepare) {
        for (replica_number, replica_url) in self.replica_urls.iter().enumerate() {
            if replica_number != self.replica_number {
                // let _: PrepareOk = self
                //     .replica_client
                //     .post(format!("{}/prepare", replica_url))
                //     .json(&prepare_request)
                //     .timeout(Duration::new(1, 0))
                //     .send()
                //     .await
                //     .unwrap()
                //     .json()
                //     .await
                //     .unwrap();
            }
        }
    }

    pub async fn process_prepare_message(&mut self, prepare_request: Prepare) {
        println!(
            "Replica {}: received {:?}",
            self.replica_number, prepare_request
        );

        if prepare_request.commit_number > 0 {
            let commit = Commit {
                view_number: prepare_request.view_number,
                commit_number: prepare_request.commit_number,
            };
            self.process_commit_message(commit).await;
        }

        if self.log.len() + 1 < prepare_request.operation_number {
            // missing operations - need sync
            println!(
                "Replica {}: missing operations, need sync",
                self.replica_number
            );
        }

        self.operation_number += 1;
        self.log.push(prepare_request.client_request.clone());
        self.client_table.insert(
            prepare_request.client_request.client_id.clone(),
            ClientState {
                last_request_number: prepare_request.client_request.request_number,
                result: None,
            },
        );

        PrepareOk {
            view_number: self.view_number,
            operation_number: prepare_request.operation_number,
            replica_number: self.replica_number,
        };
    }

    pub async fn process_commit_message(&mut self, commit_request: Commit) {
        println!(
            "Replica {}: received {:?}",
            self.replica_number, commit_request
        );

        if self.log.len() < commit_request.commit_number {
            println!(
                "Replica {}: missing operations, need sync",
                self.replica_number
            );
        }

        let client_request = self.log.get(commit_request.commit_number - 1).unwrap();
        let result = self.execute_operation(&client_request.operation).await;

        self.commit_number += 1;
        self.client_table.insert(
            client_request.client_id.clone(),
            ClientState {
                last_request_number: client_request.request_number,
                result: Some(result),
            },
        );
    }

    async fn execute_operation(&self, operation: &str) -> String {
        let file_path = format!("replica-{}.txt", self.replica_number);
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .unwrap();

        file.write(&operation.as_bytes()).await.unwrap();
        return format!("executed {}", operation);
    }
}
