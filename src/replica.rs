use std::collections::HashMap;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Clone, Debug)]
pub enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovering,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRequest {
    pub client_id: String,
    pub request_number: u64,
    pub operation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerResponse {
    pub view_number: usize,
    pub request_number: u64,
    pub result: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareRequest {
    pub view_number: usize,
    pub client_request: ClientRequest,
    pub operation_number: u64,
    pub commit_number: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareResponse {
    pub view_number: usize,
    pub operation_number: u64,
    pub replica_number: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRequest {
    pub view_number: usize,
    pub commit_number: u64,
}

pub struct VrReplica {
    pub replica_number: usize,
    pub view_number: usize,
    pub status: ReplicaStatus,
    pub operation_number: u64,
    pub log: Vec<ClientRequest>,
    pub commit_number: u64,
    pub client_table: HashMap<String, ClientState>,
    pub replica_urls: Vec<String>,
    pub client: Client,
}

#[derive(Clone, Debug)]
pub struct ClientState {
    pub last_request_number: u64,
    pub result: Option<String>,
}

impl VrReplica {
    pub fn new(
        mut replica_urls: Vec<String>,
        replica_number: usize,
        status: ReplicaStatus,
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
            client: Client::new(),
        }
    }

    pub async fn process_client_request(
        &mut self,
        client_request: ClientRequest,
    ) -> ServerResponse {
        println!(
            "Replica {}: received client request {:?}",
            self.replica_number, client_request
        );

        let is_primary = self.view_number % self.replica_urls.len() == self.replica_number;
        if !is_primary {
            println!(
                "Replica {}: is not primary, dropping {:?}",
                self.replica_number, client_request
            );
            return ServerResponse {
                view_number: self.view_number,
                request_number: client_request.request_number,
                result: "not primary".to_owned(),
            };
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
                    ServerResponse {
                        view_number: self.view_number,
                        request_number: state.last_request_number,
                        result: state.result.clone().unwrap(),
                    }
                } else {
                    println!(
                        "Replica {}: is not primary, dropping {:?}",
                        self.replica_number, client_request
                    );
                    ServerResponse {
                        view_number: self.view_number,
                        request_number: state.last_request_number,
                        result: "already executed".to_owned(),
                    }
                }
            }
            None => self.do_process_client_request(client_request).await,
        };
    }

    async fn do_process_client_request(&mut self, client_request: ClientRequest) -> ServerResponse {
        self.operation_number += 1;
        self.log.push(client_request.clone());
        self.client_table.insert(
            client_request.client_id.clone(),
            ClientState {
                last_request_number: client_request.request_number,
                result: None,
            },
        );

        let prepare = PrepareRequest {
            client_request: client_request.clone(),
            view_number: self.view_number,
            operation_number: self.operation_number,
            commit_number: self.commit_number,
        };

        self.send_prepare_message_to_replicas(prepare);

        // wait for f+1 responses etc. (skipped for simplicity)
        let result = self.execute_operation(&client_request.operation).await;
        self.commit_number += 1;

        self.client_table.insert(
            client_request.client_id.clone(),
            ClientState {
                last_request_number: client_request.request_number,
                result: Some(result.clone()),
            },
        );

        ServerResponse {
            view_number: self.view_number,
            request_number: client_request.request_number,
            result,
        }
    }

    fn send_prepare_message_to_replicas(&self, prepare_request: PrepareRequest) {
        // for s in self
        //     .servers
        //     .iter()
        //     .filter(|s| s.borrow().replica_number != self.replica_number)
        // {
        //     s.borrow_mut()
        //         .process_prepare_message(prepare_request.clone());
        // }
    }

    pub async fn process_prepare_message(
        &mut self,
        prepare_request: PrepareRequest,
    ) -> PrepareResponse {
        println!("Received PREPARE request: {:?}", prepare_request);

        if prepare_request.commit_number > 0 {
            let commit = CommitRequest {
                view_number: prepare_request.view_number,
                commit_number: prepare_request.commit_number,
            };
            self.process_commit_message(commit).await;
        }

        if self.log.len() as u64 + 1 < prepare_request.operation_number {
            // missing operations - need sync
            println!("Missing operations, need sync");
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

        PrepareResponse {
            view_number: self.view_number,
            operation_number: self.operation_number,
            replica_number: self.replica_number,
        }
    }

    pub fn send_commit_message_to_replicas(&self, commit_request: CommitRequest) {
        // for s in self
        //     .servers
        //     .iter()
        //     .filter(|s| s.borrow().replica_number != self.replica_number)
        // {
        //     s.borrow_mut()
        //         .process_commit_message(commit_request.clone());
        // }
    }

    pub async fn process_commit_message(&mut self, commit_request: CommitRequest) {
        println!("Received COMMIT request: {:?}", commit_request);

        if self.log.len() as u64 + 1 < commit_request.commit_number {
            println!("Missing operations, need sync");
        }

        if let Some(client_request) = self
            .log
            .get((commit_request.commit_number - 1) as usize)
            .cloned()
        {
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
    }

    async fn execute_operation(&self, operation: &str) -> String {
        fs::write(format!("replica-{}.txt", self.replica_number), operation)
            .await
            .unwrap();
        return format!("executed {}", operation);
    }
}
