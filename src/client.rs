use std::collections::HashMap;

use anyhow::Result;
use anyhow::anyhow;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::message::{Reply, Request};

pub struct VrClient {
    pub id: String,
    pub request_number: u64,
    pub view_number: usize,
    pub replica_urls: Vec<String>,
    pub replica_connections: HashMap<usize, Framed<TcpStream, LengthDelimitedCodec>>,
}

impl VrClient {
    pub fn new(id: String, mut replica_urls: Vec<String>) -> VrClient {
        replica_urls.sort();
        return VrClient {
            id,
            request_number: 0,
            view_number: 0,
            replica_urls,
            replica_connections: HashMap::new(),
        };
    }

    pub async fn send(&mut self, operation: String) -> Result<Reply> {
        let primary_idx = self.primary_idx();
        let connection_opt = self.replica_connections.get_mut(&primary_idx);

        let connection = match connection_opt {
            Some(c) => c,
            None => {
                let primary_url = self.replica_urls.get(primary_idx).unwrap();
                let stream = TcpStream::connect(primary_url).await?;
                let connection = Framed::new(stream, LengthDelimitedCodec::new());
                self.replica_connections.insert(primary_idx, connection);
                self.replica_connections.get_mut(&primary_idx).unwrap()
            }
        };

        let request = Request {
            operation,
            client_id: self.id.clone(),
            request_number: self.request_number,
        };
        let json = serde_json::to_string(&request)?;
        let send_result = connection.send(Bytes::from(json)).await;

        if send_result.is_err() {
            self.replica_connections.remove(&primary_idx);
            return Err(anyhow!(send_result.unwrap_err()));
        }

        self.request_number += 1;

        return match connection.next().await {
            Some(response) => {
                let bytes = response?;

                let json = String::from_utf8(bytes.to_vec())?;
                let message: Reply = serde_json::from_str(&json)?;
                return Ok(message);
            }
            None => Err(anyhow!("Empty response")),
        };
    }

    fn primary_idx(&self) -> usize {
        self.view_number % self.replica_urls.len()
    }
}
