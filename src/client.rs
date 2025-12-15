use anyhow::Result;
use reqwest::Client;

use crate::replica::{ClientRequest, ServerResponse};

pub struct VrClient {
    pub id: String,
    pub request_number: u64,
    pub view_number: usize,
    pub client: Client,
    pub replica_urls: Vec<String>,
}

impl VrClient {
    pub fn new(id: String, mut replica_urls: Vec<String>) -> VrClient {
        replica_urls.sort();
        return VrClient {
            id,
            request_number: 0,
            view_number: 0,
            client: Client::new(),
            replica_urls,
        };
    }

    pub async fn send(&mut self, operation: String) -> Result<ServerResponse> {
        let request = ClientRequest {
            operation,
            client_id: self.id.clone(),
            request_number: self.request_number,
        };
        let primary_idx = self.view_number % self.replica_urls.len();
        let primary_url = self.replica_urls.get(primary_idx).unwrap();
        println!(
            "Client {:?}: request {:?} to {:?}",
            self.id, request, primary_url
        );
        let response: ServerResponse = self
            .client
            .post(primary_url)
            .json(&request)
            .send()
            .await?
            .json()
            .await?;
        println!(
            "Client {:?}: response {:?} from {:?}",
            self.id, response, primary_url
        );
        self.request_number = self.request_number + 1;
        return Ok(response);
    }
}
