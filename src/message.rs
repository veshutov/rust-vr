use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    Request(Request),
    Reply(Reply),
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub client_id: String,
    pub request_number: u64,
    pub operation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Prepare {
    pub view_number: usize,
    pub client_request: Request,
    pub operation_number: usize,
    pub commit_number: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareOk {
    pub view_number: usize,
    pub operation_number: usize,
    pub replica_number: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reply {
    pub view_number: usize,
    pub request_number: u64,
    pub result: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    pub view_number: usize,
    pub commit_number: usize,
}
