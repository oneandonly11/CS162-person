//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tokio::sync::Mutex;
use tokio::time::Instant;
use std::collections::HashMap;
use std::sync::Arc;

use crate::rpc::coordinator::*;
use crate::*;
use crate::log;

pub mod args;


pub struct Worker {
    pub id: WorkerId,
    pub last_heartbeat: Instant,
}
pub struct CoordinatorData {
    pub workers: HashMap<WorkerId, Worker>,
    pub next_worker_id: WorkerId,
}

pub struct Coordinator {
    // TODO: add your own fields
    data : Arc<Mutex<CoordinatorData>>,
}

impl Coordinator {
    pub fn new() -> Self {
        Self { data: Arc::new(Mutex::new(CoordinatorData {
            workers: HashMap::new(),
            next_worker_id: INITIAL_WORKER_ID,
        })) }
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    /// An example RPC.
    ///
    /// Feel free to delete this.
    /// Make sure to also delete the RPC in `proto/coordinator.proto`.
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        let req = req.get_ref();
        let message = format!("Hello, {}!", req.name);
        Ok(Response::new(ExampleReply { message }))
    }

    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        todo!("Job submission")
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        todo!("Job submission")
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        // TODO: Worker registration

        let worker_id = req.into_inner().worker_id;
        let mut data = self.data.lock().await;
        if let Some(worker) = data.workers.get_mut(&worker_id) {
            worker.last_heartbeat = Instant::now();
            log::info!("Heartbeat from worker {}", worker_id);
        } else {
            return Err(Status::not_found("Worker not found"));
        }
        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        // TODO: Worker registration
        let mut data = self.data.lock().await;
        let worker_id = data.next_worker_id;
        data.next_worker_id += 1;
        data.workers.insert(worker_id, Worker {
            id: worker_id,
            last_heartbeat: Instant::now(),
        });
        log::info!("Registered worker {}", worker_id);
        Ok(Response::new(RegisterReply { worker_id }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        // TODO: Tasks
        Ok(Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "".to_string(),
            app: "".to_string(),
            task: 0,
            file: "".to_string(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new(),
        }))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        // TODO: Tasks
        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        // TODO: Fault tolerance
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();

    let coordinator = Coordinator::new();
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
