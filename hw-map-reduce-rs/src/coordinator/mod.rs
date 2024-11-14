//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic::Code;
use tokio::sync::Mutex;
use tokio::time::Instant;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::rpc::coordinator::*;
use crate::*;
use crate::log;
use crate::app::named;

pub mod args;


pub enum WorkerStatus{
    Idle,
    Mapping,
    Reducing,
    GG,
}

pub enum TaskStatus{
    Waiting,
    Running,
    Finished,
}

pub enum JobStatus{
    Waiting,
    Mapping,
    Reducing,
    Finished,
}

pub struct MapTask{
    pub task_id: TaskNumber,
    pub worker_id: WorkerId,
    pub status: TaskStatus,
}

pub struct ReduceTask{
    pub task_id: TaskNumber,
    pub worker_id: WorkerId,
    pub status: TaskStatus,
}

pub struct Job{
    pub files: Vec<String>,
    pub n_reduce: usize,
    pub app: String,
    pub output_dir: String,
    pub args: Vec<u8>,
    pub status: JobStatus,
    pub map_tasks: Vec<MapTask>,
    pub reduce_tasks: Vec<ReduceTask>,
}

pub struct Worker {
    pub id: WorkerId,
    pub last_heartbeat: Instant,
    pub status: WorkerStatus,
}
pub struct CoordinatorData {
    pub workers: HashMap<WorkerId, Worker>,
    pub next_worker_id: WorkerId,
    pub jobs: HashMap<JobId,Job>,
    pub next_job_id: JobId,
    pub job_queue: VecDeque<JobId>,
}

impl Job {
    pub fn new(req: SubmitJobRequest ) -> Self {
        let job = Job {
            files: req.files.clone(),
            n_reduce: req.n_reduce as usize,
            app: req.app.clone(),
            output_dir: req.output_dir.clone(),
            args: req.args.clone(),
            status: JobStatus::Waiting,
            map_tasks: Vec::new(),
            reduce_tasks: Vec::new(),
        };
        job
        
    }
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
            jobs: HashMap::new(),
            next_job_id: INITIAL_JOB_ID,
            job_queue: VecDeque::new(),
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
        // todo!("Job submission")
        let req = req.into_inner();
        let mut data = self.data.lock().await;
        let job_id = data.next_job_id;
        data.next_job_id += 1;
        named(&req.app).map_err(|e| Status::new(Code::InvalidArgument, e.to_string()))?;  
        
        let job = Job::new(req);
        data.jobs.insert(job_id, job);
        data.job_queue.push_back(job_id);
        log::info!("Job {} submitted", job_id);
        Ok(Response::new(SubmitJobReply { job_id }))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        // todo!("Job submission")
        let mut data = self.data.lock().await;
        let job_id = req.get_ref().job_id;
        let job = data.jobs.get_mut(&job_id).ok_or(Status::new(Code::NotFound, "job id is invalid"))?;
        Ok(Response::new(PollJobReply { 
            done: false ,
            failed: false,
            errors: Vec::new(),
        }))
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
            status: WorkerStatus::Idle,
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
