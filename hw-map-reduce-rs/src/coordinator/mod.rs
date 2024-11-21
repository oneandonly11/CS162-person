//! The MapReduce coordinator.
//!

use anyhow::Result;
use ::log::info;
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
    Doing,
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
    pub is_full: bool,
    pub map_tasks: Vec<MapTask>,
    pub reduce_tasks: Vec<ReduceTask>,
}

pub struct WorkerTask{
    pub job_id: JobId,
    pub task_id: TaskNumber,
    pub reduce: bool,
}

pub struct Worker {
    pub id: WorkerId,
    pub last_heartbeat: Instant,
    pub status: WorkerStatus,
    pub task: WorkerTask,
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
        let mut job = Job {
            files: req.files.clone(),
            n_reduce: req.n_reduce as usize,
            app: req.app.clone(),
            output_dir: req.output_dir.clone(),
            args: req.args.clone(),
            status: JobStatus::Waiting,
            is_full: false,
            map_tasks: Vec::new(),
            reduce_tasks: Vec::new(),
        };
        job.map_tasks = (0..job.files.len() as TaskNumber).map(|task_id| {
            MapTask {
                task_id,
                worker_id: 0,
                status: TaskStatus::Waiting,
            }
        }).collect();
        job.reduce_tasks = (0..job.n_reduce as TaskNumber).map(|task_id| {
            ReduceTask {
                task_id,
                worker_id: 0,
                status: TaskStatus::Waiting,
            }
        }).collect();
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
            done: matches!(job.status, JobStatus::Finished),
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
            task: WorkerTask {
                job_id: 0,
                task_id: 0,
                reduce: false,
            },
            
        });
        log::info!("Registered worker {}", worker_id);
        Ok(Response::new(RegisterReply { worker_id }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        // TODO: Tasks
        let mut task =  GetTaskReply {
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
        };

        let mut data = self.data.lock().await;
        let worker_id = req.get_ref().worker_id;

        if let Some(worker) = data.workers.get_mut(&worker_id) {
            if let WorkerStatus::Doing = worker.status {
                return Ok(Response::new(task));
            }
        } 

        let job_queue = data.job_queue.clone();
        
        let job_id = match job_queue.iter().find(|job_id| {
            matches!(data.jobs.get_mut(job_id).unwrap().is_full, false)
        }){
            Some(&job_id) => job_id,
            None => return Ok(Response::new(task)),
        };
        
        let job = data.jobs.get_mut(&job_id).unwrap();
        task.job_id = job_id;
        task.app = job.app.clone();
        task.output_dir = job.output_dir.clone();
        task.args = job.args.clone();
        task.n_reduce = job.n_reduce as u32;
        task.n_map = job.files.len() as u32;

        if matches!(job.status, JobStatus::Waiting) {
            job.status = JobStatus::Mapping;
        }

        match job.status {
            JobStatus::Mapping => {
                let map_task = job.map_tasks.iter_mut().find(|map_task| {
                    matches!(map_task.status, TaskStatus::Waiting)
                }).unwrap();
                map_task.status = TaskStatus::Running;
                task.task = map_task.task_id as u32;
                task.file = job.files[map_task.task_id as usize].clone();
                if map_task.task_id == job.files.len() as TaskNumber - 1 {
                    job.is_full = true;
                }
                task.reduce = false;
                task.wait = false;
            }
            JobStatus::Reducing => {
                let reduce_task = job.reduce_tasks.iter_mut().find(|reduce_task| {
                    matches!(reduce_task.status, TaskStatus::Waiting)
                }).unwrap();
                reduce_task.status = TaskStatus::Running;
                task.task = reduce_task.task_id as u32;
                if reduce_task.task_id == job.n_reduce as TaskNumber - 1 {
                    job.is_full = true;
                }
                task.reduce = true;
                task.wait = false;
            }
            _ => return Ok(Response::new(task)),
        }
        let worker = data.workers.get_mut(&worker_id).unwrap();
        worker.task = WorkerTask {
            job_id: job_id,
            task_id: task.task as usize,
            reduce: task.reduce,
        };
        worker.status = WorkerStatus::Doing;
        log::info!("Assigning task {} to worker {}", worker.task.task_id, worker_id);
        Ok(Response::new(task))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        // TODO: Tasks
        let req = req.into_inner();
        let mut data = self.data.lock().await;
        let worker_id = req.worker_id;

        let worker = data.workers.get_mut(&worker_id).unwrap();
        worker.status = WorkerStatus::Idle;
        let job_id = req.job_id;
        let job = data.jobs.get_mut(&job_id).unwrap();
        if req.reduce {
            let reduce_task = job.reduce_tasks.iter_mut().find(|reduce_task| {
                reduce_task.task_id == req.task as usize
            }).unwrap();
            reduce_task.status = TaskStatus::Finished;
            if job.reduce_tasks.iter().all(|reduce_task| matches!(reduce_task.status, TaskStatus::Finished)) {
                job.status = JobStatus::Finished;
                data.job_queue.pop_front();
            }
        } else {
            let map_task = job.map_tasks.iter_mut().find(|map_task| {
                map_task.task_id == req.task as usize
            }).unwrap();
            map_task.status = TaskStatus::Finished;
            if job.map_tasks.iter().all(|map_task| matches!(map_task.status , TaskStatus::Finished)) {
                job.status = JobStatus::Reducing;
                job.is_full = false;
            }
        }
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
