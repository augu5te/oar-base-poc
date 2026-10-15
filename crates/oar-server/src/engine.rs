//! The server engine: everything the automaton steps actually do.
//!
//! The engine owns the (placeholder) database, the node registry, the scheduler
//! policy and the node launcher. It is driven exclusively by [`crate::automaton`],
//! through [`EngineSteps`], which:
//!
//! * wakes up dedicated worker tasks for the *scheduling* and *launching jobs*
//!   steps, waiting for their completion;
//! * spawns a task for the *checking nodes* and *finishing jobs* steps and waits
//!   for it.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use oar_common::db::Database;
use oar_common::registry::NodeRegistry;
use oar_common::types::{Job, JobFilter, JobId, JobRequest, JobState, NodeId, now_unix_secs};
use tokio::sync::{mpsc, oneshot};

use crate::ServerError;
use crate::automaton::{Event, Step, StepExecutor};
use crate::node_client::{GrpcNodeLauncher, NodeLauncher};
use crate::scheduler::{FifoScheduler, Scheduler};

/// Exit code recorded when a job is terminated for exceeding its walltime.
pub const WALLTIME_EXIT_CODE: i32 = -9;

/// Replaceable clock, so walltime expiry can be tested deterministically.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Counters reported by the `AdminService`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClusterStatus {
    pub nodes_registered: u32,
    pub nodes_alive: u32,
    pub jobs_waiting: u32,
    pub jobs_active: u32,
    pub jobs_finished: u32,
}

/// The server engine.
pub struct Engine {
    config: Arc<oar_common::config::Config>,
    db: Arc<dyn Database>,
    registry: Arc<NodeRegistry>,
    launcher: Arc<dyn NodeLauncher>,
    scheduler: Arc<dyn Scheduler>,
    events: mpsc::UnboundedSender<Event>,
    clock: Clock,
    last_node_check: Mutex<Instant>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("nodes", &self.registry.len())
            .field("scheduler", &self.scheduler.name())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
pub struct EngineBuilder {
    config: Option<Arc<oar_common::config::Config>>,
    db: Option<Arc<dyn Database>>,
    registry: Option<Arc<NodeRegistry>>,
    launcher: Option<Arc<dyn NodeLauncher>>,
    scheduler: Option<Arc<dyn Scheduler>>,
    clock: Option<Clock>,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: impl Into<Arc<oar_common::config::Config>>) -> Self {
        self.config = Some(config.into());
        self
    }

    pub fn database(mut self, db: Arc<dyn Database>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn registry(mut self, registry: Arc<NodeRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn launcher(mut self, launcher: Arc<dyn NodeLauncher>) -> Self {
        self.launcher = Some(launcher);
        self
    }

    pub fn scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn clock(mut self, clock: Clock) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Builds the engine and the receiving end of its event queue.
    pub fn build(self) -> (Arc<Engine>, mpsc::UnboundedReceiver<Event>) {
        let (events, receiver) = mpsc::unbounded_channel();
        let engine = Arc::new(Engine {
            config: self
                .config
                .unwrap_or_else(|| Arc::new(oar_common::config::Config::default())),
            db: self
                .db
                .unwrap_or_else(|| Arc::new(oar_common::db::MemoryDatabase::new())),
            registry: self.registry.unwrap_or_default(),
            launcher: self
                .launcher
                .unwrap_or_else(|| Arc::new(GrpcNodeLauncher::new())),
            scheduler: self
                .scheduler
                .unwrap_or_else(|| Arc::new(FifoScheduler::new())),
            events,
            clock: self.clock.unwrap_or_else(|| Arc::new(now_unix_secs)),
            last_node_check: Mutex::new(Instant::now()),
        });
        (engine, receiver)
    }
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::new()
    }

    pub fn config(&self) -> &oar_common::config::Config {
        &self.config
    }

    pub fn database(&self) -> &Arc<dyn Database> {
        &self.db
    }

    pub fn registry(&self) -> &Arc<NodeRegistry> {
        &self.registry
    }

    pub fn scheduler_name(&self) -> &'static str {
        self.scheduler.name()
    }

    /// Pushes an event in the server queue (never blocks: the queue is unbounded
    /// and drained by the automaton).
    pub fn emit(&self, event: Event) {
        if self.events.send(event).is_err() {
            tracing::warn!("event queue closed, event dropped");
        }
    }

    // -- called by the gRPC services ----------------------------------------

    /// Stores a new job and wakes the automaton up for scheduling.
    pub async fn submit_job(&self, request: JobRequest) -> Result<JobId, ServerError> {
        request.validate()?;
        let job_id = self.db.allocate_job_id().await?;
        self.db.insert_job(Job::new(job_id, request)).await?;
        self.emit(Event::NewJob { job_id });
        tracing::info!(job_id, "job submitted");
        Ok(job_id)
    }

    /// Registers (or refreshes) a compute node. Returns `false` on invalid input.
    pub fn register_node(&self, node_id: &str, address: &str) -> bool {
        let node_id = node_id.trim();
        let address = address.trim();
        if node_id.is_empty() || address.is_empty() {
            return false;
        }
        self.registry.upsert(node_id, address);
        tracing::info!(node_id, address, "node registered");
        self.emit(Event::CheckNodes);
        true
    }

    /// Marks a known node alive. Returns `None` when the node is unknown.
    pub fn heartbeat(&self, node_id: &str, running_jobs: u32) -> Option<bool> {
        tracing::trace!(node_id, running_jobs, "heartbeat");
        if !self.registry.contains(node_id) {
            return None;
        }
        self.registry.set_alive(node_id, true)
    }

    /// Records a job completion. Unknown or already-finished jobs are ignored
    /// (returns `false`); the operation is idempotent.
    pub async fn finish_job(
        &self,
        job_id: JobId,
        node_id: &str,
        exit_code: i32,
    ) -> Result<bool, ServerError> {
        let Some(mut job) = self.db.get_job(job_id).await? else {
            tracing::warn!(job_id, node_id, "finish notification for an unknown job");
            return Ok(false);
        };
        if job.state.is_final() || job.state == JobState::Finishing {
            tracing::debug!(job_id, state = %job.state, "duplicate finish notification ignored");
            return Ok(true);
        }
        if !job.state.can_transition_to(JobState::Finishing) {
            tracing::warn!(job_id, state = %job.state, "finish notification in an unexpected state");
            return Ok(false);
        }
        job.exit_code = Some(exit_code);
        job.message = Some(format!("finished on {node_id}"));
        job.transition_to(JobState::Finishing)?;
        self.db.update_job(&job).await?;
        self.emit(Event::JobFinished {
            job_id,
            node_id: node_id.to_string(),
            exit_code,
        });
        tracing::info!(job_id, node_id, exit_code, "job finished");
        Ok(true)
    }

    pub async fn job(&self, job_id: JobId) -> Result<Option<Job>, ServerError> {
        Ok(self.db.get_job(job_id).await?)
    }

    pub async fn jobs(&self, filter: &JobFilter) -> Result<Vec<Job>, ServerError> {
        Ok(self.db.list_jobs(filter).await?)
    }

    pub async fn cluster_status(&self) -> Result<ClusterStatus, ServerError> {
        let jobs = self.db.list_jobs(&JobFilter::all()).await?;
        Ok(ClusterStatus {
            nodes_registered: self.registry.len() as u32,
            nodes_alive: self.registry.alive().len() as u32,
            jobs_waiting: jobs.iter().filter(|j| j.state == JobState::Waiting).count() as u32,
            jobs_active: jobs.iter().filter(|j| j.state.is_active()).count() as u32,
            jobs_finished: jobs.iter().filter(|j| j.state.is_final()).count() as u32,
        })
    }

    // -- automaton steps -----------------------------------------------------

    /// Scheduling step: run the scheduler on the waiting jobs and mark the
    /// selected jobs `ToLaunch`.
    pub async fn scheduling(&self) -> Result<(), ServerError> {
        let waiting = self
            .db
            .list_jobs(&JobFilter::state(JobState::Waiting))
            .await?;
        if waiting.is_empty() {
            return Ok(());
        }

        let busy = self.busy_node_ids().await?;
        let available: Vec<_> = self
            .registry
            .alive()
            .into_iter()
            .filter(|node| !busy.contains(&node.id))
            .collect();

        let assignments = self.scheduler.schedule(&waiting, &available);
        for assignment in &assignments {
            let Some(mut job) = self.db.get_job(assignment.job_id).await? else {
                continue;
            };
            job.assigned_nodes = assignment.nodes.clone();
            if let Err(error) = job.transition_to(JobState::ToLaunch) {
                tracing::warn!(job_id = job.id, %error, "cannot mark job to launch");
                continue;
            }
            self.db.update_job(&job).await?;
            tracing::info!(job_id = job.id, nodes = ?assignment.nodes, "job scheduled");
        }

        if !assignments.is_empty() {
            self.emit(Event::JobsToLaunch);
        }
        Ok(())
    }

    /// Launching jobs step: ask the head node of every `ToLaunch` job to start it.
    pub async fn launching(&self) -> Result<(), ServerError> {
        for mut job in self
            .db
            .list_jobs(&JobFilter::state(JobState::ToLaunch))
            .await?
        {
            let job_id = job.id;
            if let Err(error) = job.transition_to(JobState::Launching) {
                tracing::warn!(job_id, %error, "cannot mark job launching");
                continue;
            }
            self.db.update_job(&job).await?;

            let Some(node) = job.head_node().and_then(|id| self.registry.get(id)) else {
                let missing = job.head_node().cloned().unwrap_or_default();
                job.message = Some(format!("head node `{missing}` is not registered"));
                job.exit_code = Some(-1);
                let _ = job.transition_to(JobState::Error);
                self.db.update_job(&job).await?;
                tracing::error!(job_id, node = %missing, "cannot launch job on an unknown node");
                continue;
            };

            match self.launcher.start_job(&node, &job).await {
                Ok(()) => {
                    job.transition_to(JobState::Running)?;
                    // Use the engine clock so walltime expiry is testable.
                    job.started_at_unix = Some((self.clock)());
                    self.db.update_job(&job).await?;
                    tracing::info!(job_id, node = %node.id, "job launched");
                }
                Err(error) => {
                    job.message = Some(error.to_string());
                    job.exit_code = Some(-1);
                    let _ = job.transition_to(JobState::Error);
                    self.db.update_job(&job).await?;
                    tracing::error!(job_id, node = %node.id, %error, "job launch failed");
                }
            }
        }
        Ok(())
    }

    /// Checking nodes step: probe every registered node and update liveness.
    pub async fn checking_nodes(&self) -> Result<(), ServerError> {
        *self.last_node_check.lock().expect("clock lock poisoned") = Instant::now();
        let nodes = self.registry.list();
        let mut alive = 0usize;
        let mut revived = 0usize;
        for node in &nodes {
            match self.launcher.ping(node).await {
                Ok(ping) => {
                    alive += 1;
                    if !node.alive {
                        revived += 1;
                    }
                    self.registry.set_alive(&node.id, true);
                    tracing::trace!(node = %node.id, running_jobs = ping.running_jobs, "node alive");
                }
                Err(error) => {
                    self.registry.set_alive(&node.id, false);
                    tracing::warn!(node = %node.id, %error, "node suspected");
                }
            }
        }
        if !nodes.is_empty() {
            tracing::debug!(total = nodes.len(), alive, "node check done");
        }
        // Nodes that came back are new capacity: try to schedule on them.
        if revived > 0 {
            self.emit(Event::ScheduleJobs);
        }
        Ok(())
    }

    /// Finishing jobs step: move every `Finishing` job to `Terminated`.
    pub async fn finishing_jobs(&self) -> Result<(), ServerError> {
        let mut terminated = 0usize;
        for mut job in self
            .db
            .list_jobs(&JobFilter::state(JobState::Finishing))
            .await?
        {
            if let Err(error) = job.transition_to(JobState::Terminated) {
                tracing::warn!(job_id = job.id, %error, "cannot terminate job");
                continue;
            }
            self.db.update_job(&job).await?;
            terminated += 1;
            tracing::info!(job_id = job.id, exit_code = ?job.exit_code, "job terminated");
        }
        // The jobs just terminated released their nodes: wake the scheduler up
        // so that the jobs still waiting can be launched.
        if terminated > 0 {
            self.emit(Event::ScheduleJobs);
        }
        Ok(())
    }

    /// Waiting step: terminate jobs that exceeded their walltime and trigger the
    /// periodic node check.
    pub async fn waiting(&self) -> Result<(), ServerError> {
        self.expire_walltimes().await?;

        let due = self
            .last_node_check
            .lock()
            .expect("clock lock poisoned")
            .elapsed()
            >= Duration::from_secs(self.config.finaud_frequency_secs.max(1));
        if due {
            self.emit(Event::CheckNodes);
        }
        Ok(())
    }

    /// Terminates the running jobs whose walltime elapsed.
    async fn expire_walltimes(&self) -> Result<(), ServerError> {
        let now = (self.clock)();
        for mut job in self
            .db
            .list_jobs(&JobFilter::state(JobState::Running))
            .await?
        {
            if !job.walltime_exceeded_at(now) {
                continue;
            }
            job.exit_code = Some(WALLTIME_EXIT_CODE);
            job.message = Some("walltime exceeded".into());
            if let Err(error) = job.transition_to(JobState::Finishing) {
                tracing::warn!(job_id = job.id, %error, "cannot finish expired job");
                continue;
            }
            self.db.update_job(&job).await?;
            let head = job.head_node().cloned().unwrap_or_default();
            tracing::info!(job_id = job.id, "walltime exceeded, job finishing");
            self.emit(Event::JobFinished {
                job_id: job.id,
                node_id: head,
                exit_code: WALLTIME_EXIT_CODE,
            });
        }
        Ok(())
    }

    /// Nodes currently held by jobs that are not finished yet.
    async fn busy_node_ids(&self) -> Result<HashSet<NodeId>, ServerError> {
        let mut busy = HashSet::new();
        for job in self.db.list_jobs(&JobFilter::all()).await? {
            if job.state.is_active() {
                busy.extend(job.assigned_nodes.iter().cloned());
            }
        }
        Ok(busy)
    }
}

/// Worker signal: the automaton sends a reply channel and waits for the result.
type WorkerSignal = mpsc::Sender<oneshot::Sender<Result<(), ServerError>>>;

/// [`StepExecutor`] connecting the automaton to an [`Engine`].
///
/// The scheduling and launching steps are delegated to two long-lived worker
/// tasks; the checking and finishing steps are spawned per invocation.
pub struct EngineSteps {
    engine: Arc<Engine>,
    scheduler_worker: WorkerSignal,
    launcher_worker: WorkerSignal,
    worker_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl EngineSteps {
    /// Starts the two worker tasks and returns the step executor driving them.
    pub fn start(engine: Arc<Engine>) -> Arc<Self> {
        let (scheduler_worker, scheduler_rx) = mpsc::channel(16);
        let (launcher_worker, launcher_rx) = mpsc::channel(16);

        let scheduler_task = tokio::spawn(worker_loop(
            Arc::clone(&engine),
            scheduler_rx,
            WorkerKind::Scheduler,
        ));
        let launcher_task = tokio::spawn(worker_loop(
            Arc::clone(&engine),
            launcher_rx,
            WorkerKind::Launcher,
        ));

        Arc::new(Self {
            engine,
            scheduler_worker,
            launcher_worker,
            worker_tasks: Mutex::new(vec![scheduler_task, launcher_task]),
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    async fn wake(&self, signal: &WorkerSignal, step: Step) -> Result<(), ServerError> {
        let (reply, wait) = oneshot::channel();
        signal
            .send(reply)
            .await
            .map_err(|_| ServerError::WorkerGone {
                step: step.as_str(),
            })?;
        wait.await.map_err(|_| ServerError::WorkerGone {
            step: step.as_str(),
        })?
    }

    async fn spawn_step(&self, step: Step) -> Result<(), ServerError> {
        let engine = Arc::clone(&self.engine);
        let handle = match step {
            Step::CheckingNodes => tokio::spawn(async move { engine.checking_nodes().await }),
            Step::FinishingJobs => tokio::spawn(async move { engine.finishing_jobs().await }),
            other => {
                return Err(ServerError::Task(format!("step {other} cannot be spawned")));
            }
        };
        handle
            .await
            .map_err(|error| ServerError::Task(error.to_string()))?
    }

    /// Aborts the worker tasks (used when shutting the server down).
    pub fn shutdown(&self) {
        for task in self
            .worker_tasks
            .lock()
            .expect("worker lock poisoned")
            .drain(..)
        {
            task.abort();
        }
    }
}

#[async_trait]
impl StepExecutor for EngineSteps {
    async fn run(&self, step: Step) -> Result<(), ServerError> {
        match step {
            Step::Scheduling => self.wake(&self.scheduler_worker, step).await,
            Step::LaunchingJobs => self.wake(&self.launcher_worker, step).await,
            Step::CheckingNodes | Step::FinishingJobs => self.spawn_step(step).await,
            Step::Waiting => self.engine.waiting().await,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum WorkerKind {
    Scheduler,
    Launcher,
}

async fn worker_loop(
    engine: Arc<Engine>,
    mut receiver: mpsc::Receiver<oneshot::Sender<Result<(), ServerError>>>,
    kind: WorkerKind,
) {
    while let Some(reply) = receiver.recv().await {
        let result = match kind {
            WorkerKind::Scheduler => engine.scheduling().await,
            WorkerKind::Launcher => engine.launching().await,
        };
        // The automaton may have been shut down; ignore the send error.
        let _ = reply.send(result);
    }
    tracing::debug!(?kind, "engine worker stops");
}

#[cfg(test)]
mod tests {
    use super::*;
    use oar_common::db::MemoryDatabase;
    use oar_common::types::NodeInfo;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::node_client::{LauncherError, NodePing};
    use crate::scheduler::FifoScheduler;

    #[derive(Default)]
    struct FakeLauncher {
        started: Mutex<Vec<(String, JobId)>>,
        alive: Mutex<HashMap<String, bool>>,
    }

    #[async_trait]
    impl NodeLauncher for FakeLauncher {
        async fn start_job(&self, node: &NodeInfo, job: &Job) -> Result<(), LauncherError> {
            self.started.lock().unwrap().push((node.id.clone(), job.id));
            if !self
                .alive
                .lock()
                .unwrap()
                .get(&node.id)
                .copied()
                .unwrap_or(true)
            {
                return Err(LauncherError::rejected(node, "down"));
            }
            Ok(())
        }

        async fn ping(&self, node: &NodeInfo) -> Result<NodePing, LauncherError> {
            if self
                .alive
                .lock()
                .unwrap()
                .get(&node.id)
                .copied()
                .unwrap_or(true)
            {
                Ok(NodePing {
                    node_id: node.id.clone(),
                    running_jobs: 0,
                })
            } else {
                Err(LauncherError::unreachable(node, "timeout"))
            }
        }
    }

    struct Harness {
        engine: Arc<Engine>,
        events: mpsc::UnboundedReceiver<Event>,
        launcher: Arc<FakeLauncher>,
        now: Arc<AtomicU64>,
    }

    impl Harness {
        fn new(nodes: &[&str]) -> Self {
            let registry = Arc::new(NodeRegistry::with_nodes(
                nodes
                    .iter()
                    .map(|id| NodeInfo::new(*id, format!("127.0.0.1:7{id}"))),
            ));
            let launcher = Arc::new(FakeLauncher::default());
            let now = Arc::new(AtomicU64::new(1_000_000));
            let clock: Clock = {
                let now = Arc::clone(&now);
                Arc::new(move || now.load(Ordering::SeqCst))
            };
            let (engine, events) = Engine::builder()
                .database(Arc::new(MemoryDatabase::new()))
                .registry(registry)
                .launcher(launcher.clone())
                .scheduler(Arc::new(FifoScheduler::new()))
                .clock(clock)
                .build();
            Self {
                engine,
                events,
                launcher,
                now,
            }
        }

        async fn submit(&self, nodes: u32, walltime: u32) -> JobId {
            self.engine
                .submit_job(JobRequest::new(1000, "echo hi", nodes, walltime))
                .await
                .unwrap()
        }

        fn drain(&mut self) -> Vec<Event> {
            let mut events = Vec::new();
            while let Ok(event) = self.events.try_recv() {
                events.push(event);
            }
            events
        }

        async fn state(&self, job_id: JobId) -> JobState {
            self.engine.job(job_id).await.unwrap().unwrap().state
        }
    }

    #[tokio::test]
    async fn submitting_a_job_stores_it_and_wakes_the_automaton() {
        let mut harness = Harness::new(&["n1"]);
        let job_id = harness.submit(1, 60).await;
        assert_eq!(harness.state(job_id).await, JobState::Waiting);
        assert_eq!(harness.drain(), vec![Event::NewJob { job_id }]);
    }

    #[tokio::test]
    async fn invalid_submissions_are_rejected() {
        let harness = Harness::new(&["n1"]);
        let error = harness
            .engine
            .submit_job(JobRequest::new(1000, "  ", 1, 60))
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::Validation(_)));
        assert!(
            harness
                .engine
                .database()
                .list_jobs(&JobFilter::all())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn scheduling_assigns_nodes_and_requests_a_launch() {
        let mut harness = Harness::new(&["n1", "n2"]);
        let first = harness.submit(1, 60).await;
        let second = harness.submit(2, 60).await;
        harness.drain();

        harness.engine.scheduling().await.unwrap();

        let first_job = harness.engine.job(first).await.unwrap().unwrap();
        assert_eq!(first_job.state, JobState::ToLaunch);
        assert_eq!(first_job.assigned_nodes, vec!["n1".to_string()]);

        let second_job = harness.engine.job(second).await.unwrap().unwrap();
        assert_eq!(
            second_job.state,
            JobState::Waiting,
            "not enough nodes for the second job"
        );

        assert_eq!(harness.drain(), vec![Event::JobsToLaunch]);
    }

    #[tokio::test]
    async fn scheduling_ignores_nodes_held_by_active_jobs() {
        let harness = Harness::new(&["n1"]);
        let first = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();
        assert_eq!(harness.state(first).await, JobState::Running);

        let second = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        assert_eq!(harness.state(second).await, JobState::Waiting, "n1 is busy");
    }

    #[tokio::test]
    async fn launching_calls_the_head_node_and_marks_the_job_running() {
        let harness = Harness::new(&["n1"]);
        let job_id = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();

        assert_eq!(harness.state(job_id).await, JobState::Running);
        assert_eq!(
            harness.launcher.started.lock().unwrap().as_slice(),
            &[("n1".to_string(), job_id)]
        );
        let job = harness.engine.job(job_id).await.unwrap().unwrap();
        assert!(job.started_at_unix.is_some());
    }

    #[tokio::test]
    async fn a_failed_launch_puts_the_job_in_error() {
        let harness = Harness::new(&["n1"]);
        harness
            .launcher
            .alive
            .lock()
            .unwrap()
            .insert("n1".into(), false);
        let job_id = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();

        let job = harness.engine.job(job_id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Error);
        assert_eq!(job.exit_code, Some(-1));
        assert!(job.message.unwrap().contains("down"));
    }

    #[tokio::test]
    async fn checking_nodes_updates_liveness() {
        let harness = Harness::new(&["n1", "n2"]);
        harness
            .launcher
            .alive
            .lock()
            .unwrap()
            .insert("n2".into(), false);
        harness.engine.checking_nodes().await.unwrap();

        assert!(harness.engine.registry().get("n1").unwrap().alive);
        assert!(!harness.engine.registry().get("n2").unwrap().alive);
        assert_eq!(
            harness.engine.cluster_status().await.unwrap().nodes_alive,
            1
        );
    }

    #[tokio::test]
    async fn finish_job_is_idempotent_and_ignores_unknown_jobs() {
        let mut harness = Harness::new(&["n1"]);
        let job_id = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();
        harness.drain();

        assert!(harness.engine.finish_job(job_id, "n1", 0).await.unwrap());
        assert_eq!(harness.state(job_id).await, JobState::Finishing);
        assert_eq!(
            harness.drain(),
            vec![Event::JobFinished {
                job_id,
                node_id: "n1".into(),
                exit_code: 0
            }]
        );

        // Second notification is a no-op.
        assert!(harness.engine.finish_job(job_id, "n1", 0).await.unwrap());
        assert!(harness.drain().is_empty());

        harness.engine.finishing_jobs().await.unwrap();
        assert_eq!(harness.state(job_id).await, JobState::Terminated);

        assert!(!harness.engine.finish_job(999, "n1", 0).await.unwrap());
    }

    #[tokio::test]
    async fn walltime_expiry_finishes_a_running_job() {
        let mut harness = Harness::new(&["n1"]);
        let job_id = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();
        harness.drain();

        // Not yet expired.
        harness.engine.waiting().await.unwrap();
        assert_eq!(harness.state(job_id).await, JobState::Running);
        assert!(harness.drain().is_empty());

        harness.now.fetch_add(61, Ordering::SeqCst);
        harness.engine.waiting().await.unwrap();
        let job = harness.engine.job(job_id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Finishing);
        assert_eq!(job.exit_code, Some(WALLTIME_EXIT_CODE));
        assert_eq!(job.message.as_deref(), Some("walltime exceeded"));

        let events = harness.drain();
        assert!(events.contains(&Event::JobFinished {
            job_id,
            node_id: "n1".into(),
            exit_code: WALLTIME_EXIT_CODE
        }));

        harness.engine.finishing_jobs().await.unwrap();
        assert_eq!(harness.state(job_id).await, JobState::Terminated);
    }

    #[tokio::test]
    async fn finishing_a_job_wakes_the_scheduler_for_the_next_one() {
        let mut harness = Harness::new(&["n1"]);
        let first = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();

        // The only node is busy, so the second job must wait.
        let second = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        assert_eq!(harness.state(second).await, JobState::Waiting);
        harness.drain();

        harness.engine.finish_job(first, "n1", 0).await.unwrap();
        harness.engine.finishing_jobs().await.unwrap();
        assert_eq!(harness.state(first).await, JobState::Terminated);

        // Terminating the first job released `n1`: the scheduler is woken up.
        assert!(harness.drain().contains(&Event::ScheduleJobs));
        harness.engine.scheduling().await.unwrap();
        let second_job = harness.engine.job(second).await.unwrap().unwrap();
        assert_eq!(second_job.state, JobState::ToLaunch);
        assert_eq!(second_job.assigned_nodes, vec!["n1".to_string()]);
    }

    #[tokio::test]
    async fn a_node_coming_back_wakes_the_scheduler() {
        let mut harness = Harness::new(&["n1"]);
        harness
            .launcher
            .alive
            .lock()
            .unwrap()
            .insert("n1".into(), false);
        harness.engine.checking_nodes().await.unwrap();
        assert!(!harness.engine.registry().get("n1").unwrap().alive);
        assert!(
            harness.drain().is_empty(),
            "losing a node is not new capacity"
        );

        harness
            .launcher
            .alive
            .lock()
            .unwrap()
            .insert("n1".into(), true);
        harness.engine.checking_nodes().await.unwrap();
        assert!(harness.engine.registry().get("n1").unwrap().alive);
        assert_eq!(harness.drain(), vec![Event::ScheduleJobs]);
    }

    #[tokio::test]
    async fn node_registration_and_heartbeat() {
        let mut harness = Harness::new(&[]);
        assert!(harness.engine.register_node("n9", "127.0.0.1:7009"));
        assert!(!harness.engine.register_node("", "127.0.0.1:7009"));
        assert!(harness.engine.registry().contains("n9"));
        assert_eq!(harness.drain(), vec![Event::CheckNodes]);

        harness.engine.registry().set_alive("n9", false);
        assert_eq!(harness.engine.heartbeat("n9", 2), Some(true));
        assert!(harness.engine.registry().get("n9").unwrap().alive);
        assert_eq!(harness.engine.heartbeat("ghost", 0), None);
    }

    #[tokio::test]
    async fn engine_steps_drive_workers_and_spawned_tasks() {
        let harness = Harness::new(&["n1"]);
        let steps = EngineSteps::start(Arc::clone(&harness.engine));

        let job_id = harness
            .engine
            .submit_job(JobRequest::new(0, "x", 1, 60))
            .await
            .unwrap();
        steps.run(Step::Scheduling).await.unwrap();
        steps.run(Step::LaunchingJobs).await.unwrap();
        assert_eq!(harness.state(job_id).await, JobState::Running);

        steps.run(Step::CheckingNodes).await.unwrap();
        assert!(harness.engine.registry().get("n1").unwrap().alive);

        harness.engine.finish_job(job_id, "n1", 0).await.unwrap();
        steps.run(Step::FinishingJobs).await.unwrap();
        assert_eq!(harness.state(job_id).await, JobState::Terminated);

        steps.run(Step::Waiting).await.unwrap();
        steps.shutdown();
        assert!(matches!(
            steps.run(Step::Scheduling).await,
            Err(ServerError::WorkerGone { .. })
        ));
    }

    #[tokio::test]
    async fn cluster_status_counts_every_phase() {
        let harness = Harness::new(&["n1", "n2"]);
        let waiting = harness.submit(1, 60).await;
        harness.engine.scheduling().await.unwrap();
        harness.engine.launching().await.unwrap();
        harness.engine.finish_job(waiting, "n1", 0).await.unwrap();
        harness.engine.finishing_jobs().await.unwrap();

        let status = harness.engine.cluster_status().await.unwrap();
        assert_eq!(status.nodes_registered, 2);
        assert_eq!(status.jobs_active, 0);
        assert_eq!(status.jobs_finished, 1);
        assert_eq!(status.jobs_waiting, 0);
    }
}
