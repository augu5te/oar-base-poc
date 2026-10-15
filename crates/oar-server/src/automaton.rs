//! The server automaton.
//!
//! The server core cycles over five steps:
//!
//! ```text
//! Scheduling -> LaunchingJobs -> CheckingNodes -> FinishingJobs -> Waiting -> ...
//! ```
//!
//! The next step is decided by an event popped from a queue (pushed by the gRPC
//! services when a client or a node notifies the server) or, when no event
//! arrives before the timeout, by falling back to the *Waiting* step.
//!
//! * *Scheduling* and *LaunchingJobs* are wake-up signals sent to dedicated
//!   worker tasks; the automaton waits for their completion.
//! * *CheckingNodes* and *FinishingJobs* spawn a task and wait for it.
//! * *Waiting* idles until the next event (and emits housekeeping events such as
//!   the periodic node check).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use oar_common::types::{JobId, NodeId};
use tokio::sync::mpsc;

/// The five steps of the server automaton.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    Scheduling,
    LaunchingJobs,
    CheckingNodes,
    FinishingJobs,
    Waiting,
}

impl Step {
    pub fn as_str(self) -> &'static str {
        match self {
            Step::Scheduling => "scheduling",
            Step::LaunchingJobs => "launching_jobs",
            Step::CheckingNodes => "checking_nodes",
            Step::FinishingJobs => "finishing_jobs",
            Step::Waiting => "waiting",
        }
    }
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Events pushed into the server queue by the gRPC services.
///
/// An event is a *wake-up*: the state it refers to always lives in the
/// database, so a step can be replayed safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A client submitted a new job.
    NewJob { job_id: JobId },
    /// The scheduler must run again (a job was submitted, a node came back, or
    /// resources were released). Usually emitted by the engine itself.
    ScheduleJobs,
    /// The scheduling step found jobs to start.
    JobsToLaunch,
    /// A node reported that a job finished.
    JobFinished {
        job_id: JobId,
        node_id: NodeId,
        exit_code: i32,
    },
    /// A node registered, or the periodic node check is due.
    CheckNodes,
}

impl Event {
    /// The step this event wakes up.
    pub fn step(&self) -> Step {
        match self {
            Event::NewJob { .. } | Event::ScheduleJobs => Step::Scheduling,
            Event::JobsToLaunch => Step::LaunchingJobs,
            Event::JobFinished { .. } => Step::FinishingJobs,
            Event::CheckNodes => Step::CheckingNodes,
        }
    }
}

/// Executes one automaton step, returning once the underlying work (worker task
/// or spawned task) has completed.
#[async_trait]
pub trait StepExecutor: Send + Sync + 'static {
    async fn run(&self, step: Step) -> Result<(), crate::ServerError>;
}

/// The automaton loop: event queue + timeout -> step -> wait for the step.
pub struct Automaton {
    executor: Arc<dyn StepExecutor>,
    events: mpsc::UnboundedReceiver<Event>,
    wait: Duration,
}

impl Automaton {
    pub fn new(
        executor: Arc<dyn StepExecutor>,
        events: mpsc::UnboundedReceiver<Event>,
        wait: Duration,
    ) -> Self {
        Self {
            executor,
            events,
            wait,
        }
    }

    /// Pops the next step: the event at the head of the queue, or [`Step::Waiting`]
    /// when nothing arrives within `wait`. `None` means the queue is closed and
    /// the automaton should stop.
    pub async fn next_step(&mut self) -> Option<Step> {
        match tokio::time::timeout(self.wait, self.events.recv()).await {
            Ok(Some(event)) => {
                tracing::debug!(event = ?event, step = %event.step(), "automaton wake-up");
                Some(event.step())
            }
            Ok(None) => None,
            Err(_) => Some(Step::Waiting),
        }
    }

    /// Runs until the event queue is closed (all senders dropped).
    pub async fn run(mut self) {
        while let Some(step) = self.next_step().await {
            if let Err(error) = self.executor.run(step).await {
                tracing::warn!(step = %step, %error, "automaton step failed");
            }
        }
        tracing::info!("event queue closed, automaton stops");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records the steps it is asked to run and can fail on demand.
    #[derive(Default)]
    struct Recorder {
        steps: Mutex<Vec<Step>>,
        fail_on: Mutex<Option<Step>>,
    }

    #[async_trait]
    impl StepExecutor for Recorder {
        async fn run(&self, step: Step) -> Result<(), crate::ServerError> {
            self.steps.lock().unwrap().push(step);
            if *self.fail_on.lock().unwrap() == Some(step) {
                return Err(crate::ServerError::Task("boom".into()));
            }
            Ok(())
        }
    }

    #[test]
    fn events_map_to_their_step() {
        assert_eq!(Event::NewJob { job_id: 1 }.step(), Step::Scheduling);
        assert_eq!(Event::JobsToLaunch.step(), Step::LaunchingJobs);
        assert_eq!(
            Event::JobFinished {
                job_id: 1,
                node_id: "n1".into(),
                exit_code: 0
            }
            .step(),
            Step::FinishingJobs
        );
        assert_eq!(Event::CheckNodes.step(), Step::CheckingNodes);
        assert_eq!(Step::Waiting.to_string(), "waiting");
    }

    #[tokio::test]
    async fn steps_follow_the_event_queue() {
        let (tx, rx) = mpsc::unbounded_channel();
        let recorder = Arc::new(Recorder::default());
        let mut automaton = Automaton::new(recorder.clone(), rx, Duration::from_millis(50));

        tx.send(Event::NewJob { job_id: 1 }).unwrap();
        tx.send(Event::CheckNodes).unwrap();
        tx.send(Event::JobFinished {
            job_id: 1,
            node_id: "n1".into(),
            exit_code: 0,
        })
        .unwrap();
        tx.send(Event::ScheduleJobs).unwrap();
        tx.send(Event::JobsToLaunch).unwrap();

        for expected in [
            Step::Scheduling,
            Step::CheckingNodes,
            Step::FinishingJobs,
            Step::Scheduling,
            Step::LaunchingJobs,
        ] {
            assert_eq!(automaton.next_step().await, Some(expected));
        }
        assert!(
            recorder.steps.lock().unwrap().is_empty(),
            "next_step does not run the step"
        );
    }

    #[tokio::test]
    async fn no_event_falls_back_to_waiting() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let recorder = Arc::new(Recorder::default());
        let mut automaton = Automaton::new(recorder, rx, Duration::from_millis(20));
        assert_eq!(automaton.next_step().await, Some(Step::Waiting));
    }

    #[tokio::test]
    async fn closed_queue_stops_the_automaton() {
        let (tx, rx) = mpsc::unbounded_channel();
        let recorder = Arc::new(Recorder::default());
        let mut automaton = Automaton::new(recorder.clone(), rx, Duration::from_millis(20));
        drop(tx);
        assert_eq!(automaton.next_step().await, None);

        // `run` must return promptly when the queue is closed.
        let (tx, rx) = mpsc::unbounded_channel();
        let automaton = Automaton::new(recorder.clone(), rx, Duration::from_millis(20));
        let handle = tokio::spawn(automaton.run());
        tx.send(Event::CheckNodes).unwrap();
        drop(tx);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*recorder.steps.lock().unwrap(), vec![Step::CheckingNodes]);
    }

    #[tokio::test]
    async fn a_failing_step_does_not_stop_the_automaton() {
        let (tx, rx) = mpsc::unbounded_channel();
        let recorder = Arc::new(Recorder::default());
        *recorder.fail_on.lock().unwrap() = Some(Step::CheckingNodes);
        let automaton = Automaton::new(recorder.clone(), rx, Duration::from_millis(20));

        let handle = tokio::spawn(automaton.run());
        tx.send(Event::CheckNodes).unwrap();
        tx.send(Event::JobsToLaunch).unwrap();
        drop(tx);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *recorder.steps.lock().unwrap(),
            vec![Step::CheckingNodes, Step::LaunchingJobs]
        );
    }
}
