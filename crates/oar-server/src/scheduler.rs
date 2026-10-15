//! Placeholder scheduling.
//!
//! The real OAR3 scheduler (bestsched / gantt) will implement the [`Scheduler`]
//! trait later. [`FifoScheduler`] is a deliberately simple placeholder: it walks
//! the waiting jobs in submission order and gives each of them the first free
//! nodes it asks for.

use oar_common::types::{Job, NodeId, NodeInfo};

/// One scheduling decision: the nodes granted to a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub job_id: u64,
    pub nodes: Vec<NodeId>,
}

/// Scheduling policy, called from the automaton's *scheduling* step.
pub trait Scheduler: Send + Sync + 'static {
    /// Human readable policy name, for logs.
    fn name(&self) -> &'static str;

    /// Decides which waiting jobs can start on the given free nodes.
    ///
    /// `jobs` is sorted by submission order and `available` only contains
    /// alive nodes that are not held by an active job.
    fn schedule(&self, jobs: &[Job], available: &[NodeInfo]) -> Vec<Assignment>;
}

/// First-in-first-out placeholder scheduler.
#[derive(Debug, Default, Clone, Copy)]
pub struct FifoScheduler;

impl FifoScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Scheduler for FifoScheduler {
    fn name(&self) -> &'static str {
        "fifo-placeholder"
    }

    fn schedule(&self, jobs: &[Job], available: &[NodeInfo]) -> Vec<Assignment> {
        let mut free: Vec<&NodeInfo> = available.iter().collect();
        let mut assignments = Vec::new();

        for job in jobs {
            let wanted = job.request.nodes as usize;
            if wanted == 0 || free.len() < wanted {
                // Not enough resources for this job; a real scheduler would try
                // the next one without starving it, which FIFO does here too.
                continue;
            }
            let granted: Vec<NodeId> = free.drain(..wanted).map(|node| node.id.clone()).collect();
            assignments.push(Assignment {
                job_id: job.id,
                nodes: granted,
            });
        }

        assignments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oar_common::types::JobRequest;

    fn job(id: u64, nodes: u32) -> Job {
        Job::new(id, JobRequest::new(1000, "run", nodes, 60))
    }

    fn nodes(count: usize) -> Vec<NodeInfo> {
        (0..count)
            .map(|i| NodeInfo::new(format!("n{i}"), format!("127.0.0.1:{}", 7000 + i)))
            .collect()
    }

    #[test]
    fn allocates_in_submission_order() {
        let scheduler = FifoScheduler::new();
        let jobs = vec![job(1, 1), job(2, 2)];
        let assignments = scheduler.schedule(&jobs, &nodes(3));
        assert_eq!(
            assignments,
            vec![
                Assignment {
                    job_id: 1,
                    nodes: vec!["n0".into()]
                },
                Assignment {
                    job_id: 2,
                    nodes: vec!["n1".into(), "n2".into()]
                },
            ]
        );
    }

    #[test]
    fn skips_jobs_that_do_not_fit() {
        let scheduler = FifoScheduler::new();
        let jobs = vec![job(1, 3), job(2, 1)];
        let assignments = scheduler.schedule(&jobs, &nodes(2));
        assert_eq!(
            assignments,
            vec![Assignment {
                job_id: 2,
                nodes: vec!["n0".into()]
            }]
        );
    }

    #[test]
    fn does_not_reuse_a_node() {
        let scheduler = FifoScheduler::new();
        let jobs = vec![job(1, 2), job(2, 2)];
        assert_eq!(scheduler.schedule(&jobs, &nodes(3)).len(), 1);
        assert_eq!(
            scheduler.schedule(&jobs, &nodes(4)).len(),
            2,
            "both jobs fit with 4 nodes"
        );
    }

    #[test]
    fn no_available_nodes_means_no_assignment() {
        let scheduler = FifoScheduler::new();
        assert!(scheduler.schedule(&[job(1, 1)], &[]).is_empty());
        assert_eq!(scheduler.name(), "fifo-placeholder");
    }
}
