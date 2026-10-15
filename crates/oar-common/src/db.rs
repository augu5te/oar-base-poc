//! Placeholder persistence layer.
//!
//! Every database operation used by the base system goes through the
//! [`Database`] trait so the PostgreSQL implementation of OAR3 can be plugged in
//! later without touching the engine. [`MemoryDatabase`] is the in-memory
//! placeholder used by the daemons and their tests.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::types::{Job, JobFilter, JobId, JobState, TransitionError};

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("job {0} not found")]
    NotFound(JobId),
    #[error("job {id} already exists")]
    AlreadyExists { id: JobId },
    #[error("backend error: {0}")]
    Backend(String),
    #[error(transparent)]
    Transition(#[from] TransitionError),
}

pub type DbResult<T> = Result<T, DbError>;

/// Persistence contract of the OAR base system (placeholder for PostgreSQL).
#[async_trait]
pub trait Database: Send + Sync + 'static {
    /// Reserves a fresh, monotonic job id.
    async fn allocate_job_id(&self) -> DbResult<JobId>;

    async fn insert_job(&self, job: Job) -> DbResult<()>;

    async fn get_job(&self, job_id: JobId) -> DbResult<Option<Job>>;

    /// Persists the whole job; fails if the job does not exist yet.
    async fn update_job(&self, job: &Job) -> DbResult<()>;

    async fn list_jobs(&self, filter: &JobFilter) -> DbResult<Vec<Job>>;

    /// Reads-modifies-writes a job through a validated state transition.
    async fn transition_job(&self, job_id: JobId, to: JobState) -> DbResult<Job> {
        let mut job = self
            .get_job(job_id)
            .await?
            .ok_or(DbError::NotFound(job_id))?;
        job.transition_to(to)?;
        self.update_job(&job).await?;
        Ok(job)
    }

    async fn count_jobs(&self, filter: &JobFilter) -> DbResult<usize> {
        Ok(self.list_jobs(filter).await?.len())
    }
}

/// In-memory placeholder implementation of [`Database`].
#[derive(Debug, Default)]
pub struct MemoryDatabase {
    next_id: AtomicU64,
    jobs: RwLock<HashMap<JobId, Job>>,
}

impl MemoryDatabase {
    pub fn new() -> Self {
        Self::with_first_id(1)
    }

    /// Creates a database whose first allocated id is `first_id`.
    pub fn with_first_id(first_id: JobId) -> Self {
        Self {
            next_id: AtomicU64::new(first_id),
            jobs: RwLock::new(HashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.jobs.read().expect("db lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<JobId, Job>> {
        self.jobs.read().expect("db lock poisoned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<JobId, Job>> {
        self.jobs.write().expect("db lock poisoned")
    }
}

#[async_trait]
impl Database for MemoryDatabase {
    async fn allocate_job_id(&self) -> DbResult<JobId> {
        Ok(self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn insert_job(&self, job: Job) -> DbResult<()> {
        let mut jobs = self.write();
        if jobs.contains_key(&job.id) {
            return Err(DbError::AlreadyExists { id: job.id });
        }
        jobs.insert(job.id, job);
        Ok(())
    }

    async fn get_job(&self, job_id: JobId) -> DbResult<Option<Job>> {
        Ok(self.read().get(&job_id).cloned())
    }

    async fn update_job(&self, job: &Job) -> DbResult<()> {
        let mut jobs = self.write();
        if !jobs.contains_key(&job.id) {
            return Err(DbError::NotFound(job.id));
        }
        jobs.insert(job.id, job.clone());
        Ok(())
    }

    async fn list_jobs(&self, filter: &JobFilter) -> DbResult<Vec<Job>> {
        let mut jobs: Vec<Job> = self
            .read()
            .values()
            .filter(|j| filter.matches(j))
            .cloned()
            .collect();
        jobs.sort_by_key(|j| j.id);
        Ok(jobs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JobRequest;

    fn request(user_uid: u32) -> JobRequest {
        JobRequest::new(user_uid, "echo hi", 1, 60)
    }

    async fn insert(db: &MemoryDatabase, user_uid: u32) -> JobId {
        let id = db.allocate_job_id().await.unwrap();
        db.insert_job(Job::new(id, request(user_uid)))
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn ids_are_monotonic_and_start_at_one() {
        let db = MemoryDatabase::new();
        assert_eq!(db.allocate_job_id().await.unwrap(), 1);
        assert_eq!(db.allocate_job_id().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn insert_get_update_roundtrip() {
        let db = MemoryDatabase::new();
        let id = insert(&db, 1000).await;

        let mut job = db.get_job(id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Waiting);
        job.transition_to(JobState::ToLaunch).unwrap();
        db.update_job(&job).await.unwrap();
        assert_eq!(
            db.get_job(id).await.unwrap().unwrap().state,
            JobState::ToLaunch
        );

        assert!(db.get_job(999).await.unwrap().is_none());
        assert_eq!(db.len(), 1);
        assert!(!db.is_empty());
    }

    #[tokio::test]
    async fn duplicate_insert_is_refused() {
        let db = MemoryDatabase::new();
        let id = insert(&db, 1000).await;
        let err = db
            .insert_job(Job::new(id, request(1000)))
            .await
            .unwrap_err();
        assert!(matches!(err, DbError::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn update_or_transition_missing_job_fails() {
        let db = MemoryDatabase::new();
        assert!(matches!(
            db.update_job(&Job::new(7, request(0))).await,
            Err(DbError::NotFound(7))
        ));
        assert!(matches!(
            db.transition_job(7, JobState::ToLaunch).await,
            Err(DbError::NotFound(7))
        ));
    }

    #[tokio::test]
    async fn transition_job_validates_the_state_machine() {
        let db = MemoryDatabase::new();
        let id = insert(&db, 1000).await;
        assert!(matches!(
            db.transition_job(id, JobState::Running).await,
            Err(DbError::Transition(_))
        ));
        let job = db.transition_job(id, JobState::ToLaunch).await.unwrap();
        assert_eq!(job.state, JobState::ToLaunch);
    }

    #[tokio::test]
    async fn listing_filters_and_sorts() {
        let db = MemoryDatabase::new();
        let first = insert(&db, 1000).await;
        let second = insert(&db, 1001).await;
        db.transition_job(second, JobState::ToLaunch).await.unwrap();

        assert_eq!(db.list_jobs(&JobFilter::all()).await.unwrap().len(), 2);
        assert_eq!(
            db.list_jobs(&JobFilter::state(JobState::Waiting))
                .await
                .unwrap()[0]
                .id,
            first
        );
        assert_eq!(
            db.list_jobs(&JobFilter::user(1001)).await.unwrap()[0].id,
            second
        );
        assert_eq!(
            db.count_jobs(&JobFilter::state(JobState::Waiting))
                .await
                .unwrap(),
            1
        );
    }
}
