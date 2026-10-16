use std::{future::Future, path::PathBuf, pin::pin, sync::Arc};

use anyhow::Result;
use serde::Serialize;
use tokio::{sync::RwLock, task::JoinSet};

use specta::Type;

use super::{
    atomics::{Atomic, QueueType, SchedulerState, TaskState},
    frontend,
    manager::MANAGER,
    runtime::{CtrlEvent, RUNTIME},
};

use crate::{
    shared::{get_sec, get_unique_path, process_err},
    storage::{config, schedulers, tasks},
    TauriResult,
};

#[derive(Clone, Copy, Debug)]
enum Phase {
    Download,
    Postprocess,
}

pub struct Scheduler {
    pub sid: String,
    pub ts: i64,
    pub list: RwLock<Vec<String>>,
    pub queue: Atomic<QueueType>,
    pub state: Atomic<SchedulerState>,
    pub folder: PathBuf,
}

#[derive(Debug, Clone, Serialize, Type)]
pub struct SchedulerView {
    pub sid: String,
    pub ts: i64,
    pub list: Vec<String>,
    pub queue: QueueType,
    pub state: SchedulerState,
}

impl SchedulerView {
    pub async fn from(value: &Scheduler) -> Self {
        Self {
            sid: value.sid.clone(),
            ts: value.ts,
            list: value.list.read().await.clone(),
            queue: value.queue.get(),
            state: value.state.get(),
        }
    }
}

impl Scheduler {
    pub fn new(sid: String, list: Vec<String>, top_folder: PathBuf) -> Arc<Self> {
        let folder = if config::read().organize.top_folder {
            let down_dir = config::read().down_dir.clone();
            let path = down_dir.join(&top_folder);
            // An empty naming result would place the download next to the
            // output folder
            if path == down_dir {
                down_dir
            } else {
                get_unique_path(path)
            }
        } else {
            config::read().down_dir.clone()
        };

        Arc::new(Self {
            sid,
            ts: get_sec(),
            list: RwLock::new(list),
            queue: Atomic::new(QueueType::Pending),
            state: Atomic::new(SchedulerState::Idle),
            folder,
        })
    }

    pub fn interrupted(&self) -> bool {
        self.queue.get() == QueueType::Doing && self.state.get() == SchedulerState::Idle
    }

    pub fn resumable(&self) -> bool {
        self.queue.get() == QueueType::Doing
            && matches!(
                self.state.get(),
                SchedulerState::Idle | SchedulerState::Paused
            )
    }

    pub async fn dispatch(self: Arc<Self>) -> TauriResult<()> {
        log::info!("Scheduler#{} dispatch started", self.sid);
        self.state(SchedulerState::Running).await?;
        let mut list = { self.list.read().await.clone() };
        {
            let tasks = MANAGER.tasks.read().await;
            list.sort_by_key(|id| tasks.get(id).map(|task| task.seq).unwrap_or(usize::MAX));
        }
        for id in &list {
            let Ok(task) = MANAGER.get_task(id).await else {
                continue;
            };
            if matches!(
                task.state.get(),
                TaskState::Completed | TaskState::Cancelled
            ) {
                continue;
            }
            task.state(TaskState::Pending).await?;
        }

        let ok_download = self.run_phase(Phase::Download, &list).await;

        let mut active = Vec::new();
        for id in &list {
            let Ok(task) = MANAGER.get_task(id).await else {
                continue;
            };
            if task.state.get() == TaskState::Active {
                active.push(id.clone());
            }
        }

        let ok_postprocess = self.run_phase(Phase::Postprocess, &active).await;
        let ok = ok_download && ok_postprocess;

        if ok {
            MANAGER
                .move_scheduler(&self.sid, QueueType::Complete)
                .await?;
            self.state(SchedulerState::Completed).await?;
            // The record of a finished scheduler is temporary, it only lives in
            // the memory of this session.
            if let Err(e) = self.drop_rows().await {
                log::error!("Scheduler#{} failed to drop its records: {e:#}", self.sid);
            }
        } else {
            log::error!(
                "Scheduler#{} not fully completed, currently in {:?}, state {:?}",
                self.sid,
                self.queue.get(),
                self.state.get(),
            );
            self.state(SchedulerState::Failed).await?;
        }
        Ok(())
    }

    async fn run_phase(self: &Arc<Self>, phase: Phase, list: &[String]) -> bool {
        let mut set = JoinSet::new();

        for id in list {
            let Ok(task) = MANAGER.get_task(id).await else {
                continue;
            };
            if matches!(
                task.state.get(),
                TaskState::Completed | TaskState::Cancelled
            ) {
                continue;
            }
            if matches!(phase, Phase::Postprocess) && task.state.get() != TaskState::Active {
                continue;
            }

            let sem = match phase {
                Phase::Download => RUNTIME.download_semaphore.read().await.clone(),
                Phase::Postprocess => RUNTIME.ffmpeg_semaphore.read().await.clone(),
            };
            let permit = sem.acquire_owned().await;
            let this = self.clone();
            set.spawn(async move {
                // Extend the lifetime of permit.
                let _permit = permit;
                task.state(TaskState::Active).await?;
                MANAGER.move_scheduler(&this.sid, QueueType::Doing).await?;

                let res = match phase {
                    Phase::Download => task.process_download(&this.sid).await,
                    Phase::Postprocess => task.process_postprocess(&this.sid).await,
                };
                match &res {
                    Ok(_) if matches!(phase, Phase::Postprocess) => {
                        task.state(TaskState::Completed).await
                    }
                    Err(e) => {
                        process_err(e, &format!("Task#{} ({phase:?})", task.id));
                        task.state(TaskState::Failed).await
                    }
                    _ => Ok(()),
                }?;
                res
            });
        }

        let mut ok = true;
        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(_)) => (),
                Ok(Err(_)) => ok = false,
                Err(e) => {
                    log::error!("Join error: {e:?}");
                    process_err(e, "Dispatch");
                    ok = false;
                }
            }
        }
        ok
    }

    pub async fn try_join<T: Default>(
        &self,
        id: &str,
        sub_id: &str,
        fut: impl Future<Output = TauriResult<T>>,
    ) -> TauriResult<T> {
        let ctrl = RUNTIME.ctrl.get_handle(id).await?;
        if ctrl.is_cancelled() {
            return Ok(Default::default());
        }
        let mut rx = ctrl.tx.subscribe();

        let res = tokio::select! {
            r = &mut pin!(fut) => r,
            _ = ctrl.cancel.cancelled() => return Ok(Default::default()),
        };

        if let Err(e) = &res {
            log::error!("Task#{id} failed: {e:#}");
            // A task that is being cancelled fails on purpose, the frontend
            // must not be told about it.
            if !ctrl.is_cancelled() {
                frontend::error(id, Some(sub_id), e)?;
            }
            return res;
        }

        if !ctrl.is_paused() {
            return res;
        }
        log::info!("Subtask#{sub_id}, Task#{id} is paused, waiting for resume...");
        tokio::select! {
            Ok(CtrlEvent::Resume) = rx.recv() => {
                log::info!(
                    "Subtask#{sub_id}, Task#{id} resumed"
                );
            }
            _ = ctrl.cancel.cancelled() => {
                log::info!(
                    "Subtask#{sub_id}, Task#{id} cancelled while paused"
                );
            }
        }

        res
    }

    pub async fn queue(&self, queue: QueueType) -> Result<()> {
        /* BACKEND */
        self.queue.set(queue);

        /* FRONTEND */
        frontend::scheduler_updated(&self.sid, None, Some(&queue), None, None)?;

        /* DATABASE */
        schedulers::update_queue(&self.sid, queue as u8).await?;
        Ok(())
    }

    pub async fn state(&self, state: SchedulerState) -> Result<()> {
        /* BACKEND */
        self.state.set(state);

        /* FRONTEND */
        frontend::scheduler_updated(&self.sid, Some(&state), None, None, None)?;

        /* DATABASE */
        schedulers::update_state(&self.sid, state as u8).await?;
        Ok(())
    }

    // The rows go away instead of being kept up to date: the record of a
    // finished scheduler is temporary and must not come back after a restart.
    async fn drop_rows(&self) -> Result<()> {
        let list = self.list.read().await.clone();
        tasks::delete_many(&list).await?;
        schedulers::delete(&self.sid).await?;
        Ok(())
    }

    // Deletes the record of a finished scheduler. Nothing is left to stop, so
    // the tasks are not cancelled, only their records are dropped.
    pub async fn discard(self: Arc<Self>) -> Result<()> {
        let list = self.list.read().await.clone();

        /* BACKEND */
        {
            let mut tasks = MANAGER.tasks.write().await;
            for id in &list {
                tasks.remove(id);
            }
        }

        /* FRONTEND */
        for id in &list {
            frontend::task_updated(id, None, None, Some(true))?;
        }

        if let Err(e) = MANAGER.remove(&self.sid, None).await {
            log::error!("Scheduler#{} failed to remove itself: {e:#}", self.sid);
            return Err(e);
        }

        /* FRONTEND */
        frontend::scheduler_updated(&self.sid, None, None, None, Some(true))?;

        /* DATABASE */
        if let Err(e) = self.drop_rows().await {
            log::error!("Scheduler#{} failed to drop its records: {e:#}", self.sid);
        }
        Ok(())
    }

    pub async fn cancel(self: Arc<Self>) -> Result<()> {
        /* BACKEND */
        self.state(SchedulerState::Cancelled).await?;

        // `task.cancel()` will deadlock if we iterate while holding the lock.
        let list = self.list.read().await.clone();
        let mut first_error: Option<anyhow::Error> = None;
        for id in list {
            let Ok(task) = MANAGER.get_task(&id).await else {
                continue;
            };
            if let Err(e) = task.cancel(&self.sid).await {
                log::error!("Scheduler#{} failed to cancel Task#{id}: {e:#}", self.sid);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        if let Err(e) = MANAGER.remove(&self.sid, None).await {
            log::error!("Scheduler#{} failed to remove itself: {e:#}", self.sid);
            if first_error.is_none() {
                first_error = Some(e);
            }
        }

        /* BACKEND */
        if let Err(e) = frontend::scheduler_updated(&self.sid, None, None, None, Some(true)) {
            log::error!(
                "Scheduler#{} failed to notify cancellation: {e:#}",
                self.sid
            );
            if first_error.is_none() {
                first_error = Some(e);
            }
        }

        /* DATABASE */
        if let Err(e) = schedulers::delete(&self.sid).await {
            log::error!(
                "Scheduler#{} failed to delete database record: {e:#}",
                self.sid
            );
            if first_error.is_none() {
                first_error = Some(e);
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub async fn restore(self: Arc<Self>) -> Result<()> {
        if !self.resumable() {
            return Ok(());
        }
        let sid = self.sid.clone();
        log::info!("Scheduler#{sid} respawned via restore");

        tauri::async_runtime::spawn(async move {
            let _ = self
                .dispatch()
                .await
                .map_err(|e| process_err(e, "dispatch"));
        });
        Ok(())
    }
}
