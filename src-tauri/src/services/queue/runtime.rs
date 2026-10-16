use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use specta::Type;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc, LazyLock,
    },
};
use tokio::sync::{broadcast::Sender, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{storage::config, TauriResult};

use super::{
    atomics::{SchedulerState, TaskState},
    manager::MANAGER,
};

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(Runtime::new);

type CleanFn = Box<dyn FnOnce() -> CleanFnFut + Send + Sync + 'static>;

type CleanFnFut = Pin<Box<dyn Future<Output = TauriResult<()>> + Send>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Type)]
#[serde(rename_all = "lowercase")]
pub enum CtrlEvent {
    Pause,
    Resume,
    Cancel,
    Retry,
}

pub struct CtrlHandle {
    pub tx: Sender<CtrlEvent>,
    pub cancel: CancellationToken,
    pub paused: AtomicBool,
    pub cleaners: RwLock<Vec<CleanFn>>,
    pub epoch: AtomicUsize,
}

impl CtrlHandle {
    pub fn is_paused(&self) -> bool {
        self.paused.load(SeqCst)
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub async fn reg_cleaner<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = TauriResult<()>> + Send + Sync + 'static,
    {
        let cleaner = Box::new(move || Box::pin(fut) as CleanFnFut) as CleanFn;
        self.cleaners.write().await.push(cleaner);
    }
    pub async fn clean_all(&self) {
        let cleaners = { self.cleaners.write().await.split_off(0) };
        // The cleaners are registered in the order the work was started: the
        // temp folder cleanup first, the running aria2c download or ffmpeg
        // process last. A download or mux has to be stopped before the files
        // it writes are deleted, otherwise the sidecar keeps writing into a
        // directory that is gone and loses the state it would resume from.
        for c in cleaners.into_iter().rev() {
            if let Err(e) = c().await {
                log::warn!("Failed to run a cleaner: {e:#}");
            }
        }
    }
}

pub struct Ctrl {
    map: RwLock<HashMap<String, Arc<CtrlHandle>>>,
}

impl Ctrl {
    fn new() -> Self {
        Self {
            map: Default::default(),
        }
    }
    pub async fn get_handle(&self, id: &str) -> Result<Arc<CtrlHandle>> {
        self.map
            .read()
            .await
            .get(id)
            .ok_or(anyhow!("Missing ctrl handle for {id}"))
            .cloned()
    }
    pub async fn reg(&self, id: String) {
        let mut map = self.map.write().await;
        map.insert(
            id,
            Arc::new(CtrlHandle {
                tx: Sender::new(8),
                cancel: CancellationToken::new(),
                paused: AtomicBool::new(false),
                cleaners: Default::default(),
                epoch: AtomicUsize::new(0),
            }),
        );
    }
    pub async fn send_task(
        &self,
        sid: Option<&str>,
        id: &str,
        event: &CtrlEvent,
        state: TaskState,
    ) -> Result<()> {
        log::info!("Received event for Task#{id}: {event:?}, state: {state:?}");

        let task = MANAGER.get_task(id).await?;
        let ctrl = self.get_handle(id).await?;

        let Some(sid) = sid else {
            if task.state.get() == TaskState::Backlog {
                ctrl.cancel.cancel();
                task.cancel_backlog().await?;
            }
            return Ok(());
        };

        task.state(state).await?;
        let _ = ctrl.tx.send(event.clone());

        match event {
            CtrlEvent::Cancel => {
                ctrl.cancel.cancel();
                task.cancel(sid).await?;
            }
            CtrlEvent::Retry => {
                ctrl.cancel.cancel();
                ctrl.epoch.fetch_add(1, SeqCst);
                task.retry(sid).await?;
            }
            CtrlEvent::Pause => {
                ctrl.paused.store(true, SeqCst);
            }
            CtrlEvent::Resume => {
                ctrl.paused.store(false, SeqCst);
                task.restore(sid).await?;
            }
        }
        Ok(())
    }
    pub async fn send_scheduler(
        &self,
        sid: &str,
        event: &CtrlEvent,
        state: SchedulerState,
    ) -> Result<()> {
        log::info!("Received event for scheduler#{sid}: {event:?}, state: {state:?}");

        let scheduler = match MANAGER.get_scheduler(sid).await {
            Ok(scheduler) => scheduler,
            Err(e) => {
                // A record that is already gone has nothing left to cancel, a
                // repeated cancel must stay quiet instead of reporting an error
                // that the frontend shows again and again.
                if !matches!(event, CtrlEvent::Cancel) {
                    return Err(e);
                }
                log::warn!("Scheduler#{sid} not found, cancel ignored");
                return Ok(());
            }
        };

        // A finished scheduler has no download left to stop, deleting it only
        // drops its records.
        if matches!(event, CtrlEvent::Cancel) && scheduler.state.get() == SchedulerState::Completed
        {
            return scheduler.discard().await;
        }

        if !matches!(event, CtrlEvent::Resume) {
            scheduler.state(state).await?;
        }

        match event {
            CtrlEvent::Cancel => {
                scheduler.cancel().await?;
            }
            CtrlEvent::Retry => {
                // Re-run every task of the scheduler that is not finished yet
                let scheduler = scheduler.clone();
                tauri::async_runtime::spawn(async move {
                    let _ = scheduler
                        .dispatch()
                        .await
                        .map_err(|e| crate::shared::process_err(e, "dispatch"));
                });
            }
            CtrlEvent::Pause => {
                let list = scheduler.list.read().await.clone();
                for id in list {
                    if let Ok(ctrl) = RUNTIME.ctrl.get_handle(&id).await {
                        ctrl.paused.store(true, SeqCst);
                        let _ = ctrl.tx.send(CtrlEvent::Pause);
                    }
                }
            }
            CtrlEvent::Resume => {
                let list = scheduler.list.read().await.clone();
                for id in list {
                    if let Ok(ctrl) = RUNTIME.ctrl.get_handle(&id).await {
                        ctrl.paused.store(false, SeqCst);
                        let _ = ctrl.tx.send(CtrlEvent::Resume);
                    }
                }
                scheduler.restore().await?;
            }
        }
        Ok(())
    }
}

pub struct Runtime {
    pub download_semaphore: RwLock<Arc<Semaphore>>,
    pub max_conc: usize,
    pub ffmpeg_semaphore: RwLock<Arc<Semaphore>>,
    pub max_ffmpeg: usize,
    pub ctrl: Ctrl,
}

impl Runtime {
    fn new() -> Self {
        let max_conc = config::read().max_conc;
        let max_ffmpeg = config::read().max_ffmpeg;
        Self {
            download_semaphore: RwLock::new(Arc::new(Semaphore::new(max_conc))),
            max_conc,
            ffmpeg_semaphore: RwLock::new(Arc::new(Semaphore::new(max_ffmpeg))),
            max_ffmpeg,
            ctrl: Ctrl::new(),
        }
    }
    pub async fn new_conc(&self, value: usize) {
        if value == self.max_conc {
            return;
        }
        let mut sem = self.download_semaphore.write().await;
        if value > self.max_conc {
            sem.add_permits(value - self.max_conc);
        } else {
            *sem = Arc::new(Semaphore::new(value));
        }
    }
    pub async fn new_ffmpeg_conc(&self, value: usize) {
        if value == self.max_ffmpeg {
            return;
        }
        let mut sem = self.ffmpeg_semaphore.write().await;
        if value > self.max_ffmpeg {
            sem.add_permits(value - self.max_ffmpeg);
        } else {
            *sem = Arc::new(Semaphore::new(value));
        }
    }
}

#[tauri::command(async)]
#[specta::specta]
pub async fn ctrl_event(event: CtrlEvent, sid: Option<&str>, id: Option<&str>) -> TauriResult<()> {
    if let Some(id) = id {
        let state = match event {
            CtrlEvent::Pause => TaskState::Paused,
            CtrlEvent::Cancel => TaskState::Cancelled,
            CtrlEvent::Resume => TaskState::Active,
            CtrlEvent::Retry => TaskState::Pending,
        };
        RUNTIME.ctrl.send_task(sid, id, &event, state).await?;
    } else if let Some(sid) = sid {
        let state = match event {
            CtrlEvent::Pause => SchedulerState::Paused,
            CtrlEvent::Cancel => SchedulerState::Cancelled,
            CtrlEvent::Resume => SchedulerState::Running,
            CtrlEvent::Retry => SchedulerState::Idle,
        };
        RUNTIME.ctrl.send_scheduler(sid, &event, state).await?;
    }
    Ok(())
}

#[tauri::command(async)]
#[specta::specta]
pub async fn open_folder(sid: Option<&str>, id: Option<&str>) -> TauriResult<()> {
    let path = if let Some(sid) = sid {
        let scheduler = MANAGER.get_scheduler(sid).await?;
        Some(scheduler.folder.clone())
    } else if let Some(id) = id {
        let task = MANAGER.get_task(id).await?;
        let folder = task.folder.read().await;
        Some(folder.clone())
    } else {
        None
    };
    let Some(path) = path else {
        return Ok(());
    };
    tauri_plugin_opener::open_path(&*path, None::<&str>)?;
    Ok(())
}
