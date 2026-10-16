use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use specta::Type;
use tokio::{
    fs,
    sync::{OnceCell, RwLock},
};

use super::{
    atomics::{Atomic, TaskState},
    frontend::{self, TaskPrepareResp},
    handlers,
    manager::MANAGER,
    runtime::RUNTIME,
    types::{MediaItem, MediaNfo, PopupSelect},
};

use crate::{
    shared::process_err,
    storage::{config, tasks},
    TauriError, TauriResult,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Type)]
#[serde(rename_all = "camelCase")]
pub enum TaskType {
    OpusContent,
    OpusImages,
    AiSummary,
    Subtitles,
    AlbumNfo,
    SingleNfo,
    LiveDanmaku,
    HistoryDanmaku,
    Thumb,
    Video,
    Audio,
    AudioVideo,
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct SubTask {
    pub id: String,
    #[serde(rename = "type")]
    pub task_type: TaskType,
    #[serde(skip_serializing, skip_deserializing, default)]
    task_weak: OnceCell<Weak<Task>>,
}

impl SubTask {
    pub fn reg_task(&self, task: &Arc<Task>) {
        let _ = self.task_weak.set(Arc::downgrade(task));
    }
    pub async fn send(&self, content: u64, chunk: u64) -> Result<()> {
        let Some(task_weak) = self.task_weak.get() else {
            return Ok(());
        };
        let Some(task) = task_weak.upgrade() else {
            return Ok(());
        };

        let now = Instant::now();
        let should_send = {
            let mut last = task.progress_last.write().await;
            let elapsed = last
                .get(&self.id)
                .map(Instant::elapsed)
                .unwrap_or(Duration::MAX);
            if chunk == content || elapsed >= Duration::from_millis(250) {
                last.insert(self.id.to_string(), now);
                true
            } else {
                false
            }
        };

        if !should_send {
            return Ok(());
        }

        let should_persist = {
            let mut last = task.progress_persist.write().await;
            let elapsed = last
                .get(&self.id)
                .map(Instant::elapsed)
                .unwrap_or(Duration::MAX);
            if chunk == content || elapsed >= Duration::from_secs(1) {
                last.insert(self.id.to_string(), now);
                true
            } else {
                false
            }
        };

        /* FRONTEND */
        frontend::progress(&task.id, &self.id, &content, &chunk)?;

        /* BACKEND */
        {
            let mut status = task.status.write().await;
            status.insert(self.id.to_string(), SubTaskStatus { chunk, content });
        }

        /* DATABASE */
        if should_persist {
            let status = task.status.read().await;
            tasks::update_status(&task.id, &status).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct SubTaskStatus {
    pub chunk: u64,
    pub content: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct TaskMeta {
    pub id: String,
    pub ts: u64,
    pub seq: usize,
    pub item: MediaItem,
    #[serde(rename = "type")]
    pub media_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct TaskPrepare {
    pub select: PopupSelect,
    pub subtasks: Vec<SubTask>,
    pub nfo: MediaNfo,
    pub folder: PathBuf,
}

#[derive(Debug, Default)]
pub struct MediaPaths {
    pub video: Option<PathBuf>,
    pub audio: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct TaskHotData {
    pub status: HashMap<String, SubTaskStatus>,
    pub state: TaskState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct TaskView {
    pub meta: TaskMeta,
    pub prepare: TaskPrepare,
    pub hot: TaskHotData,
}

#[derive(Debug)]
pub struct Task {
    pub id: String,
    pub ts: u64,
    pub seq: usize,
    pub item: MediaItem,
    pub media_type: String,

    /* NEED TO PREPARE */
    pub select: RwLock<PopupSelect>,
    pub subtasks: RwLock<Vec<SubTask>>,
    pub nfo: RwLock<MediaNfo>,
    pub folder: RwLock<PathBuf>,
    pub media_paths: RwLock<MediaPaths>,

    /* HOT DATA */
    pub status: RwLock<HashMap<String, SubTaskStatus>>,
    pub progress_last: RwLock<HashMap<String, Instant>>,
    pub progress_persist: RwLock<HashMap<String, Instant>>,
    pub state: Atomic<TaskState>,
    pub retrying: AtomicBool,
}

impl Task {
    pub fn new(value: TaskView) -> Arc<Self> {
        let m = value.meta;
        let p = value.prepare;
        let h = value.hot;
        Arc::new(Self {
            id: m.id,
            ts: m.ts,
            seq: m.seq,
            item: m.item,
            media_type: m.media_type,

            select: RwLock::new(p.select),
            subtasks: RwLock::new(p.subtasks),
            nfo: RwLock::new(p.nfo),
            folder: RwLock::new(p.folder),
            media_paths: RwLock::new(MediaPaths::default()),

            status: RwLock::new(h.status),
            progress_last: RwLock::new(HashMap::new()),
            progress_persist: RwLock::new(HashMap::new()),
            state: Atomic::new(h.state),
            retrying: AtomicBool::new(false),
        })
    }

    pub async fn init(&self) -> Result<()> {
        RUNTIME.ctrl.reg(self.id.clone()).await;
        Ok(())
    }

    pub async fn process_download(self: &Arc<Self>, sid: &str) -> TauriResult<()> {
        let id = &self.id;
        log::info!("Downloading Task#{id}");

        let temp = config::read().temp_dir().join(&**id);
        fs::create_dir_all(&*temp)
            .await
            .context(format!("Failed to create temp folder for {id}"))?;

        let scheduler = MANAGER.get_scheduler(sid).await?;

        let res = handlers::handle_download(scheduler, &temp, self.clone()).await;

        if res.is_err() {
            let _ = fs::remove_dir_all(&*temp).await;
        }

        res
    }

    pub async fn process_postprocess(self: &Arc<Self>, sid: &str) -> TauriResult<()> {
        let id = &self.id;
        log::info!("Postprocessing Task#{id}");

        let temp = config::read().temp_dir().join(&**id);
        let scheduler = MANAGER.get_scheduler(sid).await?;

        let res = handlers::handle_postprocess(scheduler, &temp, self.clone()).await;

        // The output is already written, a leftover temp folder must not fail
        // the task
        if let Err(e) = fs::remove_dir_all(&*temp).await {
            log::warn!("Failed to cleanup temp folder for {id}: {e:#}");
        }

        res
    }

    pub async fn process(self: &Arc<Self>, sid: &str) -> TauriResult<()> {
        self.process_download(sid).await?;
        self.process_postprocess(sid).await
    }

    pub async fn prepare(&self, prepare: &TaskPrepareResp, folder: PathBuf) -> Result<()> {
        let prepare = TaskPrepare {
            select: prepare.select.to_owned(),
            subtasks: prepare.subtasks.to_owned(),
            nfo: prepare.nfo.to_owned(),
            folder: folder.clone(),
        };

        /* BACKEND */
        // The frontend corrects the quality to the one the source supports
        *self.select.write().await = prepare.select.to_owned();
        *self.nfo.write().await = prepare.nfo.to_owned();
        *self.subtasks.write().await = prepare.subtasks.to_owned();
        *self.folder.write().await = folder;

        let mut status = self.status.write().await;
        let next = prepare
            .subtasks
            .iter()
            .map(|subtask| {
                let value = status.get(&subtask.id).cloned().unwrap_or(SubTaskStatus {
                    chunk: 0,
                    content: 0,
                });
                (subtask.id.to_string(), value)
            })
            .collect();
        *status = next;

        /* FRONTEND */
        frontend::task_updated(&self.id, None, Some(&prepare), None)?;

        /* DATABASE */
        tasks::update_prepare(&self.id, &prepare).await?;
        Ok(())
    }

    pub async fn state(&self, state: TaskState) -> Result<()> {
        /* BACKEND */
        self.state.set(state);

        /* FRONTEND */
        frontend::task_updated(&self.id, Some(&state), None, None)?;

        /* DATABASE */
        tasks::update_state(&self.id, state as u8).await?;
        Ok(())
    }

    pub async fn cancel_backlog(&self) -> Result<()> {
        /* BACKEND */
        self.state.set(TaskState::Cancelled);
        MANAGER.remove_backlog(&self.id).await?;

        /* FRONTEND */
        frontend::task_updated(&self.id, None, None, Some(true))?;

        /* DATABASE */
        tasks::delete(&self.id).await?;
        Ok(())
    }

    pub async fn cancel(&self, sid: &str) -> Result<()> {
        // A finished task has no download left to stop. Its cleaners only ask
        // aria2c and ffmpeg about work that is already gone, which costs a
        // round of retries per task.
        let finished = self.state.get() == TaskState::Completed;

        /* BACKEND */
        self.state.set(TaskState::Cancelled);
        MANAGER.remove(sid, Some(&self.id)).await?;
        if !finished {
            RUNTIME.ctrl.get_handle(&self.id).await?.clean_all().await;
        }

        /* FRONTEND */
        frontend::task_updated(&self.id, None, None, Some(true))?;

        /* DATABASE */
        tasks::delete(&self.id).await?;
        Ok(())
    }

    pub async fn restore(self: Arc<Self>, sid: &str) -> Result<()> {
        let scheduler = MANAGER.get_scheduler(sid).await?;
        if !scheduler.interrupted() {
            return Ok(());
        }

        self.retry(sid).await?;
        Ok(())
    }

    pub async fn retry(self: Arc<Self>, sid: &str) -> Result<()> {
        if self
            .retrying
            .compare_exchange(false, true, SeqCst, SeqCst)
            .is_err()
        {
            log::warn!("Task#{} retry already running, ignored", self.id);
            return Ok(());
        }

        let setup = async {
            RUNTIME.ctrl.get_handle(&self.id).await?.clean_all().await;
            RUNTIME.ctrl.reg(self.id.clone()).await;
            self.state(TaskState::Pending).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;

        if let Err(e) = setup {
            self.retrying.store(false, SeqCst);
            return Err(e);
        }

        let id = self.id.clone();
        log::info!("Task#{id} respawned via retry");

        let name = format!("Task#{id} (Retry)");
        let sid = sid.to_string();
        let task = self.clone();
        let fut_task = task.clone();
        let fut = async move {
            fut_task.state(TaskState::Active).await?;
            fut_task.process(&sid).await?;
            fut_task.state(TaskState::Completed).await?;
            Ok::<(), TauriError>(())
        };
        tauri::async_runtime::spawn(async move {
            let result = fut.await.map_err(|e| process_err(e, &name));
            task.retrying.store(false, SeqCst);
            result
        });

        Ok(())
    }
}
