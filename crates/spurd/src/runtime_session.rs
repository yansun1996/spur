// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};

use crate::executor::RunningJob;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeLaunchSpec {
    pub job_id: u32,
    pub script: String,
    pub work_dir: String,
    pub name: String,
    pub user: String,
    pub node: String,
    pub environment: std::collections::HashMap<String, String>,
    pub stdout_path: String,
    pub stderr_path: String,
    pub stdin_path: String,
    pub cpus: u32,
    pub memory_mb: u64,
    pub cpu_ids: Vec<u32>,
    #[serde(default)]
    pub gpu_devices: Vec<u32>,
    pub open_mode: Option<String>,
    pub uid: u32,
    pub gid: u32,
    pub partition: String,
    pub nodelist: String,
    pub memlock: RuntimeMemlock,
    #[serde(default)]
    pub container: Option<crate::executor::ContainerLaunchConfig>,
    #[serde(default)]
    pub host_device_plan: Option<spur_devices::inject::HostInjectionPlan>,
    #[serde(default)]
    pub container_rootfs_mode: Option<crate::container::RootfsMode>,
    #[serde(default)]
    pub hooks: spur_core::config::HooksConfig,
    #[serde(default)]
    pub plugstack_path: String,
    #[serde(default)]
    pub controller_addr: String,
    #[serde(default)]
    pub reporting_node: String,
    #[serde(default)]
    pub run_attempt: u32,
    #[serde(default)]
    pub capability: String,
    #[serde(default)]
    pub allocation_only: bool,
    #[serde(default)]
    pub pmix_config: Option<spur_core::config::MpiConfig>,
    #[serde(default)]
    pub pmix_plan: Option<spur_core::mpi::PmixLaunchPlan>,
    #[serde(default)]
    pub pmix_multi_task: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeMemlock {
    Unlimited,
    Inherit,
    Bytes(u64),
}

impl From<spur_core::config::MemlockLimit> for RuntimeMemlock {
    fn from(limit: spur_core::config::MemlockLimit) -> Self {
        match limit {
            spur_core::config::MemlockLimit::Unlimited => RuntimeMemlock::Unlimited,
            spur_core::config::MemlockLimit::Inherit => RuntimeMemlock::Inherit,
            spur_core::config::MemlockLimit::Bytes(value) => RuntimeMemlock::Bytes(value),
        }
    }
}

impl From<RuntimeMemlock> for spur_core::config::MemlockLimit {
    fn from(limit: RuntimeMemlock) -> Self {
        match limit {
            RuntimeMemlock::Unlimited => spur_core::config::MemlockLimit::Unlimited,
            RuntimeMemlock::Inherit => spur_core::config::MemlockLimit::Inherit,
            RuntimeMemlock::Bytes(value) => spur_core::config::MemlockLimit::Bytes(value),
        }
    }
}

impl TryFrom<&crate::executor::JobLaunchConfig> for RuntimeLaunchSpec {
    type Error = String;

    fn try_from(config: &crate::executor::JobLaunchConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            job_id: config.job_id,
            script: config.script.clone(),
            work_dir: config.work_dir.clone(),
            name: config.name.clone(),
            user: config.user.clone(),
            node: config.node.clone(),
            environment: config.environment.clone(),
            stdout_path: config.stdout_path.clone(),
            stderr_path: config.stderr_path.clone(),
            stdin_path: config.stdin_path.clone(),
            cpus: config.cpus,
            memory_mb: config.memory_mb,
            gpu_devices: config.gpu_devices.clone(),
            cpu_ids: config.cpu_ids.clone(),
            open_mode: config.open_mode.clone(),
            uid: config.uid,
            gid: config.gid,
            partition: config.partition.clone(),
            nodelist: config.nodelist.clone(),
            memlock: config.memlock.into(),
            container: config.container.clone(),
            host_device_plan: config.host_device_plan.clone(),
            container_rootfs_mode: None,
            hooks: spur_core::config::HooksConfig::default(),
            plugstack_path: String::new(),
            controller_addr: String::new(),
            reporting_node: String::new(),
            run_attempt: config.run_attempt,
            capability: String::new(),
            allocation_only: false,
            pmix_config: None,
            pmix_plan: None,
            pmix_multi_task: config.pmix_multi_task,
        })
    }
}

impl RuntimeLaunchSpec {
    pub fn into_launch_config(self) -> crate::executor::JobLaunchConfig {
        crate::executor::JobLaunchConfig {
            job_id: self.job_id,
            run_attempt: self.run_attempt,
            script: self.script,
            work_dir: self.work_dir,
            name: self.name,
            user: self.user,
            node: self.node,
            array_job_id: None,
            array_task_id: None,
            environment: self.environment,
            stdout_path: self.stdout_path,
            stderr_path: self.stderr_path,
            stdin_path: self.stdin_path,
            cpus: self.cpus,
            memory_mb: self.memory_mb,
            gpu_devices: self.gpu_devices,
            cpu_ids: self.cpu_ids,
            open_mode: self.open_mode,
            uid: self.uid,
            gid: self.gid,
            container: self.container,
            prolog_script: None,
            partition: self.partition,
            nodelist: self.nodelist,
            host_device_plan: self.host_device_plan,
            memlock: self.memlock.into(),
            io_mode: crate::executor::LaunchIo::File,
            pmix_multi_task: self.pmix_multi_task,
        }
    }
}

const DESCRIPTOR_FILE: &str = "descriptor.json";
const OBLIGATION_FILE: &str = "obligations.jsonl";
const FAILURE_FILE: &str = "failure.txt";
const FORMAT_VERSION: u32 = 1;
pub const PROTOCOL_VERSION: u32 = 2;
const MIN_PROTOCOL_VERSION: u32 = PROTOCOL_VERSION - 1;
const STEP_OUTPUT_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum RuntimeRequest {
    Hello {
        protocol_version: u32,
        capability: String,
        spurd_instance_id: String,
        run_attempt: u32,
    },
    QueryState,
    SignalAllocation {
        signal: i32,
    },
    LaunchStep {
        step: Box<RuntimeStepLaunchSpec>,
    },
    SignalStep {
        step_id: u32,
        signal: i32,
    },
    LaunchPty {
        pty: RuntimePtyLaunchSpec,
    },
    WritePty {
        data: Vec<u8>,
    },
    ResizePty {
        winsize: RuntimeWindowSize,
    },
    SignalPty {
        signal: i32,
    },
    ReadPty {
        offset: u64,
    },
    BeginTeardown,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum RuntimeResponse {
    Hello {
        protocol_version: u32,
        job_id: u32,
        run_attempt: u32,
    },
    State {
        job_id: u32,
        run_attempt: u32,
        active: bool,
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    Acknowledged,
    StepCompleted {
        step_id: u32,
        exit_code: i32,
        stdout: String,
        stderr: String,
    },
    PtyOutput {
        start_offset: u64,
        data: Vec<u8>,
        eof: bool,
        exit_code: Option<i32>,
    },
    Rejected {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "notification", rename_all = "snake_case")]
pub enum AgentNotification {
    RuntimeSessionCompleted {
        job_id: u32,
        run_attempt: u32,
        exit_code: i32,
        signal: i32,
        epilog_failed: bool,
        capability: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AgentNotificationResponse {
    /// spurd forwarded the completion to the controller; safe to prune.
    Acknowledged,
    /// spurd released its local tracking but could not reach the
    /// controller; leave the durable record for the next startup scan.
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    pub job_id: u32,
    pub run_attempt: u32,
    pub active: bool,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "obligation", rename_all = "snake_case")]
pub enum RuntimeObligation {
    ExitObserved { exit_code: i32, signal: i32 },
    CompletionAcknowledged,
    ResourcesReleased,
}

pub struct RuntimeObligationLog {
    path: PathBuf,
}

impl RuntimeObligationLog {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn append(&self, obligation: &RuntimeObligation) -> io::Result<()> {
        let mut entry = serde_json::to_vec(obligation).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("serialize runtime obligation: {error}"),
            )
        })?;
        entry.push(b'\n');
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&entry)?;
        file.sync_data()
    }

    pub fn read(&self) -> io::Result<Vec<RuntimeObligation>> {
        let contents = match fs::read_to_string(&self.path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        contents
            .lines()
            .enumerate()
            .map(|(line, entry)| {
                serde_json::from_str(entry).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid runtime obligation at line {}: {error}", line + 1),
                    )
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeStepLaunchSpec {
    pub step_id: u32,
    pub program: String,
    pub args: Vec<String>,
    pub work_dir: String,
    pub environment: std::collections::HashMap<String, String>,
    pub uid: u32,
    pub gid: u32,
    pub memlock: RuntimeMemlock,
    #[serde(default)]
    pub pmix: Option<RuntimePmixStepSpec>,
    #[serde(default)]
    pub task_epilog: Option<RuntimeTaskEpilogSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTaskEpilogSpec {
    pub script: String,
    pub job_id: u32,
    pub work_dir: String,
    pub uid: u32,
    pub gid: u32,
    pub partition: String,
    pub nodelist: String,
    pub gpu_devices: Vec<u32>,
    pub cpus: u32,
    pub memory_mb: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePmixStepSpec {
    pub config: spur_core::config::MpiConfig,
    pub plan: spur_core::mpi::PmixLaunchPlan,
    pub command: Vec<String>,
    pub task_offset: u32,
    pub tasks_on_node: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePtyLaunchSpec {
    pub argv: Vec<String>,
    pub work_dir: String,
    pub environment: std::collections::HashMap<String, String>,
    pub uid: u32,
    pub gid: u32,
    pub memlock: RuntimeMemlock,
    pub winsize: Option<RuntimeWindowSize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeWindowSize {
    pub rows: u16,
    pub cols: u16,
    pub xpixel: u16,
    pub ypixel: u16,
}

impl From<RuntimeWindowSize> for crate::pty::WindowSize {
    fn from(value: RuntimeWindowSize) -> Self {
        Self {
            rows: value.rows,
            cols: value.cols,
            xpixel: value.xpixel,
            ypixel: value.ypixel,
        }
    }
}

struct RuntimeStep {
    pid: u32,
    completion: Arc<RuntimeStepCompletion>,
}

struct RuntimeStepPmixResources {
    host: Arc<crate::mpi_plugin::MpiPluginHost>,
    job_id: u32,
    step_dir: PathBuf,
    files: Vec<PathBuf>,
}

impl Drop for RuntimeStepPmixResources {
    fn drop(&mut self) {
        if let Err(error) = self.host.release_pmix_server(self.job_id) {
            tracing::warn!(job_id = self.job_id, %error, "PMIx runtime step release failed");
        }
        for file in &self.files {
            let _ = fs::remove_file(file);
        }
        if !self.step_dir.as_os_str().is_empty() {
            let _ = fs::remove_dir(&self.step_dir);
        }
    }
}

fn write_runtime_step_script(
    path: &Path,
    command: &[String],
    uid: u32,
    gid: u32,
) -> io::Result<()> {
    let command = shlex::try_join(command.iter().map(String::as_str))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    crate::executor::write_job_scratch(path, &format!("#!/bin/bash\nexec {command}\n"), uid, gid)
        .map_err(io::Error::other)
}

const PTY_OUTPUT_LIMIT: usize = 1024 * 1024;
const RUNTIME_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const CONTROL_LINE_LIMIT: usize = 64 * 1024;
const CONTROL_REQUEST_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Sibling of the runtime session store root, not inside it, so directory
/// scans over session state (`discover_live`, `prune_finalized`) never see it.
pub(crate) const AGENT_NOTIFY_SOCKET_NAME: &str = "agent.sock";

/// `read_line` with no cap on line length; a peer that never sends `\n` grows
/// `line` unboundedly. Bound it to a sane control-protocol frame size.
pub(crate) async fn read_line_bounded<R>(reader: &mut R, line: &mut String) -> io::Result<usize>
where
    R: AsyncBufReadExt + Unpin,
{
    let n = reader
        .take(CONTROL_LINE_LIMIT as u64)
        .read_line(line)
        .await?;
    if n >= CONTROL_LINE_LIMIT && !line.ends_with('\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control line exceeded maximum length",
        ));
    }
    Ok(n)
}

struct RuntimePtyBuffer {
    start_offset: u64,
    data: VecDeque<u8>,
    eof: bool,
    exit_code: Option<i32>,
}

impl RuntimePtyBuffer {
    fn append(&mut self, data: &[u8]) {
        self.data.extend(data);
        let excess = self.data.len().saturating_sub(PTY_OUTPUT_LIMIT);
        self.data.drain(..excess);
        self.start_offset += excess as u64;
    }

    fn read_from(&self, offset: u64) -> RuntimePtyOutput {
        let offset = offset.max(self.start_offset);
        let skip = (offset - self.start_offset) as usize;
        RuntimePtyOutput {
            start_offset: offset,
            data: self.data.iter().skip(skip).copied().collect(),
            eof: self.eof,
            exit_code: self.exit_code,
        }
    }
}

struct RuntimePty {
    master: Arc<AsyncFd<std::os::fd::OwnedFd>>,
    child_pid: i32,
    buffer: Mutex<RuntimePtyBuffer>,
    output_ready: Notify,
}

#[derive(Debug, Clone)]
pub struct RuntimePtyOutput {
    pub start_offset: u64,
    pub data: Vec<u8>,
    pub eof: bool,
    pub exit_code: Option<i32>,
}

struct RuntimeStepCompletion {
    result: Mutex<Option<RuntimeStepResult>>,
    notify: Notify,
}

impl RuntimeStepCompletion {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    async fn wait(&self) -> RuntimeStepResult {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.result.lock().await.clone() {
                return result;
            }
            notified.await;
        }
    }

    async fn complete(&self, result: RuntimeStepResult) {
        *self.result.lock().await = Some(result);
        self.notify.notify_waiters();
    }
}

#[derive(Default)]
struct RuntimeSteps {
    active: HashMap<u32, RuntimeStep>,
    completed: HashMap<u32, Arc<RuntimeStepCompletion>>,
    cancelled: HashSet<u32>,
}

pub struct RuntimeSession {
    job: Mutex<RunningJob>,
    snapshot: Arc<Mutex<RuntimeSnapshot>>,
    teardown_started: AtomicBool,
    launch_gate: Mutex<()>,
    steps: Arc<Mutex<RuntimeSteps>>,
    pty: Mutex<Option<Arc<RuntimePty>>>,
    environment: std::collections::HashMap<String, String>,
    pmix_host: Mutex<Option<Arc<crate::mpi_plugin::MpiPluginHost>>>,
}

impl RuntimeSession {
    pub fn new(job: RunningJob, job_id: u32, run_attempt: u32) -> Self {
        Self::with_environment(job, job_id, run_attempt, std::collections::HashMap::new())
    }

    pub fn with_environment(
        job: RunningJob,
        job_id: u32,
        run_attempt: u32,
        environment: std::collections::HashMap<String, String>,
    ) -> Self {
        Self::with_environment_and_pmix(job, job_id, run_attempt, environment, None)
    }

    pub fn with_environment_and_pmix(
        job: RunningJob,
        job_id: u32,
        run_attempt: u32,
        environment: std::collections::HashMap<String, String>,
        pmix_host: Option<Arc<crate::mpi_plugin::MpiPluginHost>>,
    ) -> Self {
        Self {
            job: Mutex::new(job),
            snapshot: Arc::new(Mutex::new(RuntimeSnapshot {
                job_id,
                run_attempt,
                active: true,
                exit_code: None,
                signal: None,
            })),
            teardown_started: AtomicBool::new(false),
            launch_gate: Mutex::new(()),
            steps: Arc::new(Mutex::new(RuntimeSteps::default())),
            pty: Mutex::new(None),
            environment,
            pmix_host: Mutex::new(pmix_host),
        }
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        self.snapshot.lock().await.clone()
    }

    async fn take_cgroup(&self) -> Option<PathBuf> {
        self.job.lock().await.take_cgroup()
    }

    pub async fn poll_completion(&self) -> io::Result<()> {
        let (allocation_only, completed) = {
            let mut job = self.job.lock().await;
            let allocation_only = job.is_allocation_only();
            let completed = job.try_wait().map_err(io::Error::other)?;
            (allocation_only, completed)
        };
        if let Some((exit_code, signal)) = completed {
            let mut snapshot = self.snapshot.lock().await;
            snapshot.active = false;
            snapshot.exit_code = Some(exit_code);
            snapshot.signal = Some(signal);
            return Ok(());
        }
        if !allocation_only || !self.teardown_started.load(Ordering::Acquire) {
            return Ok(());
        }
        let steps_finished = self.steps.lock().await.active.is_empty();
        let pty_finished = match self.pty.lock().await.clone() {
            Some(pty) => pty.buffer.lock().await.exit_code.is_some(),
            None => true,
        };
        if steps_finished && pty_finished {
            let mut snapshot = self.snapshot.lock().await;
            snapshot.active = false;
            snapshot.signal = Some(nix::sys::signal::Signal::SIGTERM as i32);
        }
        Ok(())
    }

    pub async fn signal(&self, signal: i32) -> io::Result<()> {
        let signal = nix::sys::signal::Signal::try_from(signal).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid signal: {error}"),
            )
        })?;
        {
            let job = self.job.lock().await;
            job.kill_signal(signal).map_err(io::Error::other)?;
            if signal == nix::sys::signal::Signal::SIGKILL {
                if let Some(cgroup_path) = job.cgroup_path() {
                    // Belt-and-suspenders: reaches descendants that detached
                    // from the signaled process group (e.g. via setsid).
                    if let Err(error) = crate::executor::cgroup_kill(cgroup_path) {
                        tracing::warn!(%error, path = %cgroup_path.display(), "cgroup.kill failed");
                    }
                }
            }
        }
        let steps = self.steps.lock().await;
        for step in steps.active.values() {
            signal_process_group(step.pid, signal);
        }
        drop(steps);
        let pty = self.pty.lock().await.clone();
        if let Some(pty) = pty {
            crate::pty::signal_foreground(
                pty.master.get_ref().as_raw_fd(),
                pty.child_pid,
                signal as i32,
            )
            .map_err(io::Error::other)?;
        }
        Ok(())
    }

    pub async fn begin_teardown(&self) {
        let launch_gate = self.launch_gate.lock().await;
        self.teardown_started.store(true, Ordering::Release);
        drop(launch_gate);
        {
            let job = self.job.lock().await;
            if let Err(error) = job.kill_signal(nix::sys::signal::Signal::SIGTERM) {
                tracing::warn!(%error, "failed to terminate runtime batch process during teardown");
            }
        }
        let steps = self.steps.lock().await;
        for step in steps.active.values() {
            signal_process_group(step.pid, nix::sys::signal::Signal::SIGTERM);
        }
        drop(steps);
        let pty = self.pty.lock().await.clone();
        if let Some(pty) = pty {
            if let Err(error) = crate::pty::signal_foreground(
                pty.master.get_ref().as_raw_fd(),
                pty.child_pid,
                nix::sys::signal::Signal::SIGTERM as i32,
            ) {
                tracing::warn!(%error, "failed to terminate runtime PTY during teardown");
            }
        }
    }

    async fn launch_step(&self, step: RuntimeStepLaunchSpec) -> io::Result<RuntimeStepResult> {
        if self.teardown_started.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "runtime session is tearing down",
            ));
        }
        {
            let steps = self.steps.lock().await;
            if steps.cancelled.contains(&step.step_id) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    format!("step {} was cancelled before launch", step.step_id),
                ));
            }
            if let Some(completion) = steps
                .active
                .get(&step.step_id)
                .map(|step| step.completion.clone())
                .or_else(|| steps.completed.get(&step.step_id).cloned())
            {
                drop(steps);
                return Ok(completion.wait().await);
            }
        }
        let mut step = step;
        let mut pmix_resources = None;
        if let Some(pmix) = step.pmix.clone() {
            let host = self.pmix_host(&pmix.config).await;
            host.start_pmix_server_and_verify(&pmix.plan)
                .map_err(io::Error::other)?;
            pmix_resources = Some(RuntimeStepPmixResources {
                host: host.clone(),
                job_id: pmix.plan.job_id,
                step_dir: PathBuf::new(),
                files: Vec::new(),
            });
            let mut environment = step.environment.clone();
            if pmix.tasks_on_node > 1 {
                let per_rank = crate::mpi_plugin::pmix_setup_fork_env_for_node_tasks(
                    &host,
                    &pmix.plan,
                    pmix.task_offset,
                    pmix.tasks_on_node,
                )
                .map_err(io::Error::other)?;
                let step_dir = crate::executor::prepare_step_script_dir(
                    &step.work_dir,
                    pmix.plan.job_id,
                    step.uid,
                    step.gid,
                )
                .map_err(io::Error::other)?;
                let user_script = step_dir.join(format!("cmd_{}.sh", step.step_id));
                let wrapper = step_dir.join(format!("wrapper_{}.sh", step.step_id));
                write_runtime_step_script(&user_script, &pmix.command, step.uid, step.gid)?;
                let wrapper_contents = spur_core::task_launch::build_multi_task_pmix_wrapper(
                    user_script.to_string_lossy().as_ref(),
                    pmix.tasks_on_node,
                    &per_rank,
                    Some(&environment),
                )
                .map_err(io::Error::other)?;
                crate::executor::write_job_scratch(&wrapper, &wrapper_contents, step.uid, step.gid)
                    .map_err(io::Error::other)?;
                step.program = "bash".into();
                step.args = vec![wrapper.to_string_lossy().into_owned()];
                let resources = pmix_resources
                    .as_mut()
                    .ok_or_else(|| io::Error::other("missing PMIx step resources"))?;
                resources.step_dir = step_dir;
                resources.files = vec![user_script, wrapper];
            } else {
                crate::mpi_plugin::apply_pmix_setup_fork_env(
                    &host,
                    &pmix.plan,
                    pmix.task_offset,
                    &mut environment,
                )
                .map_err(io::Error::other)?;
            }
            step.environment = environment;
        }
        let mut command = tokio::process::Command::new(&step.program);
        command
            .args(&step.args)
            .current_dir(&step.work_dir)
            .process_group(0)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (key, value) in &step.environment {
            command.env(key, value);
        }
        let memlock = step.memlock;
        let priv_drop = crate::privdrop::PrivDrop::resolve_if_needed(step.uid, step.gid);
        unsafe {
            command.pre_exec(move || {
                crate::executor::apply_memlock(memlock.into());
                if let Some(ref priv_drop) = priv_drop {
                    priv_drop
                        .apply()
                        .map_err(|error| io::Error::from_raw_os_error(error as i32))?;
                }
                Ok(())
            });
        }
        let launch_gate = self.launch_gate.lock().await;
        if self.teardown_started.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "runtime session is tearing down",
            ));
        }
        let mut steps = self.steps.lock().await;
        if steps.cancelled.contains(&step.step_id) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("step {} was cancelled before launch", step.step_id),
            ));
        }
        if let Some(completion) = steps
            .active
            .get(&step.step_id)
            .map(|step| step.completion.clone())
            .or_else(|| steps.completed.get(&step.step_id).cloned())
        {
            drop(steps);
            return Ok(completion.wait().await);
        }
        let mut child = command.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| io::Error::other("runtime step has no pid"))?;
        let completion = Arc::new(RuntimeStepCompletion::new());
        steps.active.insert(
            step.step_id,
            RuntimeStep {
                pid,
                completion: completion.clone(),
            },
        );
        drop(steps);
        drop(launch_gate);
        let steps = self.steps.clone();
        let task_epilog = step.task_epilog.clone();
        tokio::spawn(async move {
            let _pmix_resources = pmix_resources;
            let stdout = child
                .stdout
                .take()
                .map(|stdout| tokio::spawn(read_step_output(stdout)));
            let stderr = child
                .stderr
                .take()
                .map(|stderr| tokio::spawn(read_step_output(stderr)));
            let result = match child.wait().await {
                Ok(status) => RuntimeStepResult {
                    exit_code: spur_core::process::shell_exit_code(&status),
                    stdout: join_step_output(stdout).await,
                    stderr: join_step_output(stderr).await,
                },
                Err(error) => RuntimeStepResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: error.to_string(),
                },
            };
            if let Some(epilog) = task_epilog {
                let context = spur_core::hooks::HookContext {
                    job_id: epilog.job_id,
                    work_dir: epilog.work_dir,
                    uid: epilog.uid,
                    gid: epilog.gid,
                    partition: epilog.partition,
                    nodelist: epilog.nodelist,
                    script_context: "epilog_task".into(),
                    gpu_devices: epilog.gpu_devices,
                    cpus: epilog.cpus,
                    memory_mb: epilog.memory_mb,
                };
                if let Err(error) = spur_core::hooks::run_hook(&epilog.script, &context).await {
                    tracing::warn!(%error, "runtime task epilog failed");
                }
            }
            let mut steps = steps.lock().await;
            let Some(completion) = steps
                .active
                .remove(&step.step_id)
                .map(|step| step.completion)
            else {
                return;
            };
            steps.completed.insert(step.step_id, completion.clone());
            drop(steps);
            completion.complete(result).await;
        });
        Ok(completion.wait().await)
    }

    async fn pmix_host(
        &self,
        config: &spur_core::config::MpiConfig,
    ) -> Arc<crate::mpi_plugin::MpiPluginHost> {
        let mut host = self.pmix_host.lock().await;
        host.get_or_insert_with(|| Arc::new(crate::mpi_plugin::MpiPluginHost::new(config.clone())))
            .clone()
    }

    async fn signal_step(&self, step_id: u32, signal: i32) -> io::Result<()> {
        let signal = nix::sys::signal::Signal::try_from(signal).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid signal: {error}"),
            )
        })?;
        let mut steps = self.steps.lock().await;
        if let Some(step) = steps.active.get(&step_id) {
            signal_process_group(step.pid, signal);
        } else {
            steps.cancelled.insert(step_id);
        }
        Ok(())
    }

    async fn launch_pty(&self, spec: RuntimePtyLaunchSpec) -> io::Result<()> {
        let launch_gate = self.launch_gate.lock().await;
        if self.teardown_started.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "runtime session is tearing down",
            ));
        }
        let allocation_only = self.job.lock().await.is_allocation_only();
        let mut pty_slot = self.pty.lock().await;
        if pty_slot.is_some() {
            return Ok(());
        }
        let (master, slave) = crate::pty::openpty_with_winsize(
            spec.winsize.map(crate::pty::WindowSize::from).as_ref(),
        )
        .map_err(io::Error::other)?;
        let shell = if spec.argv.is_empty() {
            if std::path::Path::new("/bin/bash").exists() {
                vec!["/bin/bash".to_string()]
            } else {
                vec!["/bin/sh".to_string()]
            }
        } else {
            spec.argv.clone()
        };
        let mut command = tokio::process::Command::new(&shell[0]);
        command
            .args(&shell[1..])
            .current_dir(if spec.work_dir.is_empty() {
                "/tmp"
            } else {
                &spec.work_dir
            })
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        for (key, value) in &spec.environment {
            command.env(key, value);
        }
        let memlock = spec.memlock;
        let master_fd = master.as_raw_fd();
        let slave_fd = slave.as_raw_fd();
        let priv_drop = crate::privdrop::PrivDrop::resolve_if_needed(spec.uid, spec.gid);
        unsafe {
            command.pre_exec(move || {
                crate::pty::pty_pre_exec(slave_fd, master_fd)?;
                crate::executor::apply_memlock(memlock.into());
                if let Some(ref priv_drop) = priv_drop {
                    priv_drop
                        .apply()
                        .map_err(|error| io::Error::from_raw_os_error(error as i32))?;
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        let child_pid = match child.id() {
            Some(pid) => pid as i32,
            None => {
                let _ = child.start_kill();
                return Err(io::Error::other("runtime PTY has no pid"));
            }
        };
        drop(slave);
        // A failure past this point leaves an untracked orphan unless we kill
        // it ourselves — nothing else has a handle on `child` yet.
        if let Err(error) = nix::fcntl::fcntl(
            &master,
            nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK),
        ) {
            let _ = child.start_kill();
            return Err(io::Error::other(error));
        }
        let master = match AsyncFd::new(master) {
            Ok(master) => master,
            Err(error) => {
                let _ = child.start_kill();
                return Err(error);
            }
        };
        let pty = Arc::new(RuntimePty {
            master: Arc::new(master),
            child_pid,
            buffer: Mutex::new(RuntimePtyBuffer {
                start_offset: 0,
                data: VecDeque::new(),
                eof: false,
                exit_code: None,
            }),
            output_ready: Notify::new(),
        });
        *pty_slot = Some(pty.clone());
        drop(pty_slot);
        drop(launch_gate);
        tokio::spawn(read_runtime_pty(pty.clone()));
        let snapshot = self.snapshot.clone();
        tokio::spawn(async move {
            let exit_code = match child.wait().await {
                Ok(status) => spur_core::process::shell_exit_code(&status),
                Err(error) => {
                    tracing::warn!(%error, "runtime PTY wait failed");
                    1
                }
            };
            let mut buffer = pty.buffer.lock().await;
            buffer.exit_code = Some(exit_code);
            drop(buffer);
            pty.output_ready.notify_waiters();
            if allocation_only {
                let mut snapshot = snapshot.lock().await;
                snapshot.active = false;
                snapshot.exit_code = Some(exit_code);
                snapshot.signal = Some(0);
            }
        });
        Ok(())
    }

    async fn write_pty(&self, data: &[u8]) -> io::Result<()> {
        let pty =
            self.pty.lock().await.clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "runtime PTY is not running")
            })?;
        write_runtime_pty(&pty.master, data).await
    }

    async fn resize_pty(&self, winsize: RuntimeWindowSize) -> io::Result<()> {
        let pty =
            self.pty.lock().await.clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "runtime PTY is not running")
            })?;
        crate::pty::resize(pty.master.get_ref().as_raw_fd(), &winsize.into())
            .map_err(io::Error::other)
    }

    async fn signal_pty(&self, signal: i32) -> io::Result<()> {
        let pty =
            self.pty.lock().await.clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "runtime PTY is not running")
            })?;
        crate::pty::signal_foreground(pty.master.get_ref().as_raw_fd(), pty.child_pid, signal)
            .map_err(io::Error::other)
    }

    async fn read_pty(&self, offset: u64) -> io::Result<RuntimePtyOutput> {
        let pty =
            self.pty.lock().await.clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "runtime PTY is not running")
            })?;
        let output = pty.buffer.lock().await.read_from(offset);
        Ok(output)
    }
}

async fn read_runtime_pty(pty: Arc<RuntimePty>) {
    let mut read_buffer = [0u8; 8192];
    loop {
        let mut guard = match pty.master.readable().await {
            Ok(guard) => guard,
            Err(error) => {
                tracing::warn!(%error, "runtime PTY readiness failed");
                break;
            }
        };
        let result = guard.try_io(|fd| {
            let count = unsafe {
                libc::read(
                    fd.get_ref().as_raw_fd(),
                    read_buffer.as_mut_ptr().cast(),
                    read_buffer.len(),
                )
            };
            if count < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count as usize)
            }
        });
        match result {
            Ok(Ok(0)) => break,
            Ok(Err(error)) if error.raw_os_error() == Some(libc::EIO) => break,
            Ok(Ok(count)) => {
                pty.buffer.lock().await.append(&read_buffer[..count]);
                pty.output_ready.notify_waiters();
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "runtime PTY read failed");
                break;
            }
            Err(_) => {}
        }
    }
    let mut buffer = pty.buffer.lock().await;
    buffer.eof = true;
    drop(buffer);
    pty.output_ready.notify_waiters();
}

async fn write_runtime_pty(master: &AsyncFd<std::os::fd::OwnedFd>, data: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        let mut guard = master.writable().await?;
        match guard.try_io(|fd| {
            let count = unsafe {
                libc::write(
                    fd.get_ref().as_raw_fd(),
                    data[written..].as_ptr().cast(),
                    data.len() - written,
                )
            };
            if count < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count as usize)
            }
        }) {
            Ok(Ok(count)) => written += count,
            Ok(Err(error)) => return Err(error),
            Err(_) => continue,
        }
    }
    Ok(())
}

async fn read_step_output<R>(mut reader: R) -> String
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut truncated = false;
    loop {
        let count = match reader.read(&mut buffer).await {
            Ok(count) => count,
            Err(error) => return format!("runtime output read failed: {error}"),
        };
        if count == 0 {
            break;
        }
        let remaining = STEP_OUTPUT_LIMIT.saturating_sub(output.len());
        let retained = count.min(remaining);
        output.extend_from_slice(&buffer[..retained]);
        truncated |= retained != count;
    }
    if truncated {
        output.extend_from_slice(b"\n[spur runtime output truncated]\n");
    }
    String::from_utf8_lossy(&output).into_owned()
}

async fn join_step_output(output: Option<tokio::task::JoinHandle<String>>) -> String {
    match output {
        Some(output) => output
            .await
            .unwrap_or_else(|error| format!("runtime output task failed: {error}")),
        None => String::new(),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeStepResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

fn signal_process_group(pid: u32, signal: nix::sys::signal::Signal) {
    let process = nix::unistd::Pid::from_raw(pid as i32);
    if nix::sys::signal::killpg(process, signal).is_err() {
        let _ = nix::sys::signal::kill(process, signal);
    }
}

impl RuntimeSnapshot {
    pub fn response(&self) -> RuntimeResponse {
        RuntimeResponse::State {
            job_id: self.job_id,
            run_attempt: self.run_attempt,
            active: self.active,
            exit_code: self.exit_code,
            signal: self.signal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionDescriptor {
    pub format_version: u32,
    pub job_id: u32,
    pub run_attempt: u32,
    pub pid: u32,
    pub process_start_ticks: u64,
    pub socket_path: PathBuf,
    pub cgroup_path: PathBuf,
    #[serde(default)]
    pub capability: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub uid: u32,
    #[serde(default)]
    pub gid: u32,
    #[serde(default)]
    pub work_dir: String,
}

impl RuntimeSessionDescriptor {
    pub fn new(
        job_id: u32,
        run_attempt: u32,
        pid: u32,
        process_start_ticks: u64,
        socket_path: PathBuf,
        cgroup_path: PathBuf,
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            job_id,
            run_attempt,
            pid,
            process_start_ticks,
            socket_path,
            cgroup_path,
            capability: uuid::Uuid::new_v4().to_string(),
            owner: String::new(),
            uid: 0,
            gid: 0,
            work_dir: String::new(),
        }
    }
}

pub(crate) fn record_resources_released(descriptor: &RuntimeSessionDescriptor) -> io::Result<()> {
    let session_dir = descriptor.socket_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "runtime socket path has no session directory",
        )
    })?;
    let obligations = RuntimeObligationLog::new(session_dir.join(OBLIGATION_FILE));
    if obligations
        .read()?
        .iter()
        .any(|obligation| matches!(obligation, RuntimeObligation::ResourcesReleased))
    {
        return prune_finalized_session(session_dir, &obligations).map(|_| ());
    }
    obligations.append(&RuntimeObligation::ResourcesReleased)?;
    prune_finalized_session(session_dir, &obligations).map(|_| ())
}

fn finalized_obligations(obligations: &[RuntimeObligation]) -> bool {
    let mut exit_observed = false;
    let mut completion_acknowledged = false;
    let mut resources_released = false;

    for obligation in obligations {
        match obligation {
            RuntimeObligation::ExitObserved { .. } => {
                exit_observed = true;
                completion_acknowledged = false;
            }
            RuntimeObligation::CompletionAcknowledged if exit_observed => {
                completion_acknowledged = true;
            }
            RuntimeObligation::ResourcesReleased => resources_released = true,
            RuntimeObligation::CompletionAcknowledged => {}
        }
    }

    completion_acknowledged && resources_released
}

fn prune_finalized_session(
    session_dir: &Path,
    obligations: &RuntimeObligationLog,
) -> io::Result<bool> {
    if !finalized_obligations(&obligations.read()?) {
        return Ok(false);
    }
    fs::remove_dir_all(session_dir)?;
    Ok(true)
}

pub fn validate_hello(
    descriptor: &RuntimeSessionDescriptor,
    capability: &str,
    expected_capability: &str,
    protocol_version: u32,
    run_attempt: u32,
) -> RuntimeResponse {
    if !(MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION).contains(&protocol_version) {
        return RuntimeResponse::Rejected {
            message: format!(
                "runtime protocol {protocol_version} is incompatible with {MIN_PROTOCOL_VERSION}..={PROTOCOL_VERSION}"
            ),
        };
    }
    if expected_capability.is_empty()
        || capability.len() != expected_capability.len()
        || !bool::from(subtle::ConstantTimeEq::ct_eq(
            capability.as_bytes(),
            expected_capability.as_bytes(),
        ))
    {
        return RuntimeResponse::Rejected {
            message: "runtime capability rejected".into(),
        };
    }
    if run_attempt != descriptor.run_attempt {
        return RuntimeResponse::Rejected {
            message: "runtime attempt is stale".into(),
        };
    }
    RuntimeResponse::Hello {
        protocol_version,
        job_id: descriptor.job_id,
        run_attempt: descriptor.run_attempt,
    }
}

pub async fn accept_hello(
    listener: &UnixListener,
    descriptor: &RuntimeSessionDescriptor,
    expected_capability: &str,
) -> io::Result<(UnixStream, String)> {
    let (stream, _) = listener.accept().await?;
    accept_hello_stream(stream, descriptor, expected_capability).await
}

async fn accept_hello_stream(
    stream: UnixStream,
    descriptor: &RuntimeSessionDescriptor,
    expected_capability: &str,
) -> io::Result<(UnixStream, String)> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    read_line_bounded(&mut reader, &mut line).await?;
    let request: RuntimeRequest = serde_json::from_str(&line).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid runtime request: {error}"),
        )
    })?;
    let RuntimeRequest::Hello {
        protocol_version,
        capability,
        spurd_instance_id,
        run_attempt,
    } = request
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "runtime connection did not begin with hello",
        ));
    };
    let response = validate_hello(
        descriptor,
        &capability,
        expected_capability,
        protocol_version,
        run_attempt,
    );
    let accepted = matches!(response, RuntimeResponse::Hello { .. });
    let mut stream = reader.into_inner();
    let response = serde_json::to_vec(&response).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("encode runtime response: {error}"),
        )
    })?;
    stream.write_all(&response).await?;
    stream.write_all(b"\n").await?;
    if !accepted {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime hello rejected",
        ));
    }
    Ok((stream, spurd_instance_id))
}

pub async fn query_state(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
) -> io::Result<RuntimeSnapshot> {
    match runtime_request(descriptor, spurd_instance_id, RuntimeRequest::QueryState).await? {
        RuntimeResponse::State {
            job_id,
            run_attempt,
            active,
            exit_code,
            signal,
        } if job_id == descriptor.job_id && run_attempt == descriptor.run_attempt => {
            Ok(RuntimeSnapshot {
                job_id,
                run_attempt,
                active,
                exit_code,
                signal,
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime state identity mismatch",
        )),
    }
}

pub async fn signal_allocation(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    signal: i32,
) -> io::Result<()> {
    match runtime_request(
        descriptor,
        spurd_instance_id,
        RuntimeRequest::SignalAllocation { signal },
    )
    .await?
    {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, message))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime signal response was invalid",
        )),
    }
}

pub async fn shutdown_allocation(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
) -> io::Result<()> {
    match runtime_request(descriptor, spurd_instance_id, RuntimeRequest::Shutdown).await? {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, message))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime shutdown response was invalid",
        )),
    }
}

pub(crate) async fn launch_step(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    step: RuntimeStepLaunchSpec,
) -> io::Result<RuntimeStepResult> {
    let (mut reader, mut writer, _) = runtime_connect(descriptor, spurd_instance_id).await?;
    let step_id = step.step_id;
    write_request(
        &mut writer,
        &RuntimeRequest::LaunchStep {
            step: Box::new(step),
        },
    )
    .await?;
    match read_response(&mut reader).await? {
        RuntimeResponse::StepCompleted {
            step_id: response_step_id,
            exit_code,
            stdout,
            stderr,
        } if response_step_id == step_id => Ok(RuntimeStepResult {
            exit_code,
            stdout,
            stderr,
        }),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime step response was invalid",
        )),
    }
}

pub(crate) async fn signal_step(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    step_id: u32,
    signal: i32,
) -> io::Result<()> {
    let (mut reader, mut writer, _) = runtime_connect(descriptor, spurd_instance_id).await?;
    write_request(&mut writer, &RuntimeRequest::SignalStep { step_id, signal }).await?;
    match read_response(&mut reader).await? {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime step signal response was invalid",
        )),
    }
}

pub(crate) async fn launch_pty(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    pty: RuntimePtyLaunchSpec,
) -> io::Result<()> {
    match runtime_request(
        descriptor,
        spurd_instance_id,
        RuntimeRequest::LaunchPty { pty },
    )
    .await?
    {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime PTY launch response was invalid",
        )),
    }
}

pub(crate) async fn write_pty(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    data: Vec<u8>,
) -> io::Result<()> {
    match runtime_request(
        descriptor,
        spurd_instance_id,
        RuntimeRequest::WritePty { data },
    )
    .await?
    {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime PTY write response was invalid",
        )),
    }
}

pub(crate) async fn resize_pty(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    winsize: RuntimeWindowSize,
) -> io::Result<()> {
    match runtime_request(
        descriptor,
        spurd_instance_id,
        RuntimeRequest::ResizePty { winsize },
    )
    .await?
    {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime PTY resize response was invalid",
        )),
    }
}

pub(crate) async fn signal_pty(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    signal: i32,
) -> io::Result<()> {
    match runtime_request(
        descriptor,
        spurd_instance_id,
        RuntimeRequest::SignalPty { signal },
    )
    .await?
    {
        RuntimeResponse::Acknowledged => Ok(()),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime PTY signal response was invalid",
        )),
    }
}

pub(crate) async fn read_pty(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    offset: u64,
) -> io::Result<RuntimePtyOutput> {
    match runtime_request(
        descriptor,
        spurd_instance_id,
        RuntimeRequest::ReadPty { offset },
    )
    .await?
    {
        RuntimeResponse::PtyOutput {
            start_offset,
            data,
            eof,
            exit_code,
        } => Ok(RuntimePtyOutput {
            start_offset,
            data,
            eof,
            exit_code,
        }),
        RuntimeResponse::Rejected { message } => Err(io::Error::other(message)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime PTY read response was invalid",
        )),
    }
}

async fn runtime_request(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    request: RuntimeRequest,
) -> io::Result<RuntimeResponse> {
    let (mut reader, mut writer, protocol_version) =
        runtime_connect(descriptor, spurd_instance_id).await?;
    if protocol_version < PROTOCOL_VERSION
        && matches!(
            request,
            RuntimeRequest::LaunchPty { .. }
                | RuntimeRequest::WritePty { .. }
                | RuntimeRequest::ResizePty { .. }
                | RuntimeRequest::SignalPty { .. }
                | RuntimeRequest::ReadPty { .. }
        )
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "runtime protocol v1 does not support resumable PTY operations",
        ));
    }
    write_request(&mut writer, &request).await?;
    read_response(&mut reader).await
}

async fn runtime_connect(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
) -> io::Result<(
    BufReader<tokio::net::unix::OwnedReadHalf>,
    tokio::net::unix::OwnedWriteHalf,
    u32,
)> {
    let mut last_error = None;
    for protocol_version in (MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION).rev() {
        let stream = UnixStream::connect(&descriptor.socket_path).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        match runtime_hello(
            &mut reader,
            &mut writer,
            descriptor,
            spurd_instance_id.clone(),
            protocol_version,
        )
        .await
        {
            Ok(negotiated) => return Ok((reader, writer, negotiated)),
            Err(error)
                if error.kind() == io::ErrorKind::PermissionDenied
                    && protocol_version_rejected(&error) =>
            {
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "runtime has no compatible local protocol version",
        )
    }))
}

fn protocol_version_rejected(error: &io::Error) -> bool {
    error.to_string().starts_with("runtime protocol ")
        && error.to_string().contains(" is incompatible with ")
}

async fn runtime_hello(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    protocol_version: u32,
) -> io::Result<u32> {
    let hello = RuntimeRequest::Hello {
        protocol_version,
        capability: descriptor.capability.clone(),
        spurd_instance_id,
        run_attempt: descriptor.run_attempt,
    };
    write_request(writer, &hello).await?;
    match read_response(reader).await? {
        RuntimeResponse::Hello {
            protocol_version,
            job_id,
            run_attempt,
        } if job_id == descriptor.job_id
            && run_attempt == descriptor.run_attempt
            && (MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION).contains(&protocol_version) =>
        {
            Ok(protocol_version)
        }
        RuntimeResponse::Rejected { message } => {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, message))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime hello identity mismatch",
        )),
    }
}

async fn write_request(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    request: &RuntimeRequest,
) -> io::Result<()> {
    let request = serde_json::to_vec(request).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("encode runtime request: {error}"),
        )
    })?;
    writer.write_all(&request).await?;
    writer.write_all(b"\n").await
}

async fn read_response(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> io::Result<RuntimeResponse> {
    let mut line = String::new();
    read_line_bounded(reader, &mut line).await?;
    serde_json::from_str(&line).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid runtime response: {error}"),
        )
    })
}

const AGENT_NOTIFY_ATTEMPTS: u32 = 5;
const AGENT_NOTIFY_RETRY_GAP: std::time::Duration = std::time::Duration::from_secs(1);
const AGENT_NOTIFY_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Push this session's completion to spurd. `None` means the caller must
/// leave the durable record for spurd's next-startup recovery scan.
pub(crate) async fn notify_agent_completion(
    agent_socket: &Path,
    notification: &AgentNotification,
) -> Option<AgentNotificationResponse> {
    for attempt in 1..=AGENT_NOTIFY_ATTEMPTS {
        let outcome = tokio::time::timeout(
            AGENT_NOTIFY_ATTEMPT_TIMEOUT,
            try_notify_agent(agent_socket, notification),
        )
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "spurd did not answer",
            ))
        });
        match outcome {
            Ok(response) => return Some(response),
            Err(error) => {
                tracing::warn!(
                    attempt,
                    %error,
                    "failed to notify spurd of runtime session completion"
                );
                if attempt < AGENT_NOTIFY_ATTEMPTS {
                    tokio::time::sleep(AGENT_NOTIFY_RETRY_GAP).await;
                }
            }
        }
    }
    None
}

async fn try_notify_agent(
    agent_socket: &Path,
    notification: &AgentNotification,
) -> io::Result<AgentNotificationResponse> {
    let stream = UnixStream::connect(agent_socket).await?;
    let (reader, mut writer) = stream.into_split();
    let payload = serde_json::to_vec(notification).map_err(io::Error::other)?;
    writer.write_all(&payload).await?;
    writer.write_all(b"\n").await?;
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    read_line_bounded(&mut reader, &mut line).await?;
    serde_json::from_str(&line).map_err(io::Error::other)
}

pub async fn serve_control(stream: UnixStream, session: &RuntimeSession) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        // A connected client that never sends another request (hung, or
        // deliberately holding the socket open) must not pin this task
        // forever; the handshake already bounds how long accepting a new
        // connection can take, this bounds each subsequent request on it.
        let read = tokio::time::timeout(
            CONTROL_REQUEST_IDLE_TIMEOUT,
            read_line_bounded(&mut reader, &mut line),
        )
        .await
        .map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "control connection idle timeout")
        })??;
        if read == 0 {
            return Ok(());
        }
        let request: RuntimeRequest = serde_json::from_str(&line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid runtime request: {error}"),
            )
        })?;
        let response = match request {
            RuntimeRequest::QueryState => session.snapshot().await.response(),
            RuntimeRequest::BeginTeardown | RuntimeRequest::Shutdown => {
                session.begin_teardown().await;
                RuntimeResponse::Acknowledged
            }
            RuntimeRequest::SignalAllocation { signal } => match session.signal(signal).await {
                Ok(()) => RuntimeResponse::Acknowledged,
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::LaunchStep { step } => match session.launch_step(*step.clone()).await {
                Ok(result) => RuntimeResponse::StepCompleted {
                    step_id: step.step_id,
                    exit_code: result.exit_code,
                    stdout: result.stdout,
                    stderr: result.stderr,
                },
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::SignalStep { step_id, signal } => {
                match session.signal_step(step_id, signal).await {
                    Ok(()) => RuntimeResponse::Acknowledged,
                    Err(error) => RuntimeResponse::Rejected {
                        message: error.to_string(),
                    },
                }
            }
            RuntimeRequest::LaunchPty { pty } => match session.launch_pty(pty).await {
                Ok(()) => RuntimeResponse::Acknowledged,
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::WritePty { data } => match session.write_pty(&data).await {
                Ok(()) => RuntimeResponse::Acknowledged,
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::ResizePty { winsize } => match session.resize_pty(winsize).await {
                Ok(()) => RuntimeResponse::Acknowledged,
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::SignalPty { signal } => match session.signal_pty(signal).await {
                Ok(()) => RuntimeResponse::Acknowledged,
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::ReadPty { offset } => match session.read_pty(offset).await {
                Ok(output) => RuntimeResponse::PtyOutput {
                    start_offset: output.start_offset,
                    data: output.data,
                    eof: output.eof,
                    exit_code: output.exit_code,
                },
                Err(error) => RuntimeResponse::Rejected {
                    message: error.to_string(),
                },
            },
            RuntimeRequest::Hello { .. } => RuntimeResponse::Rejected {
                message: "runtime hello is only valid as the first request".into(),
            },
        };
        let response = serde_json::to_vec(&response).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("encode runtime response: {error}"),
            )
        })?;
        reader.get_mut().write_all(&response).await?;
        reader.get_mut().write_all(b"\n").await?;
    }
}

pub async fn run_supervisor(
    listener: UnixListener,
    descriptor: RuntimeSessionDescriptor,
    session: Arc<RuntimeSession>,
) -> io::Result<()> {
    let mut poll_interval = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = poll_interval.tick() => {
                session.poll_completion().await?;
                if !session.snapshot().await.active {
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let session = session.clone();
                        let descriptor = descriptor.clone();
                        tokio::spawn(serve_supervisor_connection(stream, descriptor, session));
                    }
                    Err(error) => {
                        tracing::warn!(%error, "runtime session connection handshake failed");
                    }
                }
            }
        }
    }
}

async fn serve_supervisor_connection(
    stream: UnixStream,
    descriptor: RuntimeSessionDescriptor,
    session: Arc<RuntimeSession>,
) {
    let capability = descriptor.capability.clone();
    match tokio::time::timeout(
        RUNTIME_HANDSHAKE_TIMEOUT,
        accept_hello_stream(stream, &descriptor, &capability),
    )
    .await
    {
        Ok(Ok((stream, _))) => {
            if let Err(error) = serve_control(stream, &session).await {
                tracing::warn!(%error, "runtime session control connection failed");
            }
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "runtime session connection handshake failed");
        }
        Err(_) => {
            tracing::warn!("runtime session connection handshake timed out");
        }
    }
}

pub async fn run_process(args: &[String]) -> anyhow::Result<i32> {
    if args.len() != 4 {
        anyhow::bail!(
            "usage: spurd __runtime-session <state-dir> <job-id> <attempt> <launch-spec>"
        );
    }
    let state_dir = PathBuf::from(&args[0]);
    let job_id: u32 = args[1]
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid job id: {error}"))?;
    let run_attempt: u32 = args[2]
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid run attempt: {error}"))?;
    let mut launch_spec: RuntimeLaunchSpec = serde_json::from_slice(&std::fs::read(&args[3])?)?;
    if launch_spec.job_id != job_id {
        anyhow::bail!("runtime launch spec job id does not match process arguments");
    }
    let agent_socket = state_dir.join(AGENT_NOTIFY_SOCKET_NAME);
    let store = RuntimeSessionStore::new(state_dir);
    let session_dir = store.session_dir(job_id, run_attempt);
    let obligations = store.obligations(job_id, run_attempt);
    let socket_path = session_dir.join("runtime.sock");
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }
    let pid = std::process::id();
    let mut descriptor = RuntimeSessionDescriptor::new(
        job_id,
        run_attempt,
        pid,
        process_start_ticks(pid)?,
        socket_path.clone(),
        PathBuf::new(),
    );
    if !launch_spec.capability.is_empty() {
        descriptor.capability = launch_spec.capability.clone();
    }
    descriptor.owner = launch_spec.user.clone();
    descriptor.uid = launch_spec.uid;
    descriptor.gid = launch_spec.gid;
    descriptor.work_dir = launch_spec.work_dir.clone();
    store.publish(&descriptor)?;
    let pmix_guard = prepare_pmix_launch(&mut launch_spec)?;
    let listener = UnixListener::bind(&socket_path)?;
    let runtime_environment = launch_spec.environment.clone();
    let container_rootfs_mode = launch_spec.container_rootfs_mode.clone();
    let hooks = launch_spec.hooks.clone();
    let spank = load_runtime_spank(&launch_spec.plugstack_path);
    let hook_context = spur_core::hooks::HookContext {
        job_id,
        work_dir: launch_spec.work_dir.clone(),
        uid: launch_spec.uid,
        gid: launch_spec.gid,
        partition: launch_spec.partition.clone(),
        nodelist: launch_spec.nodelist.clone(),
        script_context: "epilog_slurmd".into(),
        gpu_devices: launch_spec.gpu_devices.clone(),
        cpus: launch_spec.cpus,
        memory_mb: launch_spec.memory_mb,
    };
    let job = if launch_spec.allocation_only {
        RunningJob::AllocationOnly
    } else {
        match crate::executor::launch_job(&launch_spec.into_launch_config(), spank.as_ref()).await {
            Ok(result) => result.job,
            Err(error) => {
                if let Some(rootfs_mode) = container_rootfs_mode.as_ref() {
                    crate::container::cleanup_rootfs(job_id, rootfs_mode);
                }
                crate::executor::cleanup_job_spool(job_id);
                return Err(anyhow::anyhow!(error.to_string()));
            }
        }
    };
    if let Some(cgroup_path) = job.cgroup_path() {
        descriptor.cgroup_path = cgroup_path.to_path_buf();
        if let Err(error) = store.publish(&descriptor) {
            tracing::warn!(job_id, %error, "failed to republish runtime descriptor with cgroup path");
        }
    }
    let session = Arc::new(RuntimeSession::with_environment_and_pmix(
        job,
        job_id,
        run_attempt,
        runtime_environment,
        pmix_guard.as_ref().map(|guard| guard.host.clone()),
    ));
    let capability = descriptor.capability.clone();
    let result = run_supervisor(listener, descriptor, session.clone()).await;
    let _ = std::fs::remove_file(socket_path);
    if let Some(cgroup) = session.take_cgroup().await {
        crate::executor::cleanup_cgroup(&cgroup).await;
    }
    if let Some(rootfs_mode) = container_rootfs_mode.as_ref() {
        crate::container::cleanup_rootfs(job_id, rootfs_mode);
    }
    crate::executor::cleanup_job_spool(job_id);
    if let Err(error) = result {
        let failure_path = session_dir.join(FAILURE_FILE);
        if let Err(write_error) = std::fs::write(&failure_path, error.to_string()) {
            tracing::warn!(%write_error, path = %failure_path.display(), "failed to record runtime session failure");
        }
        return Err(error.into());
    }
    let snapshot = session.snapshot().await;
    let exit_code = snapshot
        .exit_code
        .unwrap_or_else(|| 128 + snapshot.signal.unwrap_or(0));
    let epilog_failed = if let Some(epilog) = hooks.epilog.as_deref() {
        if let Err(error) = spur_core::hooks::run_hook(epilog, &hook_context).await {
            tracing::error!(job_id, %error, "runtime epilog hook failed");
            true
        } else {
            false
        }
    } else {
        false
    };
    if let Some(spank) = spank.as_ref() {
        let context = spur_spank::SpankContext {
            job_id,
            uid: hook_context.uid,
            gid: hook_context.gid,
            ..Default::default()
        };
        let mut handle = spur_spank::SpankHandle::new(context, HashMap::new());
        for hook in [
            spur_spank::SpankHook::TaskExit,
            spur_spank::SpankHook::JobEpilog,
        ] {
            if let Err(error) = spank.invoke_hook(hook, &mut handle) {
                tracing::warn!(job_id, %error, hook = hook.symbol_name(), "runtime SPANK exit hook failed");
            }
        }
    }
    let signal = snapshot.signal.unwrap_or(0);
    obligations.append(&RuntimeObligation::ExitObserved { exit_code, signal })?;
    let notification = AgentNotification::RuntimeSessionCompleted {
        job_id,
        run_attempt,
        exit_code,
        signal,
        epilog_failed,
        capability,
    };
    if notify_agent_completion(&agent_socket, &notification).await
        == Some(AgentNotificationResponse::Acknowledged)
    {
        store.acknowledge_completion(&PendingRuntimeCompletion {
            job_id,
            run_attempt,
            exit_code,
            signal,
        })?;
    }
    Ok(exit_code)
}

struct RuntimePmixGuard {
    host: Arc<crate::mpi_plugin::MpiPluginHost>,
    job_id: u32,
}

impl Drop for RuntimePmixGuard {
    fn drop(&mut self) {
        if let Err(error) = self.host.release_pmix_server(self.job_id) {
            tracing::warn!(job_id = self.job_id, %error, "PMIx runtime release failed");
        }
    }
}

fn prepare_pmix_launch(
    launch_spec: &mut RuntimeLaunchSpec,
) -> anyhow::Result<Option<RuntimePmixGuard>> {
    let (Some(config), Some(plan)) = (
        launch_spec.pmix_config.clone(),
        launch_spec.pmix_plan.clone(),
    ) else {
        if launch_spec.pmix_config.is_some() || launch_spec.pmix_plan.is_some() {
            anyhow::bail!("runtime PMIx launch requires both configuration and plan");
        }
        return Ok(None);
    };
    let host = Arc::new(crate::mpi_plugin::MpiPluginHost::new(config));
    host.start_pmix_server_and_verify(&plan)
        .map_err(|error| anyhow::anyhow!(error))?;
    if launch_spec.pmix_multi_task {
        let tasks_on_node = u32::try_from(plan.local_procs.len())
            .map_err(|_| anyhow::anyhow!("too many local PMIx ranks"))?;
        let envs = crate::mpi_plugin::pmix_setup_fork_env_for_node_tasks(
            &host,
            &plan,
            plan.local_procs
                .iter()
                .map(|proc| proc.rank)
                .min()
                .unwrap_or(0),
            tasks_on_node,
        )
        .map_err(|error| anyhow::anyhow!(error))?;
        let script_path = PathBuf::from(&launch_spec.work_dir)
            .join(format!(".spur_user_{}.sh", launch_spec.job_id));
        crate::executor::write_job_scratch(
            &script_path,
            &launch_spec.script,
            launch_spec.uid,
            launch_spec.gid,
        )?;
        launch_spec.script = spur_core::task_launch::build_multi_task_pmix_wrapper(
            script_path.to_string_lossy().as_ref(),
            tasks_on_node,
            &envs,
            Some(&launch_spec.environment),
        )
        .map_err(|error| anyhow::anyhow!(error))?;
    } else {
        crate::mpi_plugin::apply_pmix_setup_fork_env(
            &host,
            &plan,
            plan.local_procs.first().map(|proc| proc.rank).unwrap_or(0),
            &mut launch_spec.environment,
        )
        .map_err(|error| anyhow::anyhow!(error))?;
    }
    Ok(Some(RuntimePmixGuard {
        host,
        job_id: plan.job_id,
    }))
}

fn load_runtime_spank(plugstack_path: &str) -> Option<spur_spank::SpankHost> {
    if plugstack_path.is_empty() || !Path::new(plugstack_path).exists() {
        return None;
    }
    let entries = match spur_spank::parse_plugstack(Path::new(plugstack_path)) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(path = plugstack_path, %error, "failed to parse runtime plugstack");
            return None;
        }
    };
    let mut host = spur_spank::SpankHost::new();
    for entry in entries {
        if let Err(error) = host.load_plugin(&entry.path, &entry.args) {
            tracing::warn!(plugin = %entry.path.display(), %error, required = entry.required, "runtime SPANK plugin failed to load");
        }
    }
    (host.plugin_count() > 0).then_some(host)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionLiveness {
    Live,
    Stale,
}

#[derive(Debug)]
pub(crate) struct DiscoveredRuntimeSessions {
    pub live: Vec<RuntimeSessionDescriptor>,
    pub stale: Vec<RuntimeSessionDescriptor>,
    pub rejected: Vec<(PathBuf, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingRuntimeCompletion {
    pub job_id: u32,
    pub run_attempt: u32,
    pub exit_code: i32,
    pub signal: i32,
}

#[derive(Clone)]
pub struct RuntimeSessionStore {
    root: PathBuf,
}

impl RuntimeSessionStore {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            root: state_dir.into().join("runtime"),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn session_dir(&self, job_id: u32, run_attempt: u32) -> PathBuf {
        self.root.join(format!("{job_id}.{run_attempt}"))
    }

    pub fn obligations(&self, job_id: u32, run_attempt: u32) -> RuntimeObligationLog {
        RuntimeObligationLog::new(self.session_dir(job_id, run_attempt).join(OBLIGATION_FILE))
    }

    pub fn prepare_session_dir(&self, job_id: u32, run_attempt: u32) -> io::Result<PathBuf> {
        let state_dir = self.root.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime root has no state directory",
            )
        })?;
        if !state_dir.exists() {
            fs::create_dir_all(state_dir)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(state_dir, fs::Permissions::from_mode(0o700))?;
            }
        }
        create_private_dir(&self.root)?;
        let session_dir = self.session_dir(job_id, run_attempt);
        create_private_dir(&session_dir)?;
        Ok(session_dir)
    }

    pub fn publish(&self, descriptor: &RuntimeSessionDescriptor) -> io::Result<()> {
        let session_dir = self.prepare_session_dir(descriptor.job_id, descriptor.run_attempt)?;
        let temporary_path =
            session_dir.join(format!("{DESCRIPTOR_FILE}.{}.tmp", uuid::Uuid::new_v4()));
        let descriptor_path = session_dir.join(DESCRIPTOR_FILE);
        let contents = serde_json::to_vec(descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("serialize runtime descriptor: {error}"),
            )
        })?;
        let mut temporary = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)?;
        temporary.write_all(&contents)?;
        temporary.sync_all()?;
        drop(temporary);
        fs::rename(&temporary_path, descriptor_path)?;
        fs::File::open(session_dir)?.sync_all()
    }

    pub(crate) fn discover_live(&self) -> io::Result<DiscoveredRuntimeSessions> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(DiscoveredRuntimeSessions {
                    live: Vec::new(),
                    stale: Vec::new(),
                    rejected: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };

        let mut live = Vec::new();
        let mut stale = Vec::new();
        let mut rejected = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    rejected.push((self.root.clone(), error.to_string()));
                    continue;
                }
            };
            let path = entry.path();
            if !entry.file_type()?.is_dir() {
                rejected.push((path, "runtime session entry is not a directory".into()));
                continue;
            }

            match self.load_descriptor(&path) {
                Ok(descriptor) => match session_liveness(&descriptor) {
                    Ok(SessionLiveness::Live) => live.push(descriptor),
                    Ok(SessionLiveness::Stale) => stale.push(descriptor),
                    Err(error) => rejected.push((path, error.to_string())),
                },
                Err(error) => rejected.push((path, error.to_string())),
            }
        }

        Ok(DiscoveredRuntimeSessions {
            live,
            stale,
            rejected,
        })
    }

    pub(crate) fn discover_unacknowledged_completions(
        &self,
    ) -> io::Result<Vec<PendingRuntimeCompletion>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut completions = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let session_dir = entry.path();
            let descriptor = match self.load_descriptor(&session_dir) {
                Ok(descriptor) => descriptor,
                Err(_) => continue,
            };
            if !matches!(session_liveness(&descriptor), Ok(SessionLiveness::Stale)) {
                continue;
            }
            let obligations = self.obligations(descriptor.job_id, descriptor.run_attempt);
            let mut observed_exit = None;
            let mut acknowledged = false;
            for obligation in obligations.read()? {
                match obligation {
                    RuntimeObligation::ExitObserved { exit_code, signal } => {
                        observed_exit = Some((exit_code, signal));
                        acknowledged = false;
                    }
                    RuntimeObligation::CompletionAcknowledged if observed_exit.is_some() => {
                        acknowledged = true;
                    }
                    RuntimeObligation::CompletionAcknowledged
                    | RuntimeObligation::ResourcesReleased => {}
                }
            }
            if let Some((exit_code, signal)) = observed_exit.filter(|_| !acknowledged) {
                completions.push(PendingRuntimeCompletion {
                    job_id: descriptor.job_id,
                    run_attempt: descriptor.run_attempt,
                    exit_code,
                    signal,
                });
            }
        }
        Ok(completions)
    }

    pub(crate) fn prune_finalized(&self) -> io::Result<usize> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };

        let mut pruned = 0;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let session_dir = entry.path();
            let descriptor = match self.load_descriptor(&session_dir) {
                Ok(descriptor) => descriptor,
                Err(_) => continue,
            };
            let obligations = self.obligations(descriptor.job_id, descriptor.run_attempt);
            if prune_finalized_session(&session_dir, &obligations)? {
                pruned += 1;
            }
        }
        Ok(pruned)
    }

    pub(crate) fn acknowledge_completion(
        &self,
        completion: &PendingRuntimeCompletion,
    ) -> io::Result<()> {
        let obligations = self.obligations(completion.job_id, completion.run_attempt);
        obligations.append(&RuntimeObligation::CompletionAcknowledged)?;
        prune_finalized_session(
            &self.session_dir(completion.job_id, completion.run_attempt),
            &obligations,
        )
        .map(|_| ())
    }

    pub(crate) fn observed_exit(
        &self,
        job_id: u32,
        run_attempt: u32,
    ) -> io::Result<Option<(i32, i32)>> {
        let obligations = self.obligations(job_id, run_attempt).read()?;
        Ok(obligations
            .iter()
            .rev()
            .find_map(|obligation| match obligation {
                RuntimeObligation::ExitObserved { exit_code, signal } => {
                    Some((*exit_code, *signal))
                }
                _ => None,
            }))
    }

    pub(crate) fn load_descriptor(
        &self,
        session_dir: &Path,
    ) -> io::Result<RuntimeSessionDescriptor> {
        let descriptor_path = session_dir.join(DESCRIPTOR_FILE);
        let contents = fs::read(&descriptor_path)?;
        let descriptor: RuntimeSessionDescriptor =
            serde_json::from_slice(&contents).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid {}: {e}", descriptor_path.display()),
                )
            })?;
        if descriptor.format_version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported runtime descriptor version {}",
                    descriptor.format_version
                ),
            ));
        }
        if session_dir != self.session_dir(descriptor.job_id, descriptor.run_attempt) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime descriptor identity does not match its directory",
            ));
        }
        Ok(descriptor)
    }
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    match fs::create_dir(path) {
        Ok(()) => fs::set_permissions(path, fs::Permissions::from_mode(0o700)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => verify_private_dir(path),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn verify_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("runtime path {} is not a directory", path.display()),
        ));
    }
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("runtime directory {} is not private", path.display()),
        ));
    }
    Ok(())
}

pub(crate) fn process_start_ticks(pid: u32) -> io::Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, fields) = stat
        .rsplit_once(") ")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid /proc stat format"))?;
    fields
        .split_ascii_whitespace()
        .nth(19)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start time"))?
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub(crate) fn session_liveness(
    descriptor: &RuntimeSessionDescriptor,
) -> io::Result<SessionLiveness> {
    match process_start_ticks(descriptor.pid) {
        Ok(start_ticks) if start_ticks == descriptor.process_start_ticks => {
            Ok(SessionLiveness::Live)
        }
        Ok(_) => Ok(SessionLiveness::Stale),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(SessionLiveness::Stale),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for_pty_exit(session: &RuntimeSession) -> RuntimePtyOutput {
        loop {
            let pty = session
                .pty
                .lock()
                .await
                .clone()
                .expect("PTY must be running");
            let notified = pty.output_ready.notified();
            let output = session.read_pty(0).await.expect("read PTY output");
            if output.eof && output.exit_code.is_some() {
                return output;
            }
            notified.await;
        }
    }

    fn pty_spec(argv: &[&str]) -> RuntimePtyLaunchSpec {
        RuntimePtyLaunchSpec {
            argv: argv.iter().map(|arg| (*arg).to_string()).collect(),
            work_dir: "/tmp".into(),
            environment: std::collections::HashMap::new(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            memlock: RuntimeMemlock::Inherit,
            winsize: Some(RuntimeWindowSize {
                rows: 24,
                cols: 80,
                xpixel: 0,
                ypixel: 0,
            }),
        }
    }

    fn descriptor(job_id: u32, run_attempt: u32, pid: u32) -> RuntimeSessionDescriptor {
        RuntimeSessionDescriptor::new(
            job_id,
            run_attempt,
            pid,
            process_start_ticks(pid).expect("test process must exist"),
            PathBuf::from("/run/spur/runtime.sock"),
            PathBuf::from("/sys/fs/cgroup/spur/test"),
        )
    }

    fn write_descriptor(
        store: &RuntimeSessionStore,
        descriptor: &RuntimeSessionDescriptor,
    ) -> PathBuf {
        let session_dir = store.session_dir(descriptor.job_id, descriptor.run_attempt);
        fs::create_dir_all(&session_dir).expect("create session directory");
        fs::write(
            session_dir.join(DESCRIPTOR_FILE),
            serde_json::to_vec(descriptor).expect("serialize descriptor"),
        )
        .expect("write descriptor");
        session_dir
    }

    fn launch_spec() -> RuntimeLaunchSpec {
        RuntimeLaunchSpec {
            job_id: 42,
            script: "true".into(),
            work_dir: "/tmp".into(),
            name: "runtime-test".into(),
            user: "spur".into(),
            node: "node-a".into(),
            environment: HashMap::new(),
            stdout_path: String::new(),
            stderr_path: String::new(),
            stdin_path: String::new(),
            cpus: 1,
            memory_mb: 0,
            gpu_devices: Vec::new(),
            cpu_ids: Vec::new(),
            open_mode: None,
            uid: nix::unistd::geteuid().as_raw(),
            gid: nix::unistd::getegid().as_raw(),
            partition: "default".into(),
            nodelist: "node-a".into(),
            memlock: RuntimeMemlock::Inherit,
            container: None,
            host_device_plan: None,
            container_rootfs_mode: None,
            hooks: spur_core::config::HooksConfig::default(),
            plugstack_path: String::new(),
            controller_addr: String::new(),
            reporting_node: String::new(),
            run_attempt: 1,
            capability: String::new(),
            allocation_only: false,
            pmix_config: None,
            pmix_plan: None,
            pmix_multi_task: false,
        }
    }

    #[test]
    fn launch_spec_preserves_pmix_multi_task_execution_mode() {
        let mut spec = launch_spec();
        spec.pmix_multi_task = true;
        assert!(spec.into_launch_config().pmix_multi_task);
    }

    #[test]
    fn launch_spec_persists_gpu_injection_plan() {
        let mut spec = launch_spec();
        spec.gpu_devices = vec![3, 7];
        spec.host_device_plan = Some(spur_devices::inject::HostInjectionPlan {
            env: HashMap::from([("ROCR_VISIBLE_DEVICES".into(), "3,7".into())]),
            visible_devices: vec!["/dev/dri/renderD128".into()],
            device_paths: vec!["/dev/dri/renderD128".into()],
        });
        let restored: RuntimeLaunchSpec =
            serde_json::from_slice(&serde_json::to_vec(&spec).expect("encode launch spec"))
                .expect("decode launch spec");
        let config = restored.into_launch_config();
        assert_eq!(config.gpu_devices, vec![3, 7]);
        assert_eq!(
            config
                .host_device_plan
                .as_ref()
                .and_then(|plan| plan.env.get("ROCR_VISIBLE_DEVICES")),
            Some(&"3,7".to_string())
        );
    }

    #[test]
    fn legacy_launch_spec_deserializes_runtime_defaults() {
        let mut serialized = serde_json::to_value(launch_spec()).expect("encode launch spec");
        let fields = serialized
            .as_object_mut()
            .expect("launch spec must encode as an object");
        for field in [
            "gpu_devices",
            "container",
            "host_device_plan",
            "container_rootfs_mode",
            "hooks",
            "plugstack_path",
            "controller_addr",
            "reporting_node",
            "run_attempt",
            "capability",
            "allocation_only",
            "pmix_config",
            "pmix_plan",
            "pmix_multi_task",
        ] {
            fields.remove(field);
        }

        let restored: RuntimeLaunchSpec =
            serde_json::from_value(serialized).expect("decode legacy launch spec");
        assert!(restored.gpu_devices.is_empty());
        assert!(restored.container.is_none());
        assert!(restored.host_device_plan.is_none());
        assert!(restored.container_rootfs_mode.is_none());
        assert_eq!(
            serde_json::to_value(&restored.hooks).expect("encode default hooks"),
            serde_json::to_value(spur_core::config::HooksConfig::default())
                .expect("encode expected hooks")
        );
        assert!(restored.plugstack_path.is_empty());
        assert!(restored.controller_addr.is_empty());
        assert!(restored.reporting_node.is_empty());
        assert_eq!(restored.run_attempt, 0);
        assert!(restored.capability.is_empty());
        assert!(!restored.allocation_only);
        assert!(restored.pmix_config.is_none());
        assert!(restored.pmix_plan.is_none());
        assert!(!restored.pmix_multi_task);
    }

    #[test]
    fn pmix_runtime_rejects_incomplete_persisted_inputs() {
        let mut spec = launch_spec();
        spec.pmix_config = Some(spur_core::config::MpiConfig::default());
        let error = match prepare_pmix_launch(&mut spec) {
            Ok(_) => panic!("incomplete PMIx inputs must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("both configuration and plan"));
    }

    #[test]
    fn legacy_step_request_deserializes_without_runtime_session_extensions() {
        let request = RuntimeRequest::LaunchStep {
            step: Box::new(RuntimeStepLaunchSpec {
                step_id: 7,
                program: "true".into(),
                args: Vec::new(),
                work_dir: "/tmp".into(),
                environment: HashMap::new(),
                uid: 1,
                gid: 1,
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: None,
            }),
        };
        let mut request = serde_json::to_value(request).expect("encode request");
        request
            .get_mut("step")
            .and_then(serde_json::Value::as_object_mut)
            .expect("step object")
            .remove("pmix");
        request
            .get_mut("step")
            .and_then(serde_json::Value::as_object_mut)
            .expect("step object")
            .remove("task_epilog");
        let RuntimeRequest::LaunchStep { step } =
            serde_json::from_value::<RuntimeRequest>(request).expect("decode legacy request")
        else {
            panic!("expected runtime step request");
        };
        assert!(step.pmix.is_none());
        assert!(step.task_epilog.is_none());
    }

    #[test]
    fn legacy_descriptor_deserializes_runtime_defaults() {
        let descriptor = descriptor(42, 3, std::process::id());
        let mut serialized = serde_json::to_value(descriptor).expect("encode descriptor");
        let fields = serialized
            .as_object_mut()
            .expect("descriptor must encode as an object");
        for field in ["capability", "owner", "uid", "gid", "work_dir"] {
            fields.remove(field);
        }

        let restored: RuntimeSessionDescriptor =
            serde_json::from_value(serialized).expect("decode legacy descriptor");
        assert!(restored.capability.is_empty());
        assert!(restored.owner.is_empty());
        assert_eq!(restored.uid, 0);
        assert_eq!(restored.gid, 0);
        assert!(restored.work_dir.is_empty());
    }

    #[test]
    fn reads_own_process_start_time() {
        assert!(process_start_ticks(std::process::id()).expect("read current process") > 0);
    }

    #[tokio::test]
    async fn runtime_pty_replays_output_after_client_disconnect() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 77, 1);
        session
            .launch_pty(pty_spec(&["/bin/sh", "-c", "printf runtime-pty"]))
            .await
            .expect("launch runtime PTY");

        let first = wait_for_pty_exit(&session).await;
        assert_eq!(first.data, b"runtime-pty");
        assert_eq!(first.exit_code, Some(0));

        let replay = session.read_pty(0).await.expect("replay runtime PTY");
        assert_eq!(replay.start_offset, 0);
        assert_eq!(replay.data, b"runtime-pty");
        assert!(replay.eof);
    }

    #[tokio::test]
    async fn runtime_pty_accepts_input_without_agent_owned_bridge() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 78, 1);
        session
            .launch_pty(pty_spec(&[
                "/bin/sh",
                "-c",
                "read value; printf got:$value",
            ]))
            .await
            .expect("launch runtime PTY");
        session
            .write_pty(b"reconnected\n")
            .await
            .expect("write runtime PTY");

        let output = wait_for_pty_exit(&session).await;
        assert!(String::from_utf8_lossy(&output.data).contains("got:reconnected"));
        assert_eq!(output.exit_code, Some(0));
    }

    #[tokio::test]
    async fn runtime_pty_bounds_replay_buffer_and_advances_offset() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 79, 1);
        session
            .launch_pty(pty_spec(&["/bin/sh", "-c", "head -c 1052672 /dev/zero"]))
            .await
            .expect("launch runtime PTY");

        let output = wait_for_pty_exit(&session).await;
        assert!(output.start_offset > 0);
        assert_eq!(output.data.len(), PTY_OUTPUT_LIMIT);
        assert_eq!(output.exit_code, Some(0));
    }

    #[tokio::test]
    async fn runtime_pty_launch_is_idempotent() {
        let session = Arc::new(RuntimeSession::new(RunningJob::AllocationOnly, 80, 1));
        let spec = pty_spec(&["/bin/sh", "-c", "printf runtime-pty-once"]);
        let (first, second) =
            tokio::join!(session.launch_pty(spec.clone()), session.launch_pty(spec),);
        first.expect("first PTY launch");
        second.expect("duplicate PTY launch");

        let output = wait_for_pty_exit(&session).await;
        assert_eq!(output.data, b"runtime-pty-once");
    }

    #[tokio::test]
    async fn allocation_pty_stays_active_until_its_child_exits() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 81, 1);
        session
            .launch_pty(pty_spec(&[
                "/bin/sh",
                "-c",
                "trap '' TERM; while :; do :; done",
            ]))
            .await
            .expect("launch PTY");

        session
            .signal(nix::sys::signal::Signal::SIGTERM as i32)
            .await
            .expect("send SIGTERM");
        assert!(session.snapshot().await.active);

        session
            .signal(nix::sys::signal::Signal::SIGKILL as i32)
            .await
            .expect("send SIGKILL");
        let output = wait_for_pty_exit(&session).await;
        assert!(output.exit_code.is_some());
        assert!(!session.snapshot().await.active);
    }

    #[tokio::test]
    async fn sigkill_also_kills_the_job_cgroup() {
        let cgroup = tempfile::tempdir().expect("tempdir");
        std::fs::write(cgroup.path().join("cgroup.kill"), b"").expect("seed cgroup.kill");
        let mut command = tokio::process::Command::new("/bin/sh");
        command.arg("-c").arg("trap '' TERM; while :; do :; done");
        command.process_group(0);
        let child = command.spawn().expect("spawn managed job");
        let job = RunningJob::Managed {
            child,
            cgroup_path: Some(cgroup.path().to_path_buf()),
        };
        let session = RuntimeSession::new(job, 83, 1);

        session
            .signal(nix::sys::signal::Signal::SIGTERM as i32)
            .await
            .expect("send SIGTERM");
        assert_eq!(
            std::fs::read(cgroup.path().join("cgroup.kill")).expect("read cgroup.kill"),
            b"",
            "SIGTERM must not trigger cgroup.kill"
        );

        session
            .signal(nix::sys::signal::Signal::SIGKILL as i32)
            .await
            .expect("send SIGKILL");
        assert_eq!(
            std::fs::read(cgroup.path().join("cgroup.kill")).expect("read cgroup.kill"),
            b"1",
            "SIGKILL must escalate through cgroup.kill"
        );
    }

    #[tokio::test]
    async fn allocation_only_signals_preserve_session_liveness() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 82, 1);
        for signal in [
            nix::sys::signal::Signal::SIGSTOP,
            nix::sys::signal::Signal::SIGCONT,
            nix::sys::signal::Signal::SIGTERM,
        ] {
            session
                .signal(signal as i32)
                .await
                .expect("signal allocation-only runtime session");
            assert!(
                session.snapshot().await.active,
                "{signal:?} must not end session"
            );
        }
    }

    #[test]
    fn discovers_only_live_identity_matched_sessions() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let pid = std::process::id();
        let live = descriptor(42, 3, pid);
        write_descriptor(&store, &live);

        let stale = descriptor(43, 1, pid);
        let mut stale = stale;
        stale.process_start_ticks += 1;
        write_descriptor(&store, &stale);

        let discovered = store.discover_live().expect("discover sessions");
        assert_eq!(discovered.live, vec![live]);
        assert_eq!(discovered.stale, vec![stale]);
        assert!(discovered.rejected.is_empty());
    }

    #[test]
    fn rejects_descriptor_with_mismatched_directory_identity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let session_dir = store.session_dir(42, 3);
        fs::create_dir_all(&session_dir).expect("create session directory");
        let descriptor = descriptor(99, 1, std::process::id());
        fs::write(
            session_dir.join(DESCRIPTOR_FILE),
            serde_json::to_vec(&descriptor).expect("serialize descriptor"),
        )
        .expect("write descriptor");

        let discovered = store.discover_live().expect("discover sessions");
        assert!(discovered.live.is_empty());
        assert_eq!(discovered.rejected.len(), 1);
        assert!(discovered.rejected[0]
            .1
            .contains("identity does not match its directory"));
    }

    #[test]
    fn publish_writes_a_private_reconnectable_descriptor() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let descriptor = descriptor(42, 3, std::process::id());
        store.publish(&descriptor).expect("publish descriptor");
        let session_dir = store.session_dir(42, 3);
        assert_eq!(
            fs::metadata(&session_dir)
                .expect("session metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let discovered = store.discover_live().expect("discover session");
        assert_eq!(discovered.live, vec![descriptor]);
    }

    #[test]
    fn prepares_a_missing_configured_state_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("missing-state-directory");
        let store = RuntimeSessionStore::new(&state_dir);

        let session_dir = store
            .prepare_session_dir(42, 3)
            .expect("prepare nested runtime session directory");

        assert_eq!(session_dir, state_dir.join("runtime/42.3"));
        assert!(session_dir.is_dir());
    }

    #[test]
    fn obligations_preserve_exit_and_acknowledgement_order() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        store
            .prepare_session_dir(42, 3)
            .expect("prepare session directory");
        let obligations = store.obligations(42, 3);
        obligations
            .append(&RuntimeObligation::ExitObserved {
                exit_code: 0,
                signal: 0,
            })
            .expect("record exit");
        obligations
            .append(&RuntimeObligation::CompletionAcknowledged)
            .expect("record acknowledgement");
        assert_eq!(
            obligations.read().expect("read obligations"),
            vec![
                RuntimeObligation::ExitObserved {
                    exit_code: 0,
                    signal: 0,
                },
                RuntimeObligation::CompletionAcknowledged,
            ]
        );
    }

    #[test]
    fn resource_release_obligation_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.socket_path = store.session_dir(42, 3).join("runtime.sock");
        store.publish(&descriptor).expect("publish descriptor");

        record_resources_released(&descriptor).expect("record first release");
        record_resources_released(&descriptor).expect("record duplicate release");

        assert_eq!(
            store.obligations(42, 3).read().expect("read obligations"),
            vec![RuntimeObligation::ResourcesReleased]
        );
    }

    #[test]
    fn stale_exit_without_acknowledgement_is_recoverable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.process_start_ticks += 1;
        descriptor.socket_path = store.session_dir(42, 3).join("runtime.sock");
        write_descriptor(&store, &descriptor);
        let obligations = store.obligations(42, 3);
        obligations
            .append(&RuntimeObligation::ExitObserved {
                exit_code: 7,
                signal: 0,
            })
            .expect("record exit");
        let completions = store
            .discover_unacknowledged_completions()
            .expect("discover completion");
        assert_eq!(
            completions,
            vec![PendingRuntimeCompletion {
                job_id: 42,
                run_attempt: 3,
                exit_code: 7,
                signal: 0,
            }]
        );
        store
            .acknowledge_completion(&completions[0])
            .expect("acknowledge completion");
        assert!(store
            .discover_unacknowledged_completions()
            .expect("rediscover completion")
            .is_empty());
    }

    #[test]
    fn observed_exit_remains_available_after_completion_acknowledgement() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.socket_path = store.session_dir(42, 3).join("runtime.sock");
        store.publish(&descriptor).expect("publish descriptor");
        store
            .obligations(42, 3)
            .append(&RuntimeObligation::ExitObserved {
                exit_code: 7,
                signal: 9,
            })
            .expect("record exit");
        store
            .acknowledge_completion(&PendingRuntimeCompletion {
                job_id: 42,
                run_attempt: 3,
                exit_code: 7,
                signal: 9,
            })
            .expect("acknowledge completion");

        assert_eq!(
            store.observed_exit(42, 3).expect("read observed exit"),
            Some((7, 9))
        );
    }

    #[test]
    fn finalized_attempt_is_pruned_only_after_acknowledgement_and_resource_release() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.process_start_ticks += 1;
        descriptor.socket_path = store.session_dir(42, 3).join("runtime.sock");
        let session_dir = write_descriptor(&store, &descriptor);
        let obligations = store.obligations(42, 3);
        obligations
            .append(&RuntimeObligation::ExitObserved {
                exit_code: 0,
                signal: 0,
            })
            .expect("record exit");
        let completion = PendingRuntimeCompletion {
            job_id: 42,
            run_attempt: 3,
            exit_code: 0,
            signal: 0,
        };

        store
            .acknowledge_completion(&completion)
            .expect("acknowledge completion");
        assert!(session_dir.is_dir());

        record_resources_released(&descriptor).expect("record resource release");
        assert!(!session_dir.exists());
    }

    #[test]
    fn startup_pruning_keeps_unacknowledged_and_unreleased_attempts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());

        let mut unacknowledged = descriptor(42, 3, std::process::id());
        unacknowledged.process_start_ticks += 1;
        unacknowledged.socket_path = store.session_dir(42, 3).join("runtime.sock");
        let unacknowledged_dir = write_descriptor(&store, &unacknowledged);
        let unacknowledged_obligations = store.obligations(42, 3);
        unacknowledged_obligations
            .append(&RuntimeObligation::ExitObserved {
                exit_code: 0,
                signal: 0,
            })
            .expect("record exit");
        record_resources_released(&unacknowledged).expect("record resource release");

        let mut unreleased = descriptor(43, 3, std::process::id());
        unreleased.process_start_ticks += 1;
        unreleased.socket_path = store.session_dir(43, 3).join("runtime.sock");
        let unreleased_dir = write_descriptor(&store, &unreleased);
        let unreleased_obligations = store.obligations(43, 3);
        unreleased_obligations
            .append(&RuntimeObligation::ExitObserved {
                exit_code: 0,
                signal: 0,
            })
            .expect("record exit");
        store
            .acknowledge_completion(&PendingRuntimeCompletion {
                job_id: 43,
                run_attempt: 3,
                exit_code: 0,
                signal: 0,
            })
            .expect("acknowledge completion");

        let mut finalized = descriptor(44, 3, std::process::id());
        finalized.process_start_ticks += 1;
        let finalized_dir = write_descriptor(&store, &finalized);
        let finalized_obligations = store.obligations(44, 3);
        for obligation in [
            RuntimeObligation::ExitObserved {
                exit_code: 0,
                signal: 0,
            },
            RuntimeObligation::CompletionAcknowledged,
            RuntimeObligation::ResourcesReleased,
        ] {
            finalized_obligations
                .append(&obligation)
                .expect("record finalized obligation");
        }

        assert_eq!(store.prune_finalized().expect("prune attempts"), 1);
        assert!(unacknowledged_dir.is_dir());
        assert!(unreleased_dir.is_dir());
        assert!(!finalized_dir.exists());
    }

    #[test]
    fn hello_requires_compatible_version_capability_and_attempt() {
        let descriptor = descriptor(42, 3, std::process::id());
        assert_eq!(
            validate_hello(
                &descriptor,
                &descriptor.capability,
                &descriptor.capability,
                PROTOCOL_VERSION,
                3
            ),
            RuntimeResponse::Hello {
                protocol_version: PROTOCOL_VERSION,
                job_id: 42,
                run_attempt: 3,
            }
        );
        assert!(matches!(
            validate_hello(
                &descriptor,
                "wrong",
                &descriptor.capability,
                PROTOCOL_VERSION,
                3
            ),
            RuntimeResponse::Rejected { .. }
        ));
        assert_eq!(
            validate_hello(
                &descriptor,
                &descriptor.capability,
                &descriptor.capability,
                MIN_PROTOCOL_VERSION,
                3
            ),
            RuntimeResponse::Hello {
                protocol_version: MIN_PROTOCOL_VERSION,
                job_id: 42,
                run_attempt: 3,
            }
        );
        assert!(matches!(
            validate_hello(
                &descriptor,
                &descriptor.capability,
                &descriptor.capability,
                MIN_PROTOCOL_VERSION - 1,
                3
            ),
            RuntimeResponse::Rejected { .. }
        ));
        assert!(matches!(
            validate_hello(
                &descriptor,
                &descriptor.capability,
                &descriptor.capability,
                PROTOCOL_VERSION,
                4
            ),
            RuntimeResponse::Rejected { .. }
        ));
    }

    #[tokio::test]
    async fn unix_socket_hello_authenticates_before_returning_connection() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("runtime.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind socket");
        let descriptor = descriptor(42, 3, std::process::id());
        let capability = descriptor.capability.clone();
        let server = tokio::spawn(async move {
            accept_hello(&listener, &descriptor, &descriptor.capability)
                .await
                .map(|(_, instance_id)| instance_id)
        });

        let stream = UnixStream::connect(&socket_path)
            .await
            .expect("connect socket");
        let (reader, mut writer) = stream.into_split();
        let request = RuntimeRequest::Hello {
            protocol_version: PROTOCOL_VERSION,
            capability,
            spurd_instance_id: "agent-1".into(),
            run_attempt: 3,
        };
        writer
            .write_all(
                format!(
                    "{}\n",
                    serde_json::to_string(&request).expect("encode request")
                )
                .as_bytes(),
            )
            .await
            .expect("write hello");
        let mut reader = BufReader::new(reader);
        let mut response = String::new();
        reader.read_line(&mut response).await.expect("read hello");
        assert!(matches!(
            serde_json::from_str::<RuntimeResponse>(&response).expect("decode response"),
            RuntimeResponse::Hello { job_id: 42, .. }
        ));
        assert_eq!(
            server.await.expect("server task").expect("accepted"),
            "agent-1"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_hello_does_not_block_a_later_control_connection() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("runtime.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind socket");
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.socket_path = socket_path;
        let session = Arc::new(RuntimeSession::new(RunningJob::AllocationOnly, 42, 3));
        let supervisor = tokio::spawn(run_supervisor(listener, descriptor.clone(), session));

        let _partial = UnixStream::connect(&descriptor.socket_path)
            .await
            .expect("connect incomplete client");
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        let query_descriptor = descriptor.clone();
        let query =
            tokio::spawn(async move { query_state(&query_descriptor, "agent-1".into()).await });
        for _ in 0..64 {
            if query.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            query.is_finished(),
            "a partial hello must not prevent later control connections"
        );
        assert!(
            query
                .await
                .expect("query task")
                .expect("query state")
                .active
        );

        supervisor.abort();
        let _ = supervisor.await;
    }

    #[tokio::test]
    async fn query_state_falls_back_to_a_v1_runtime() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("runtime.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind socket");
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.socket_path = socket_path;
        let capability = descriptor.capability.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept v2 client");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read v2 hello");
            assert!(matches!(
                serde_json::from_str::<RuntimeRequest>(&line).expect("decode v2 hello"),
                RuntimeRequest::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    ..
                }
            ));
            write_response(
                &mut writer,
                &RuntimeResponse::Rejected {
                    message: format!(
                        "runtime protocol {PROTOCOL_VERSION} is incompatible with {MIN_PROTOCOL_VERSION}..={MIN_PROTOCOL_VERSION}"
                    ),
                },
            )
            .await
            .expect("reject v2 hello");

            let (stream, _) = listener.accept().await.expect("accept v1 client");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            line.clear();
            reader.read_line(&mut line).await.expect("read v1 hello");
            let RuntimeRequest::Hello {
                protocol_version,
                capability: received_capability,
                run_attempt,
                ..
            } = serde_json::from_str(&line).expect("decode v1 hello")
            else {
                panic!("expected v1 hello");
            };
            assert_eq!(protocol_version, MIN_PROTOCOL_VERSION);
            assert_eq!(received_capability, capability);
            assert_eq!(run_attempt, 3);
            write_response(
                &mut writer,
                &RuntimeResponse::Hello {
                    protocol_version: MIN_PROTOCOL_VERSION,
                    job_id: 42,
                    run_attempt: 3,
                },
            )
            .await
            .expect("accept v1 hello");
            line.clear();
            reader.read_line(&mut line).await.expect("read state query");
            assert!(matches!(
                serde_json::from_str::<RuntimeRequest>(&line).expect("decode state query"),
                RuntimeRequest::QueryState
            ));
            write_response(
                &mut writer,
                &RuntimeResponse::State {
                    job_id: 42,
                    run_attempt: 3,
                    active: true,
                    exit_code: None,
                    signal: None,
                },
            )
            .await
            .expect("write state response");
        });

        let state = query_state(&descriptor, "agent-1".into())
            .await
            .expect("query v1 runtime");
        assert!(state.active);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn resumable_pty_is_not_sent_to_a_v1_runtime() {
        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("runtime.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind socket");
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.socket_path = socket_path;
        let capability = descriptor.capability.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept v2 client");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read v2 hello");
            write_response(
                &mut writer,
                &RuntimeResponse::Rejected {
                    message: format!(
                        "runtime protocol {PROTOCOL_VERSION} is incompatible with {MIN_PROTOCOL_VERSION}..={MIN_PROTOCOL_VERSION}"
                    ),
                },
            )
            .await
            .expect("reject v2 hello");

            let (stream, _) = listener.accept().await.expect("accept v1 client");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            line.clear();
            reader.read_line(&mut line).await.expect("read v1 hello");
            let RuntimeRequest::Hello {
                protocol_version,
                capability: received_capability,
                run_attempt,
                ..
            } = serde_json::from_str(&line).expect("decode v1 hello")
            else {
                panic!("expected v1 hello");
            };
            assert_eq!(protocol_version, MIN_PROTOCOL_VERSION);
            assert_eq!(received_capability, capability);
            assert_eq!(run_attempt, 3);
            write_response(
                &mut writer,
                &RuntimeResponse::Hello {
                    protocol_version: MIN_PROTOCOL_VERSION,
                    job_id: 42,
                    run_attempt: 3,
                },
            )
            .await
            .expect("accept v1 hello");
            line.clear();
            assert_eq!(
                reader.read_line(&mut line).await.expect("read post-hello"),
                0
            );
        });

        let error = launch_pty(&descriptor, "agent-1".into(), pty_spec(&["/bin/true"]))
            .await
            .expect_err("v1 runtime must reject resumable PTY");
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("v1 does not support"));
        server.await.expect("server task");
    }

    #[test]
    fn only_version_rejections_are_retryable() {
        assert!(protocol_version_rejected(&io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime protocol 2 is incompatible with 1..=1",
        )));
        assert!(!protocol_version_rejected(&io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime capability rejected",
        )));
        assert!(!protocol_version_rejected(&io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime attempt is stale",
        )));
    }

    async fn write_response(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        response: &RuntimeResponse,
    ) -> io::Result<()> {
        writer
            .write_all(
                format!(
                    "{}\n",
                    serde_json::to_string(response).map_err(io::Error::other)?
                )
                .as_bytes(),
            )
            .await
    }

    #[tokio::test]
    async fn control_loop_reports_live_state_and_records_teardown() {
        let (server_stream, client_stream) = UnixStream::pair().expect("socket pair");
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        let server = tokio::spawn(async move { serve_control(server_stream, &session).await });
        let (reader, mut writer) = client_stream.into_split();
        for request in [RuntimeRequest::QueryState, RuntimeRequest::BeginTeardown] {
            writer
                .write_all(
                    format!(
                        "{}\n",
                        serde_json::to_string(&request).expect("encode request")
                    )
                    .as_bytes(),
                )
                .await
                .expect("write request");
        }
        drop(writer);
        let mut reader = BufReader::new(reader);
        let mut state = String::new();
        reader.read_line(&mut state).await.expect("read state");
        assert!(matches!(
            serde_json::from_str::<RuntimeResponse>(&state).expect("decode state"),
            RuntimeResponse::State { active: true, .. }
        ));
        let mut acknowledged = String::new();
        reader
            .read_line(&mut acknowledged)
            .await
            .expect("read teardown acknowledgement");
        assert_eq!(
            serde_json::from_str::<RuntimeResponse>(&acknowledged).expect("decode acknowledgement"),
            RuntimeResponse::Acknowledged
        );
        server.await.expect("server task").expect("serve control");
    }

    #[tokio::test]
    async fn allocation_signals_do_not_end_the_runtime_session() {
        let (server_stream, client_stream) = UnixStream::pair().expect("socket pair");
        let session = Arc::new(RuntimeSession::new(RunningJob::AllocationOnly, 42, 3));
        let server_session = session.clone();
        let server =
            tokio::spawn(async move { serve_control(server_stream, &server_session).await });
        let (reader, mut writer) = client_stream.into_split();
        writer
            .write_all(
                format!(
                    "{}\n",
                    serde_json::to_string(&RuntimeRequest::SignalAllocation {
                        signal: nix::sys::signal::Signal::SIGSTOP as i32,
                    })
                    .expect("encode signal request")
                )
                .as_bytes(),
            )
            .await
            .expect("write signal request");
        drop(writer);
        let mut reader = BufReader::new(reader);
        let mut response = String::new();
        reader
            .read_line(&mut response)
            .await
            .expect("read response");
        assert_eq!(
            serde_json::from_str::<RuntimeResponse>(&response).expect("decode response"),
            RuntimeResponse::Acknowledged
        );
        server.await.expect("server task").expect("serve control");
        assert!(session.snapshot().await.active);
    }

    #[tokio::test]
    async fn shutdown_ends_an_allocation_runtime_session() {
        let (server_stream, client_stream) = UnixStream::pair().expect("socket pair");
        let session = Arc::new(RuntimeSession::new(RunningJob::AllocationOnly, 42, 3));
        let server_session = session.clone();
        let server =
            tokio::spawn(async move { serve_control(server_stream, &server_session).await });
        let (reader, mut writer) = client_stream.into_split();
        writer
            .write_all(
                format!(
                    "{}\n",
                    serde_json::to_string(&RuntimeRequest::Shutdown).expect("encode shutdown")
                )
                .as_bytes(),
            )
            .await
            .expect("write shutdown request");
        drop(writer);
        let mut reader = BufReader::new(reader);
        let mut response = String::new();
        reader
            .read_line(&mut response)
            .await
            .expect("read response");
        assert_eq!(
            serde_json::from_str::<RuntimeResponse>(&response).expect("decode response"),
            RuntimeResponse::Acknowledged
        );
        server.await.expect("server task").expect("serve control");
        session
            .poll_completion()
            .await
            .expect("poll teardown completion");
        assert!(!session.snapshot().await.active);
    }

    #[tokio::test]
    async fn shutdown_waits_for_a_runtime_pty_to_exit() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 83, 1);
        session
            .launch_pty(pty_spec(&[
                "/bin/sh",
                "-c",
                "trap '' TERM; while :; do :; done",
            ]))
            .await
            .expect("launch runtime PTY");

        session.begin_teardown().await;
        session.poll_completion().await.expect("poll teardown");
        assert!(session.snapshot().await.active);

        session
            .signal(nix::sys::signal::Signal::SIGKILL as i32)
            .await
            .expect("kill runtime PTY");
        let _ = wait_for_pty_exit(&session).await;
        session
            .poll_completion()
            .await
            .expect("poll PTY completion");
        assert!(!session.snapshot().await.active);
    }

    #[tokio::test]
    async fn control_loop_runs_a_logical_step_and_returns_its_output() {
        let (server_stream, client_stream) = UnixStream::pair().expect("socket pair");
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        let server = tokio::spawn(async move { serve_control(server_stream, &session).await });
        let (reader, mut writer) = client_stream.into_split();
        let request = RuntimeRequest::LaunchStep {
            step: Box::new(RuntimeStepLaunchSpec {
                step_id: 7,
                program: "sh".into(),
                args: vec!["-c".into(), "printf runtime-step".into()],
                work_dir: "/tmp".into(),
                environment: std::collections::HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: None,
            }),
        };
        writer
            .write_all(
                format!(
                    "{}\n",
                    serde_json::to_string(&request).expect("encode launch step")
                )
                .as_bytes(),
            )
            .await
            .expect("write launch step");
        drop(writer);
        let mut reader = BufReader::new(reader);
        let mut response = String::new();
        reader
            .read_line(&mut response)
            .await
            .expect("read response");
        assert_eq!(
            serde_json::from_str::<RuntimeResponse>(&response).expect("decode response"),
            RuntimeResponse::StepCompleted {
                step_id: 7,
                exit_code: 0,
                stdout: "runtime-step".into(),
                stderr: String::new(),
            }
        );
        server.await.expect("server task").expect("serve control");
    }

    #[tokio::test]
    async fn cancelled_step_cannot_start_after_agent_reconnect() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        session
            .signal_step(7, nix::sys::signal::Signal::SIGTERM as i32)
            .await
            .expect("record cancellation");
        let error = session
            .launch_step(RuntimeStepLaunchSpec {
                step_id: 7,
                program: "sh".into(),
                args: vec!["-c".into(), "exit 1".into()],
                work_dir: "/tmp".into(),
                environment: HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: None,
            })
            .await
            .expect_err("cancelled step must not spawn");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    }

    #[tokio::test]
    async fn teardown_rejects_new_logical_steps() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        session.begin_teardown().await;

        let error = session
            .launch_step(RuntimeStepLaunchSpec {
                step_id: 7,
                program: "sh".into(),
                args: vec!["-c".into(), "exit 1".into()],
                work_dir: "/tmp".into(),
                environment: HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: None,
            })
            .await
            .expect_err("tearing down session must not launch a step");

        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    }

    #[tokio::test]
    async fn teardown_rejects_new_ptys() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        session.begin_teardown().await;

        let error = session
            .launch_pty(RuntimePtyLaunchSpec {
                argv: vec!["sh".into()],
                work_dir: "/tmp".into(),
                environment: HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                winsize: None,
            })
            .await
            .expect_err("tearing down session must not launch a PTY");

        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    }

    #[tokio::test]
    async fn completed_step_replays_its_original_result() {
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        let first = session
            .launch_step(RuntimeStepLaunchSpec {
                step_id: 7,
                program: "sh".into(),
                args: vec!["-c".into(), "printf original".into()],
                work_dir: "/tmp".into(),
                environment: HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: None,
            })
            .await
            .expect("launch original step");
        let replay = session
            .launch_step(RuntimeStepLaunchSpec {
                step_id: 7,
                program: "sh".into(),
                args: vec!["-c".into(), "printf replacement".into()],
                work_dir: "/tmp".into(),
                environment: HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: None,
            })
            .await
            .expect("replay completed step");
        assert_eq!(first.stdout, "original");
        assert_eq!(replay.stdout, "original");
    }

    #[tokio::test]
    async fn runtime_step_runs_its_task_epilog_before_returning() {
        let temp = tempfile::tempdir().expect("tempdir");
        let marker = temp.path().join("task-epilog-ran");
        let script = temp.path().join("task-epilog.sh");
        fs::write(
            &script,
            format!("#!/bin/sh\nprintf task > {}\n", marker.display()),
        )
        .expect("write epilog");
        let mut permissions = fs::metadata(&script)
            .expect("script metadata")
            .permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(0o755);
        }
        fs::set_permissions(&script, permissions).expect("make epilog executable");
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        session
            .launch_step(RuntimeStepLaunchSpec {
                step_id: 8,
                program: "true".into(),
                args: Vec::new(),
                work_dir: temp.path().to_string_lossy().into_owned(),
                environment: HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
                pmix: None,
                task_epilog: Some(RuntimeTaskEpilogSpec {
                    script: script.to_string_lossy().into_owned(),
                    job_id: 42,
                    work_dir: temp.path().to_string_lossy().into_owned(),
                    uid: nix::unistd::geteuid().as_raw(),
                    gid: nix::unistd::getegid().as_raw(),
                    partition: "default".into(),
                    nodelist: "node-a".into(),
                    gpu_devices: Vec::new(),
                    cpus: 1,
                    memory_mb: 0,
                }),
            })
            .await
            .expect("run step");
        assert_eq!(fs::read_to_string(marker).expect("read marker"), "task");
    }
}
