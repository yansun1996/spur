// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
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
    pub open_mode: Option<String>,
    pub uid: u32,
    pub gid: u32,
    pub partition: String,
    pub nodelist: String,
    pub memlock: RuntimeMemlock,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeMemlock {
    Unlimited,
    Inherit,
    Bytes(u64),
}

impl TryFrom<&crate::executor::JobLaunchConfig> for RuntimeLaunchSpec {
    type Error = String;

    fn try_from(config: &crate::executor::JobLaunchConfig) -> Result<Self, Self::Error> {
        if config.container.is_some() {
            return Err("container launches are not yet supported by RuntimeSession".into());
        }
        if !config.gpu_devices.is_empty() {
            return Err("GPU launches are not yet supported by RuntimeSession".into());
        }
        if config.pmix_multi_task {
            return Err("PMIx launches are not yet supported by RuntimeSession".into());
        }
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
            cpu_ids: config.cpu_ids.clone(),
            open_mode: config.open_mode.clone(),
            uid: config.uid,
            gid: config.gid,
            partition: config.partition.clone(),
            nodelist: config.nodelist.clone(),
            memlock: match config.memlock {
                spur_core::config::MemlockLimit::Unlimited => RuntimeMemlock::Unlimited,
                spur_core::config::MemlockLimit::Inherit => RuntimeMemlock::Inherit,
                spur_core::config::MemlockLimit::Bytes(value) => RuntimeMemlock::Bytes(value),
            },
            controller_addr: String::new(),
            reporting_node: String::new(),
            run_attempt: 0,
            capability: String::new(),
            allocation_only: false,
        })
    }
}

impl RuntimeLaunchSpec {
    pub fn into_launch_config(self) -> crate::executor::JobLaunchConfig {
        crate::executor::JobLaunchConfig {
            job_id: self.job_id,
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
            gpu_devices: Vec::new(),
            cpu_ids: self.cpu_ids,
            open_mode: self.open_mode,
            uid: self.uid,
            gid: self.gid,
            container: None,
            prolog_script: None,
            partition: self.partition,
            nodelist: self.nodelist,
            host_device_plan: None,
            memlock: match self.memlock {
                RuntimeMemlock::Unlimited => spur_core::config::MemlockLimit::Unlimited,
                RuntimeMemlock::Inherit => spur_core::config::MemlockLimit::Inherit,
                RuntimeMemlock::Bytes(value) => spur_core::config::MemlockLimit::Bytes(value),
            },
            io_mode: crate::executor::LaunchIo::File,
            pmix_multi_task: false,
        }
    }
}

const DESCRIPTOR_FILE: &str = "descriptor.json";
const OBLIGATION_FILE: &str = "obligations.jsonl";
const FORMAT_VERSION: u32 = 1;
pub const PROTOCOL_VERSION: u32 = 1;
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
        step: RuntimeStepLaunchSpec,
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

const PTY_OUTPUT_LIMIT: usize = 1024 * 1024;

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
    snapshot: Mutex<RuntimeSnapshot>,
    steps: Arc<Mutex<RuntimeSteps>>,
    pty: Mutex<Option<Arc<RuntimePty>>>,
    environment: std::collections::HashMap<String, String>,
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
        Self {
            job: Mutex::new(job),
            snapshot: Mutex::new(RuntimeSnapshot {
                job_id,
                run_attempt,
                active: true,
                exit_code: None,
                signal: None,
            }),
            steps: Arc::new(Mutex::new(RuntimeSteps::default())),
            pty: Mutex::new(None),
            environment,
        }
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        self.snapshot.lock().await.clone()
    }

    pub async fn poll_completion(&self) -> io::Result<()> {
        let completed = self.job.lock().await.try_wait().map_err(io::Error::other)?;
        let Some((exit_code, signal)) = completed else {
            return Ok(());
        };
        let mut snapshot = self.snapshot.lock().await;
        snapshot.active = false;
        snapshot.exit_code = Some(exit_code);
        snapshot.signal = Some(signal);
        Ok(())
    }

    pub async fn signal(&self, signal: i32) -> io::Result<()> {
        let signal = nix::sys::signal::Signal::try_from(signal).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid signal: {error}"),
            )
        })?;
        let allocation_only = {
            let job = self.job.lock().await;
            job.kill_signal(signal).map_err(io::Error::other)?;
            job.is_allocation_only()
        };
        let steps = self.steps.lock().await;
        for step in steps.active.values() {
            signal_process_group(step.pid, signal);
        }
        drop(steps);
        if self.pty.lock().await.is_some() {
            self.signal_pty(signal as i32).await?;
        }
        if allocation_only {
            self.snapshot.lock().await.active = false;
        }
        Ok(())
    }

    pub async fn begin_teardown(&self) {
        self.snapshot.lock().await.active = false;
        let steps = self.steps.lock().await;
        for step in steps.active.values() {
            signal_process_group(step.pid, nix::sys::signal::Signal::SIGTERM);
        }
    }

    async fn launch_step(&self, step: RuntimeStepLaunchSpec) -> io::Result<RuntimeStepResult> {
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
                crate::executor::apply_memlock(match memlock {
                    RuntimeMemlock::Unlimited => spur_core::config::MemlockLimit::Unlimited,
                    RuntimeMemlock::Inherit => spur_core::config::MemlockLimit::Inherit,
                    RuntimeMemlock::Bytes(value) => spur_core::config::MemlockLimit::Bytes(value),
                });
                if let Some(ref priv_drop) = priv_drop {
                    priv_drop
                        .apply()
                        .map_err(|error| io::Error::from_raw_os_error(error as i32))?;
                }
                Ok(())
            });
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
        let steps = self.steps.clone();
        tokio::spawn(async move {
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
        if self.pty.lock().await.is_some() {
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
                crate::executor::apply_memlock(match memlock {
                    RuntimeMemlock::Unlimited => spur_core::config::MemlockLimit::Unlimited,
                    RuntimeMemlock::Inherit => spur_core::config::MemlockLimit::Inherit,
                    RuntimeMemlock::Bytes(value) => spur_core::config::MemlockLimit::Bytes(value),
                });
                if let Some(ref priv_drop) = priv_drop {
                    priv_drop
                        .apply()
                        .map_err(|error| io::Error::from_raw_os_error(error as i32))?;
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        let child_pid = child
            .id()
            .ok_or_else(|| io::Error::other("runtime PTY has no pid"))?
            as i32;
        drop(slave);
        nix::fcntl::fcntl(
            &master,
            nix::fcntl::FcntlArg::F_SETFL(nix::fcntl::OFlag::O_NONBLOCK),
        )
        .map_err(io::Error::other)?;
        let pty = Arc::new(RuntimePty {
            master: Arc::new(AsyncFd::new(master)?),
            child_pid,
            buffer: Mutex::new(RuntimePtyBuffer {
                start_offset: 0,
                data: VecDeque::new(),
                eof: false,
                exit_code: None,
            }),
            output_ready: Notify::new(),
        });
        *self.pty.lock().await = Some(pty.clone());
        tokio::spawn(read_runtime_pty(pty.clone()));
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

pub(crate) fn append_obligation(
    descriptor: &RuntimeSessionDescriptor,
    obligation: &RuntimeObligation,
) -> io::Result<()> {
    let session_dir = descriptor.socket_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "runtime socket path has no session directory",
        )
    })?;
    RuntimeObligationLog::new(session_dir.join(OBLIGATION_FILE)).append(obligation)
}

pub fn validate_hello(
    descriptor: &RuntimeSessionDescriptor,
    capability: &str,
    expected_capability: &str,
    protocol_version: u32,
    run_attempt: u32,
) -> RuntimeResponse {
    if protocol_version != PROTOCOL_VERSION {
        return RuntimeResponse::Rejected {
            message: format!(
                "runtime protocol {protocol_version} is incompatible with {PROTOCOL_VERSION}"
            ),
        };
    }
    if capability.len() != expected_capability.len()
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
        protocol_version: PROTOCOL_VERSION,
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
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
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
    let stream = UnixStream::connect(&descriptor.socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let hello = RuntimeRequest::Hello {
        protocol_version: PROTOCOL_VERSION,
        capability: descriptor.capability.clone(),
        spurd_instance_id,
        run_attempt: descriptor.run_attempt,
    };
    write_request(&mut writer, &hello).await?;
    match read_response(&mut reader).await? {
        RuntimeResponse::Hello {
            job_id,
            run_attempt,
            ..
        } if job_id == descriptor.job_id && run_attempt == descriptor.run_attempt => {}
        RuntimeResponse::Rejected { message } => {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, message));
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime hello identity mismatch",
            ));
        }
    }
    write_request(&mut writer, &RuntimeRequest::QueryState).await?;
    match read_response(&mut reader).await? {
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
    let stream = UnixStream::connect(&descriptor.socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let hello = RuntimeRequest::Hello {
        protocol_version: PROTOCOL_VERSION,
        capability: descriptor.capability.clone(),
        spurd_instance_id,
        run_attempt: descriptor.run_attempt,
    };
    write_request(&mut writer, &hello).await?;
    match read_response(&mut reader).await? {
        RuntimeResponse::Hello {
            job_id,
            run_attempt,
            ..
        } if job_id == descriptor.job_id && run_attempt == descriptor.run_attempt => {}
        RuntimeResponse::Rejected { message } => {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, message));
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime hello identity mismatch",
            ));
        }
    }
    write_request(&mut writer, &RuntimeRequest::SignalAllocation { signal }).await?;
    match read_response(&mut reader).await? {
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

pub(crate) async fn launch_step(
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
    step: RuntimeStepLaunchSpec,
) -> io::Result<RuntimeStepResult> {
    let stream = UnixStream::connect(&descriptor.socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    runtime_hello(&mut reader, &mut writer, descriptor, spurd_instance_id).await?;
    let step_id = step.step_id;
    write_request(&mut writer, &RuntimeRequest::LaunchStep { step }).await?;
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
    let stream = UnixStream::connect(&descriptor.socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    runtime_hello(&mut reader, &mut writer, descriptor, spurd_instance_id).await?;
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
    let stream = UnixStream::connect(&descriptor.socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    runtime_hello(&mut reader, &mut writer, descriptor, spurd_instance_id).await?;
    write_request(&mut writer, &request).await?;
    read_response(&mut reader).await
}

async fn runtime_hello(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    descriptor: &RuntimeSessionDescriptor,
    spurd_instance_id: String,
) -> io::Result<()> {
    let hello = RuntimeRequest::Hello {
        protocol_version: PROTOCOL_VERSION,
        capability: descriptor.capability.clone(),
        spurd_instance_id,
        run_attempt: descriptor.run_attempt,
    };
    write_request(writer, &hello).await?;
    match read_response(reader).await? {
        RuntimeResponse::Hello {
            job_id,
            run_attempt,
            ..
        } if job_id == descriptor.job_id && run_attempt == descriptor.run_attempt => Ok(()),
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
    reader.read_line(&mut line).await?;
    serde_json::from_str(&line).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid runtime response: {error}"),
        )
    })
}

pub async fn serve_control(stream: UnixStream, session: &RuntimeSession) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
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
            RuntimeRequest::LaunchStep { step } => match session.launch_step(step.clone()).await {
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
            accepted = accept_hello(&listener, &descriptor, &descriptor.capability) => {
                let (stream, _) = accepted?;
                let session = session.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_control(stream, &session).await {
                        tracing::warn!(%error, "runtime session control connection failed");
                    }
                });
            }
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
    let launch_spec: RuntimeLaunchSpec = serde_json::from_slice(&std::fs::read(&args[3])?)?;
    if launch_spec.job_id != job_id {
        anyhow::bail!("runtime launch spec job id does not match process arguments");
    }
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
    let listener = UnixListener::bind(&socket_path)?;
    let controller_addr = launch_spec.controller_addr.clone();
    let reporting_node = launch_spec.reporting_node.clone();
    let runtime_environment = launch_spec.environment.clone();
    let job = if launch_spec.allocation_only {
        RunningJob::AllocationOnly
    } else {
        crate::executor::launch_job(&launch_spec.into_launch_config(), None)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?
            .job
    };
    let session = Arc::new(RuntimeSession::with_environment(
        job,
        job_id,
        run_attempt,
        runtime_environment,
    ));
    let result = run_supervisor(listener, descriptor, session.clone()).await;
    let _ = std::fs::remove_file(socket_path);
    result?;
    let snapshot = session.snapshot().await;
    let exit_code = snapshot
        .exit_code
        .unwrap_or_else(|| 128 + snapshot.signal.unwrap_or(0));
    obligations.append(&RuntimeObligation::ExitObserved {
        exit_code,
        signal: snapshot.signal.unwrap_or(0),
    })?;
    if !controller_addr.is_empty()
        && !reporting_node.is_empty()
        && crate::agent_server::report_completion(
            &controller_addr,
            job_id,
            exit_code,
            snapshot.signal.unwrap_or(0),
            run_attempt,
            &reporting_node,
            None,
        )
        .await
    {
        obligations.append(&RuntimeObligation::CompletionAcknowledged)?;
    }
    Ok(exit_code)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionLiveness {
    Live,
    Stale,
}

#[derive(Debug)]
pub(crate) struct DiscoveredRuntimeSessions {
    pub live: Vec<RuntimeSessionDescriptor>,
    pub rejected: Vec<(PathBuf, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingRuntimeCompletion {
    pub job_id: u32,
    pub run_attempt: u32,
    pub exit_code: i32,
    pub signal: i32,
}

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
                    rejected: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };

        let mut live = Vec::new();
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
                    Ok(SessionLiveness::Stale) => {
                        rejected.push((path, "runtime PID is stale".into()))
                    }
                    Err(error) => rejected.push((path, error.to_string())),
                },
                Err(error) => rejected.push((path, error.to_string())),
            }
        }

        Ok(DiscoveredRuntimeSessions { live, rejected })
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

    pub(crate) fn acknowledge_completion(
        &self,
        completion: &PendingRuntimeCompletion,
    ) -> io::Result<()> {
        self.obligations(completion.job_id, completion.run_attempt)
            .append(&RuntimeObligation::CompletionAcknowledged)
    }

    fn load_descriptor(&self, session_dir: &Path) -> io::Result<RuntimeSessionDescriptor> {
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
        assert_eq!(discovered.rejected.len(), 1);
        assert_eq!(discovered.rejected[0].0, store.session_dir(43, 1));
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
    fn stale_exit_without_acknowledgement_is_recoverable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = RuntimeSessionStore::new(temp.path());
        let mut descriptor = descriptor(42, 3, std::process::id());
        descriptor.process_start_ticks += 1;
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
        assert!(matches!(
            validate_hello(
                &descriptor,
                &descriptor.capability,
                &descriptor.capability,
                2,
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
    async fn control_loop_acknowledges_allocation_signal() {
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
                        signal: nix::sys::signal::Signal::SIGTERM as i32,
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
        assert!(!session.snapshot().await.active);
    }

    #[tokio::test]
    async fn control_loop_runs_a_logical_step_and_returns_its_output() {
        let (server_stream, client_stream) = UnixStream::pair().expect("socket pair");
        let session = RuntimeSession::new(RunningJob::AllocationOnly, 42, 3);
        let server = tokio::spawn(async move { serve_control(server_stream, &session).await });
        let (reader, mut writer) = client_stream.into_split();
        let request = RuntimeRequest::LaunchStep {
            step: RuntimeStepLaunchSpec {
                step_id: 7,
                program: "sh".into(),
                args: vec!["-c".into(), "printf runtime-step".into()],
                work_dir: "/tmp".into(),
                environment: std::collections::HashMap::new(),
                uid: nix::unistd::geteuid().as_raw(),
                gid: nix::unistd::getegid().as_raw(),
                memlock: RuntimeMemlock::Inherit,
            },
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
            })
            .await
            .expect_err("cancelled step must not spawn");
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
            })
            .await
            .expect("replay completed step");
        assert_eq!(first.stdout, "original");
        assert_eq!(replay.stdout, "original");
    }
}
