// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::executor::RunningJob;

const DESCRIPTOR_FILE: &str = "descriptor.json";
const FORMAT_VERSION: u32 = 1;
pub const PROTOCOL_VERSION: u32 = 1;

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

pub struct RuntimeSession {
    job: Mutex<RunningJob>,
    snapshot: Mutex<RuntimeSnapshot>,
}

impl RuntimeSession {
    pub fn new(job: RunningJob, job_id: u32, run_attempt: u32) -> Self {
        Self {
            job: Mutex::new(job),
            snapshot: Mutex::new(RuntimeSnapshot {
                job_id,
                run_attempt,
                active: true,
                exit_code: None,
                signal: None,
            }),
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
        self.job
            .lock()
            .await
            .kill_signal(signal)
            .map_err(io::Error::other)
    }

    pub async fn begin_teardown(&self) {
        self.snapshot.lock().await.active = false;
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
        }
    }
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

    pub fn publish(&self, descriptor: &RuntimeSessionDescriptor) -> io::Result<()> {
        let session_dir = self.session_dir(descriptor.job_id, descriptor.run_attempt);
        create_private_dir(&self.root)?;
        create_private_dir(&session_dir)?;
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
}
