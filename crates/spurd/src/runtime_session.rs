// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const DESCRIPTOR_FILE: &str = "descriptor.json";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeSessionDescriptor {
    pub format_version: u32,
    pub job_id: u32,
    pub run_attempt: u32,
    pub pid: u32,
    pub process_start_ticks: u64,
    pub socket_path: PathBuf,
    pub cgroup_path: PathBuf,
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

pub(crate) struct RuntimeSessionStore {
    root: PathBuf,
}

impl RuntimeSessionStore {
    pub(crate) fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            root: state_dir.into().join("runtime"),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn session_dir(&self, job_id: u32, run_attempt: u32) -> PathBuf {
        self.root.join(format!("{job_id}.{run_attempt}"))
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
        RuntimeSessionDescriptor {
            format_version: FORMAT_VERSION,
            job_id,
            run_attempt,
            pid,
            process_start_ticks: process_start_ticks(pid).expect("test process must exist"),
            socket_path: PathBuf::from("/run/spur/runtime.sock"),
            cgroup_path: PathBuf::from("/sys/fs/cgroup/spur/test"),
        }
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
        write_descriptor(&store, &descriptor(42, 3, pid));

        let stale = descriptor(43, 1, pid);
        let mut stale = stale;
        stale.process_start_ticks += 1;
        write_descriptor(&store, &stale);

        let discovered = store.discover_live().expect("discover sessions");
        assert_eq!(discovered.live, vec![descriptor(42, 3, pid)]);
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
}
