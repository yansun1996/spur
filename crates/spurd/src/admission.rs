// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The node's entitlement ledger. `runtime/` answers "is it alive"; this answers
//! "what is it entitled to", and survives the supervisor that earned it.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use spur_core::step::StepId;

use crate::stepd::{create_private_dir_all, publish_private};

/// Bumped only for a change old readers cannot absorb; additive fields default.
pub const ADMISSION_SCHEMA_VERSION: u32 = 1;

const RUN_FILE: &str = "run.json";
const PARTICIPANTS_DIR: &str = "participants";

/// Records with no expiry of their own age out on this, measured from creation.
pub const DEFAULT_RETENTION_SECS: u64 = 3600;

/// The slice a run is entitled to. Device ids are node-local.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedResources {
    #[serde(default)]
    pub cpu_ids: Vec<u32>,
    #[serde(default)]
    pub memory_mb: u64,
    #[serde(default)]
    pub gpu_devices: Vec<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    #[default]
    Admitted,
    Running,
    Cleaning,
    Cleaned,
}

/// A hook whose owner died mid-run loads as `Unknown` and is never re-run: the
/// agent cannot tell whether its side effects landed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookState {
    #[default]
    NotStarted,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl HookState {
    /// What a persisted `Running` means once its owner is gone.
    pub fn settled_after_owner_loss(self) -> Self {
        match self {
            Self::Running => Self::Unknown,
            other => other,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupPhase {
    #[default]
    NotStarted,
    Running,
    Complete,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupState {
    #[serde(default)]
    pub phase: CleanupPhase,
    #[serde(default)]
    pub epilog: HookState,
}

/// Why evidence is being preserved instead of released. Set by the agent,
/// cleared only once the controller has reconciled the node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictHold {
    pub reason: String,
    #[serde(default)]
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerAck {
    #[serde(default)]
    pub release_raft_index: Option<u64>,
}

/// `admission/<job>.<attempt>/run.json`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunAdmission {
    pub schema_version: u32,
    pub job_id: u32,
    pub run_attempt: u32,
    /// Verified against this agent's node on load; a foreign record is rejected
    /// rather than adopted, since its slice describes another node's hardware.
    pub node: String,
    #[serde(default)]
    pub allocation: AdmittedResources,
    #[serde(default)]
    pub state: RunState,
    /// Agent wall clock. The only age a run whose launch aborted before any
    /// expiry was set will ever have, so it is what makes such a run collectable.
    #[serde(default)]
    pub created_at_unix_ms: u64,
    #[serde(default)]
    pub reject_before_unix_ms: u64,
    #[serde(default)]
    pub max_launch_expiry_unix_ms: u64,
    #[serde(default)]
    pub prolog: HookState,
    #[serde(default)]
    pub lifecycle_owner_step: Option<StepId>,
    #[serde(default)]
    pub cleanup: CleanupState,
    #[serde(default)]
    pub conflict_hold: Option<ConflictHold>,
    #[serde(default)]
    pub controller_ack: ControllerAck,
}

impl RunAdmission {
    pub fn new(
        job_id: u32,
        run_attempt: u32,
        node: impl Into<String>,
        allocation: AdmittedResources,
        created_at_unix_ms: u64,
    ) -> Self {
        Self {
            schema_version: ADMISSION_SCHEMA_VERSION,
            job_id,
            run_attempt,
            node: node.into(),
            allocation,
            state: RunState::Admitted,
            created_at_unix_ms,
            reject_before_unix_ms: 0,
            max_launch_expiry_unix_ms: 0,
            prolog: HookState::NotStarted,
            lifecycle_owner_step: None,
            cleanup: CleanupState::default(),
            conflict_hold: None,
            controller_ack: ControllerAck::default(),
        }
    }

    /// When this run stops being interesting if nothing else ever happens to it.
    fn ages_out_at(&self, retention_ms: u64) -> u64 {
        self.max_launch_expiry_unix_ms
            .max(self.created_at_unix_ms.saturating_add(retention_ms))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantLifecycle {
    #[default]
    Admitted,
    Starting,
    Running,
    Exited,
    Cleaned,
}

/// `(pid, start_ticks)` is unique only within one boot, and the spool outlives a
/// reboot, so the boot scopes the identity. A record from another boot is dead.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorRef {
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub start_ticks: u64,
    #[serde(default)]
    pub boot_id: Option<String>,
}

/// How a persisted supervisor identity compares to the running system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootScope {
    /// Same boot: `(pid, start_ticks)` is meaningful and may be compared.
    Same,
    /// A different boot: nothing from before it survives.
    Different,
    /// One side has no boot id. Never `Different` — treating an unknown as a
    /// change declares live supervisors dead and releases running jobs' slices.
    Unknown,
}

impl SupervisorRef {
    pub fn boot_scope(&self, current_boot_id: Option<&str>) -> BootScope {
        match (self.boot_id.as_deref(), current_boot_id) {
            (Some(recorded), Some(current)) if recorded == current => BootScope::Same,
            (Some(_), Some(_)) => BootScope::Different,
            _ => BootScope::Unknown,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalReport {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub acknowledged: bool,
}

/// `admission/<job>.<attempt>/participants/<step>.json`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantAdmission {
    pub schema_version: u32,
    pub job_id: u32,
    pub run_attempt: u32,
    pub step_id: StepId,
    pub node: String,
    #[serde(default)]
    pub command_digest: String,
    #[serde(default)]
    pub allocation_subset: AdmittedResources,
    #[serde(default)]
    pub issued_at_unix_ms: u64,
    /// Controller-stamped and respected as delivered. A duration measured from
    /// local admission would hand a launch delayed in transit a fresh window.
    #[serde(default)]
    pub expires_at_unix_ms: u64,
    #[serde(default)]
    pub lifecycle: ParticipantLifecycle,
    #[serde(default)]
    pub supervisor: Option<SupervisorRef>,
    #[serde(default)]
    pub final_report: FinalReport,
}

impl ParticipantAdmission {
    pub fn new(
        job_id: u32,
        run_attempt: u32,
        step_id: StepId,
        node: impl Into<String>,
        allocation_subset: AdmittedResources,
    ) -> Self {
        Self {
            schema_version: ADMISSION_SCHEMA_VERSION,
            job_id,
            run_attempt,
            step_id,
            node: node.into(),
            command_digest: String::new(),
            allocation_subset,
            issued_at_unix_ms: 0,
            expires_at_unix_ms: 0,
            lifecycle: ParticipantLifecycle::Admitted,
            supervisor: None,
            final_report: FinalReport::default(),
        }
    }

    /// A deadline only ever *extends* a run past its own floor. Absent reads as
    /// "adds nothing", so a pre-upgrade launch cannot pin its run forever.
    fn holds_run_until(&self) -> u64 {
        self.expires_at_unix_ms
    }
}

/// One run and its participants, as loaded from disk.
#[derive(Debug, Clone)]
pub struct AdmittedRun {
    pub run: RunAdmission,
    pub participants: Vec<ParticipantAdmission>,
}

impl AdmittedRun {
    /// Removable once cleanup finished and nothing is still owed. An owed report
    /// or a conflict hold is the evidence that must outlive the processes.
    pub fn is_settled(&self) -> bool {
        self.run.state == RunState::Cleaned
            && self.run.conflict_hold.is_none()
            && self
                .participants
                .iter()
                .all(|p| !p.final_report.required || p.final_report.acknowledged)
    }

    /// Past every deadline with nothing left that could still act. Gating on a
    /// single `RunState` would make a run whose supervisor died mid-run immortal.
    pub fn aged_out(&self, now_unix_ms: u64, retention_ms: u64) -> bool {
        // An unknown creation time has no age, so it is never old enough.
        if self.run.created_at_unix_ms == 0 {
            return false;
        }
        self.participants.iter().all(|participant| {
            !matches!(
                participant.lifecycle,
                ParticipantLifecycle::Starting | ParticipantLifecycle::Running
            )
            // An owed report is an outstanding obligation, not staleness. Age
            // must never be the thing that drops one.
            && (!participant.final_report.required || participant.final_report.acknowledged)
            && now_unix_ms >= participant.holds_run_until()
        }) && now_unix_ms >= self.run.ages_out_at(retention_ms)
    }
}

/// What the agent found on disk for one admitted run at startup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunEvidence {
    /// A supervisor whose `(pid, start_ticks)` still matches a live process.
    pub supervisor_matches_a_live_process: bool,
    pub boot_scope_of_supervisor: Option<BootScope>,
    pub recorded_exit: Option<(i32, i32)>,
    /// Admitted but never spawned: no supervisor was ever recorded.
    pub never_spawned: bool,
    /// Runtime state with no admission record behind it, or an unreadable one.
    pub residual_or_corrupt: bool,
}

/// What the evidence says happened to a run. It decides what the agent
/// *reports*, never what it frees: only an acknowledgement frees a slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunDisposition {
    Running,
    SettledWithExit {
        exit_code: i32,
        signal: i32,
    },
    /// A reboot leaves nothing to be uncertain about, so this is the one
    /// disposition that reports a definite death with no exit code.
    DeadByReboot,
    NeverStarted,
    /// The supervisor is gone and recorded no exit. The agent does not know
    /// what happened and must say so rather than guess either way.
    Unknown,
    Corrupt,
}

impl RunDisposition {
    /// Always false. The agent may refuse and may hold, but a refusal is
    /// evidence for the controller to reconcile, never licence to free a slice.
    pub fn releases_locally(self) -> bool {
        false
    }

    /// Whether the controller has to reconcile this run before the node is
    /// trusted: the agent cannot resolve it from local evidence alone.
    pub fn needs_reconciliation(self) -> bool {
        matches!(self, Self::Corrupt | Self::Unknown)
    }
}

pub fn classify_run(evidence: &RunEvidence) -> RunDisposition {
    if evidence.residual_or_corrupt {
        return RunDisposition::Corrupt;
    }
    // Checked before liveness: across a reboot a `(pid, start_ticks)` match is a
    // coincidence, and adopting it would hold a phantom and report it running.
    if evidence.boot_scope_of_supervisor == Some(BootScope::Different) {
        return RunDisposition::DeadByReboot;
    }
    if evidence.supervisor_matches_a_live_process {
        return RunDisposition::Running;
    }
    if let Some((exit_code, signal)) = evidence.recorded_exit {
        return RunDisposition::SettledWithExit { exit_code, signal };
    }
    if evidence.never_spawned {
        return RunDisposition::NeverStarted;
    }
    RunDisposition::Unknown
}

/// A record the agent could not adopt. Held rather than deleted, because the
/// record is the only evidence that something here may still hold a claim.
#[derive(Debug, Clone)]
pub struct RejectedAdmission {
    pub path: PathBuf,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct LoadedAdmissions {
    pub runs: Vec<AdmittedRun>,
    pub rejected: Vec<RejectedAdmission>,
}

#[derive(Clone)]
pub struct AdmissionStore {
    root: PathBuf,
    node: String,
}

impl AdmissionStore {
    pub fn new(state_dir: impl Into<PathBuf>, node: impl Into<String>) -> Self {
        Self {
            root: state_dir.into().join("admission"),
            node: node.into(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn run_dir(&self, job_id: u32, run_attempt: u32) -> PathBuf {
        self.root.join(format!("{job_id}.{run_attempt}"))
    }

    fn participants_dir(&self, job_id: u32, run_attempt: u32) -> PathBuf {
        self.run_dir(job_id, run_attempt).join(PARTICIPANTS_DIR)
    }

    /// Every level, because `create_dir_all` leaves intermediates at the umask
    /// default and this tree holds the environment a run was admitted with.
    pub(crate) fn prepare_run_dir(&self, job_id: u32, run_attempt: u32) -> io::Result<PathBuf> {
        create_private_dir_all(&self.root)?;
        let dir = self.run_dir(job_id, run_attempt);
        create_private_dir_all(&dir)?;
        Ok(dir)
    }

    fn prepare_participants_dir(&self, job_id: u32, run_attempt: u32) -> io::Result<PathBuf> {
        let dir = self
            .prepare_run_dir(job_id, run_attempt)?
            .join(PARTICIPANTS_DIR);
        create_private_dir_all(&dir)?;
        Ok(dir)
    }

    /// Persist the entitlement before anything is spawned against it. A crash
    /// before this leaves no record and no process; a crash after leaves both.
    pub fn admit_run(&self, run: &RunAdmission) -> io::Result<()> {
        let dir = self.prepare_run_dir(run.job_id, run.run_attempt)?;
        publish_private(&dir, RUN_FILE, &encode(run)?)
    }

    /// The write fsyncs twice. Callers hold locks every other job RPC needs, so
    /// it must not also occupy the executor thread.
    pub async fn admit_run_async(&self, run: RunAdmission) -> io::Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.admit_run(&run))
            .await
            .map_err(|error| io::Error::other(format!("admission write failed: {error}")))?
    }

    pub async fn admit_participant_async(
        &self,
        participant: ParticipantAdmission,
    ) -> io::Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.admit_participant(&participant))
            .await
            .map_err(|error| io::Error::other(format!("admission write failed: {error}")))?
    }

    pub fn admit_participant(&self, participant: &ParticipantAdmission) -> io::Result<()> {
        let dir = self.prepare_participants_dir(participant.job_id, participant.run_attempt)?;
        publish_private(
            &dir,
            &format!("{}.json", participant.step_id),
            &encode(participant)?,
        )
    }

    pub fn load_run(&self, job_id: u32, run_attempt: u32) -> io::Result<RunAdmission> {
        let path = self.run_dir(job_id, run_attempt).join(RUN_FILE);
        let run: RunAdmission = decode(&fs::read(&path)?)?;
        self.validate_run(&run, job_id, run_attempt)?;
        Ok(run)
    }

    fn validate_run(&self, run: &RunAdmission, job_id: u32, run_attempt: u32) -> io::Result<()> {
        if run.schema_version > ADMISSION_SCHEMA_VERSION {
            return Err(invalid(format!(
                "run record schema {} is newer than {ADMISSION_SCHEMA_VERSION}",
                run.schema_version
            )));
        }
        if run.job_id != job_id || run.run_attempt != run_attempt {
            return Err(invalid(format!(
                "run record names {}.{} but lives in {job_id}.{run_attempt}",
                run.job_id, run.run_attempt
            )));
        }
        if run.node != self.node {
            return Err(invalid(format!(
                "run record belongs to node '{}', not '{}'",
                run.node, self.node
            )));
        }
        Ok(())
    }

    /// This run's participants, plus the files that could not be read.
    pub fn participants(
        &self,
        job_id: u32,
        run_attempt: u32,
    ) -> io::Result<(Vec<ParticipantAdmission>, Vec<RejectedAdmission>)> {
        let dir = self.participants_dir(job_id, run_attempt);
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok((Vec::new(), Vec::new()))
            }
            Err(error) => return Err(error),
        };
        // Sorted by step so recovery order does not depend on the filesystem.
        let mut found: BTreeMap<StepId, ParticipantAdmission> = BTreeMap::new();
        let mut rejected = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            // A publish in flight is named `<step>.json.<uuid>.tmp`.
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            match self.load_participant(&path, job_id, run_attempt) {
                Ok(participant) => {
                    found.insert(participant.step_id, participant);
                }
                // One damaged file must not discard an otherwise-good run: that
                // would forget a claim the node may still be holding.
                Err(error) => rejected.push(RejectedAdmission {
                    path,
                    reason: error.to_string(),
                }),
            }
        }
        Ok((found.into_values().collect(), rejected))
    }

    fn load_participant(
        &self,
        path: &Path,
        job_id: u32,
        run_attempt: u32,
    ) -> io::Result<ParticipantAdmission> {
        let participant: ParticipantAdmission = decode(&fs::read(path)?)?;
        if participant.schema_version > ADMISSION_SCHEMA_VERSION {
            return Err(invalid(format!(
                "participant record schema {} is newer than {ADMISSION_SCHEMA_VERSION}",
                participant.schema_version
            )));
        }
        if participant.job_id != job_id
            || participant.run_attempt != run_attempt
            || participant.node != self.node
        {
            return Err(invalid(format!(
                "participant record at {} does not belong to {job_id}.{run_attempt} on '{}'",
                path.display(),
                self.node
            )));
        }
        Ok(participant)
    }

    fn run_dirs(&self) -> io::Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut dirs: Vec<PathBuf> = entries
            .flatten()
            .filter(|entry| entry.file_type().map(|kind| kind.is_dir()).unwrap_or(true))
            .map(|entry| entry.path())
            .collect();
        dirs.sort();
        Ok(dirs)
    }

    /// Every run on this node. One unreadable record is reported, never fatal:
    /// this runs at startup, and refusing to boot would strand the whole node.
    pub fn load_all(&self) -> io::Result<LoadedAdmissions> {
        let mut loaded = LoadedAdmissions::default();
        for dir in self.run_dirs()? {
            let Some((job_id, run_attempt)) = parse_run_dir_name(&dir) else {
                loaded.rejected.push(RejectedAdmission {
                    path: dir,
                    reason: "run directory name is not <job>.<attempt>".into(),
                });
                continue;
            };
            let run = match self.load_run(job_id, run_attempt) {
                Ok(run) => run,
                Err(error) => {
                    loaded.rejected.push(RejectedAdmission {
                        path: dir,
                        reason: error.to_string(),
                    });
                    continue;
                }
            };
            match self.participants(job_id, run_attempt) {
                Ok((participants, rejected)) => {
                    loaded.rejected.extend(rejected);
                    loaded.runs.push(AdmittedRun { run, participants });
                }
                Err(error) => loaded.rejected.push(RejectedAdmission {
                    path: dir,
                    reason: error.to_string(),
                }),
            }
        }
        Ok(loaded)
    }

    /// Read-modify-write so cleanup cannot silently discard a conflict hold or
    /// the creation time the age rule depends on.
    pub fn mark_run_cleaned(&self, job_id: u32, run_attempt: u32) -> io::Result<bool> {
        let mut run = match self.load_run(job_id, run_attempt) {
            Ok(run) => run,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if run.state == RunState::Cleaned {
            return Ok(true);
        }
        run.state = RunState::Cleaned;
        run.cleanup.phase = CleanupPhase::Complete;
        run.prolog = run.prolog.settled_after_owner_loss();
        run.cleanup.epilog = run.cleanup.epilog.settled_after_owner_loss();
        self.admit_run(&run)?;
        Ok(true)
    }

    /// Record the supervisor that now speaks for a participant. Read-modify-write
    /// so it cannot discard the deadline or digest the launch was admitted under.
    pub fn record_supervisor(
        &self,
        job_id: u32,
        run_attempt: u32,
        step_id: StepId,
        supervisor: SupervisorRef,
    ) -> io::Result<bool> {
        let path = self
            .participants_dir(job_id, run_attempt)
            .join(format!("{step_id}.json"));
        let mut participant = match self.load_participant(&path, job_id, run_attempt) {
            Ok(participant) => participant,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        participant.supervisor = Some(supervisor);
        participant.lifecycle = ParticipantLifecycle::Running;
        self.admit_participant(&participant)?;
        Ok(true)
    }

    pub fn remove_participant(
        &self,
        job_id: u32,
        run_attempt: u32,
        step_id: StepId,
    ) -> io::Result<()> {
        let path = self
            .participants_dir(job_id, run_attempt)
            .join(format!("{step_id}.json"));
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn remove_run(&self, job_id: u32, run_attempt: u32) -> io::Result<()> {
        match fs::remove_dir_all(self.run_dir(job_id, run_attempt)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Two removal rules, and nothing else may delete a record. A settled run is
    /// removed because it owes nothing; an aged-out one because it never can.
    pub fn sweep(&self, now_unix_ms: u64, retention_ms: u64) -> io::Result<usize> {
        // A zero floor would collect a run the instant it is admitted, which is
        // before its launch has even spawned.
        let retention_ms = retention_ms.max(1);
        let mut removed = 0;
        for admitted in self.load_all()?.runs {
            let run = &admitted.run;
            if run.conflict_hold.is_some() {
                continue;
            }
            if admitted.is_settled() || admitted.aged_out(now_unix_ms, retention_ms) {
                self.remove_run(run.job_id, run.run_attempt)?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

pub fn parse_run_dir_name(path: &Path) -> Option<(u32, u32)> {
    let name = path.file_name()?.to_str()?;
    let (job, attempt) = name.split_once('.')?;
    let (job_id, run_attempt): (u32, u32) = (job.parse().ok()?, attempt.parse().ok()?);
    // `007.1` and `+7.1` both parse to (7, 1); accepting either would make one
    // run load twice and be swept on a sibling's behalf.
    (name == format!("{job_id}.{run_attempt}")).then_some((job_id, run_attempt))
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| invalid(format!("serialize admission: {error}")))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(|error| invalid(format!("parse admission: {error}")))
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Milliseconds since the epoch, saturating rather than panicking on a clock
/// set before 1970.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

/// The kernel's boot id, when the platform exposes one. Constant for the
/// process, and read on every descriptor write, so it is read once.
pub fn current_boot_id() -> Option<String> {
    static BOOT_ID: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    BOOT_ID
        .get_or_init(|| {
            let raw = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
            let trimmed = raw.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use spur_core::step::STEP_BATCH;

    fn store(dir: &tempfile::TempDir) -> AdmissionStore {
        AdmissionStore::new(dir.path(), "n1")
    }

    fn run_with(job_id: u32, attempt: u32, created_at: u64) -> RunAdmission {
        RunAdmission::new(
            job_id,
            attempt,
            "n1",
            AdmittedResources {
                cpu_ids: vec![0, 1],
                memory_mb: 4096,
                gpu_devices: vec![3],
            },
            created_at,
        )
    }

    #[test]
    fn a_run_round_trips_with_its_participants() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let run = run_with(7, 2, 1_000);
        store.admit_run(&run).unwrap();
        let mut participant =
            ParticipantAdmission::new(7, 2, STEP_BATCH, "n1", run.allocation.clone());
        participant.expires_at_unix_ms = 5_000;
        store.admit_participant(&participant).unwrap();

        let loaded = store.load_all().unwrap();
        assert!(loaded.rejected.is_empty());
        assert_eq!(loaded.runs.len(), 1);
        assert_eq!(loaded.runs[0].run, run);
        assert_eq!(loaded.runs[0].participants, vec![participant]);
    }

    #[test]
    fn records_and_directories_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let run = run_with(7, 1, 0);
        store.admit_run(&run).unwrap();
        store
            .admit_participant(&ParticipantAdmission::new(
                7,
                1,
                STEP_BATCH,
                "n1",
                AdmittedResources::default(),
            ))
            .unwrap();

        let mode = |p: PathBuf| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(store.root().to_path_buf()), 0o700);
        assert_eq!(mode(store.run_dir(7, 1)), 0o700);
        assert_eq!(mode(store.participants_dir(7, 1)), 0o700);
        assert_eq!(mode(store.run_dir(7, 1).join(RUN_FILE)), 0o600);
        assert_eq!(
            mode(
                store
                    .participants_dir(7, 1)
                    .join(format!("{STEP_BATCH}.json"))
            ),
            0o600
        );
    }

    #[test]
    fn a_foreign_node_record_is_rejected_not_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let mut run = run_with(7, 1, 0);
        run.node = "somewhere-else".into();
        store.admit_run(&run).unwrap();

        let loaded = store.load_all().unwrap();
        assert!(
            loaded.runs.is_empty(),
            "a foreign slice must not be adopted"
        );
        assert_eq!(loaded.rejected.len(), 1);
        assert!(loaded.rejected[0].reason.contains("somewhere-else"));
    }

    #[test]
    fn a_record_that_contradicts_its_directory_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let run = run_with(7, 1, 0);
        let target = store.prepare_run_dir(9, 4).unwrap();
        publish_private(&target, RUN_FILE, &encode(&run).unwrap()).unwrap();

        let loaded = store.load_all().unwrap();
        assert!(loaded.runs.is_empty());
        assert_eq!(loaded.rejected.len(), 1);
    }

    #[test]
    fn a_newer_schema_is_rejected_rather_than_partially_understood() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let mut run = run_with(7, 1, 0);
        run.schema_version = ADMISSION_SCHEMA_VERSION + 1;
        store.admit_run(&run).unwrap();

        let loaded = store.load_all().unwrap();
        assert!(loaded.runs.is_empty());
        assert!(loaded.rejected[0].reason.contains("newer"));
    }

    #[test]
    fn an_unreadable_record_is_reported_without_failing_the_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 0)).unwrap();
        let broken = store.prepare_run_dir(8, 1).unwrap();
        publish_private(&broken, RUN_FILE, b"{ not json").unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.runs.len(), 1, "the good run must still load");
        assert_eq!(loaded.rejected.len(), 1);
    }

    #[test]
    fn sweep_removes_a_settled_run_and_keeps_one_still_owed() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);

        let mut settled = run_with(1, 1, 1);
        settled.state = RunState::Cleaned;
        store.admit_run(&settled).unwrap();
        let mut reported = ParticipantAdmission::new(1, 1, STEP_BATCH, "n1", Default::default());
        reported.final_report = FinalReport {
            required: true,
            acknowledged: true,
        };
        store.admit_participant(&reported).unwrap();

        let mut owed = run_with(2, 1, 1);
        owed.state = RunState::Cleaned;
        store.admit_run(&owed).unwrap();
        let mut unacked = ParticipantAdmission::new(2, 1, STEP_BATCH, "n1", Default::default());
        unacked.final_report = FinalReport {
            required: true,
            acknowledged: false,
        };
        store.admit_participant(&unacked).unwrap();

        assert_eq!(store.sweep(10_000, 1_000).unwrap(), 1);
        assert!(store.load_run(1, 1).is_err());
        assert!(store.load_run(2, 1).is_ok(), "an owed report must be kept");
    }

    #[test]
    fn sweep_collects_a_run_whose_launch_never_produced_anything() {
        // Its launch aborted before any expiry was set, so created_at is the
        // only age it has; without that it could never be collected.
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 1_000)).unwrap();

        assert_eq!(store.sweep(1_500, 1_000).unwrap(), 0, "not yet aged out");
        assert_eq!(store.sweep(2_001, 1_000).unwrap(), 1);
    }

    #[test]
    fn sweep_never_collects_a_run_under_a_conflict_hold() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let hold = || {
            Some(ConflictHold {
                reason: "residual runtime state".into(),
                observed_at_unix_ms: 0,
            })
        };

        let mut cleaned = run_with(7, 1, 1);
        cleaned.state = RunState::Cleaned;
        cleaned.conflict_hold = hold();
        store.admit_run(&cleaned).unwrap();

        // Aged out as well: the hold has to outrank both removal rules, and
        // only this one reaches the age path.
        let mut aged = run_with(8, 1, 1);
        aged.conflict_hold = hold();
        store.admit_run(&aged).unwrap();

        assert_eq!(store.sweep(u64::MAX, 0).unwrap(), 0);
        assert!(store.load_run(7, 1).is_ok());
        assert!(store.load_run(8, 1).is_ok());
    }

    #[test]
    fn sweep_keeps_a_run_whose_participant_started() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 1)).unwrap();
        let mut started = ParticipantAdmission::new(7, 1, STEP_BATCH, "n1", Default::default());
        started.lifecycle = ParticipantLifecycle::Running;
        store.admit_participant(&started).unwrap();

        assert_eq!(store.sweep(u64::MAX, 0).unwrap(), 0);
    }

    #[test]
    fn a_participant_with_no_deadline_does_not_pin_its_run() {
        // A pre-upgrade controller sends no expiry; reading absent as "never
        // expires" would make such a run uncollectable.
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let run = run_with(7, 1, 1_000);
        store.admit_run(&run).unwrap();
        let participant = ParticipantAdmission::new(7, 1, STEP_BATCH, "n1", Default::default());
        assert_eq!(participant.expires_at_unix_ms, 0);
        store.admit_participant(&participant).unwrap();

        assert_eq!(store.sweep(1_500, 1_000).unwrap(), 0);
        assert_eq!(store.sweep(2_001, 1_000).unwrap(), 1);
    }

    #[test]
    fn an_unexpired_participant_deadline_holds_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 1)).unwrap();
        let mut participant = ParticipantAdmission::new(7, 1, STEP_BATCH, "n1", Default::default());
        participant.expires_at_unix_ms = 9_000;
        store.admit_participant(&participant).unwrap();

        assert_eq!(store.sweep(5_000, 1_000).unwrap(), 0);
        assert_eq!(store.sweep(9_001, 1_000).unwrap(), 1);
    }

    #[test]
    fn a_run_whose_supervisor_died_mid_run_still_ages_out() {
        // Gating age-out on one RunState made such a record immortal: it
        // reaches neither Cleaned nor stays Admitted.
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let mut stuck = run_with(7, 1, 1_000);
        stuck.state = RunState::Running;
        store.admit_run(&stuck).unwrap();
        let mut exited = ParticipantAdmission::new(7, 1, STEP_BATCH, "n1", Default::default());
        exited.lifecycle = ParticipantLifecycle::Exited;
        store.admit_participant(&exited).unwrap();

        assert_eq!(store.sweep(1_500, 1_000).unwrap(), 0);
        assert_eq!(store.sweep(2_001, 1_000).unwrap(), 1);
    }

    #[test]
    fn a_record_with_no_creation_time_is_never_aged_out() {
        // Its age would otherwise be measured from 1970, making every such
        // record instantly collectable.
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 0)).unwrap();

        assert_eq!(store.sweep(u64::MAX, 1_000).unwrap(), 0);
        assert!(store.load_run(7, 1).is_ok());
    }

    #[test]
    fn a_zero_retention_does_not_collect_a_just_admitted_run() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 5_000)).unwrap();

        assert_eq!(store.sweep(5_000, 0).unwrap(), 0);
        assert!(store.load_run(7, 1).is_ok());
    }

    #[test]
    fn marking_a_run_cleaned_preserves_the_evidence_on_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let mut run = run_with(7, 1, 1_234);
        run.state = RunState::Running;
        run.prolog = HookState::Running;
        run.conflict_hold = Some(ConflictHold {
            reason: "residual runtime state".into(),
            observed_at_unix_ms: 99,
        });
        store.admit_run(&run).unwrap();

        assert!(store.mark_run_cleaned(7, 1).unwrap());
        let after = store.load_run(7, 1).unwrap();
        assert_eq!(after.state, RunState::Cleaned);
        assert_eq!(after.created_at_unix_ms, 1_234, "the age must survive");
        assert_eq!(after.allocation, run.allocation);
        assert!(after.conflict_hold.is_some(), "a hold must survive cleanup");
        assert_eq!(
            after.prolog,
            HookState::Unknown,
            "a hook still running when its owner went away is not a success"
        );
    }

    #[test]
    fn marking_a_run_that_was_never_admitted_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!store(&dir).mark_run_cleaned(9, 9).unwrap());
    }

    #[test]
    fn one_damaged_participant_does_not_discard_its_run() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 1)).unwrap();
        store
            .admit_participant(&ParticipantAdmission::new(
                7,
                1,
                STEP_BATCH,
                "n1",
                Default::default(),
            ))
            .unwrap();
        let participants = store.run_dir(7, 1).join("participants");
        publish_private(&participants, "5.json", b"{ not json").unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(
            loaded.runs.len(),
            1,
            "forgetting the run would forget a claim the node may still hold"
        );
        assert_eq!(loaded.runs[0].participants.len(), 1);
        assert_eq!(loaded.rejected.len(), 1);
    }

    #[test]
    fn a_non_canonical_run_directory_is_rejected() {
        // `007.1` parses to (7, 1) and would make the real run load twice, then
        // be swept on the impostor's behalf.
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.admit_run(&run_with(7, 1, 1)).unwrap();
        let impostor = store.root().join("007.1");
        create_private_dir_all(&impostor).unwrap();
        publish_private(&impostor, RUN_FILE, &encode(&run_with(7, 1, 1)).unwrap()).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.runs.len(), 1);
        assert_eq!(loaded.rejected.len(), 1);
    }

    #[test]
    fn no_disposition_ever_releases_locally() {
        // The invariant the whole ladder rests on: evidence buys the content of
        // a report, never permission to act on it.
        for disposition in [
            RunDisposition::Running,
            RunDisposition::SettledWithExit {
                exit_code: 0,
                signal: 0,
            },
            RunDisposition::DeadByReboot,
            RunDisposition::NeverStarted,
            RunDisposition::Unknown,
            RunDisposition::Corrupt,
        ] {
            assert!(
                !disposition.releases_locally(),
                "{disposition:?} must not free a slice on the agent's own judgement"
            );
        }
    }

    #[test]
    fn a_live_supervisor_is_adopted() {
        assert_eq!(
            classify_run(&RunEvidence {
                supervisor_matches_a_live_process: true,
                boot_scope_of_supervisor: Some(BootScope::Same),
                ..Default::default()
            }),
            RunDisposition::Running
        );
    }

    #[test]
    fn a_reboot_outranks_a_matching_pid() {
        // Across a reboot the pid and tick pair can collide. Adopting it holds a
        // phantom and reports a dead job as running.
        assert_eq!(
            classify_run(&RunEvidence {
                supervisor_matches_a_live_process: true,
                boot_scope_of_supervisor: Some(BootScope::Different),
                ..Default::default()
            }),
            RunDisposition::DeadByReboot
        );
    }

    #[test]
    fn an_unknown_boot_still_adopts_a_matching_live_supervisor() {
        // A pre-upgrade record has no boot id. Reading that as a reboot would
        // declare running jobs dead.
        assert_eq!(
            classify_run(&RunEvidence {
                supervisor_matches_a_live_process: true,
                boot_scope_of_supervisor: Some(BootScope::Unknown),
                ..Default::default()
            }),
            RunDisposition::Running
        );
    }

    #[test]
    fn a_recorded_exit_settles_a_dead_supervisor() {
        assert_eq!(
            classify_run(&RunEvidence {
                boot_scope_of_supervisor: Some(BootScope::Same),
                recorded_exit: Some((3, 0)),
                ..Default::default()
            }),
            RunDisposition::SettledWithExit {
                exit_code: 3,
                signal: 0
            }
        );
    }

    #[test]
    fn a_dead_supervisor_with_no_exit_is_unknown_not_finished() {
        // Treating this as finished is the inference that frees a slice the
        // node may still be using.
        assert_eq!(
            classify_run(&RunEvidence {
                boot_scope_of_supervisor: Some(BootScope::Same),
                ..Default::default()
            }),
            RunDisposition::Unknown
        );
    }

    #[test]
    fn an_admitted_run_that_never_spawned_is_not_running_or_finished() {
        assert_eq!(
            classify_run(&RunEvidence {
                never_spawned: true,
                ..Default::default()
            }),
            RunDisposition::NeverStarted
        );
    }

    #[test]
    fn corruption_outranks_every_other_signal() {
        let corrupt = classify_run(&RunEvidence {
            supervisor_matches_a_live_process: true,
            recorded_exit: Some((0, 0)),
            residual_or_corrupt: true,
            ..Default::default()
        });
        assert_eq!(corrupt, RunDisposition::Corrupt);
        assert!(corrupt.needs_reconciliation());
    }

    #[test]
    fn an_absent_boot_id_is_never_read_as_a_reboot() {
        // Inverting this declares live supervisors dead and releases the
        // allocations of running jobs.
        let no_record = SupervisorRef::default();
        assert_eq!(no_record.boot_scope(Some("abc")), BootScope::Unknown);

        let recorded = SupervisorRef {
            pid: 5,
            start_ticks: 9,
            boot_id: Some("abc".into()),
        };
        assert_eq!(recorded.boot_scope(None), BootScope::Unknown);
        assert_eq!(recorded.boot_scope(Some("abc")), BootScope::Same);
        assert_eq!(recorded.boot_scope(Some("def")), BootScope::Different);
    }

    #[test]
    fn a_hook_running_when_its_owner_died_settles_as_unknown() {
        assert_eq!(
            HookState::Running.settled_after_owner_loss(),
            HookState::Unknown
        );
        for settled in [
            HookState::NotStarted,
            HookState::Succeeded,
            HookState::Failed,
        ] {
            assert_eq!(settled.settled_after_owner_loss(), settled);
        }
    }

    /// Frozen at the first shipped shape. Never regenerate it: a field added
    /// without `#[serde(default)]` must fail here rather than on a live node.
    const FROZEN_RUN_V1: &str = r#"{
        "schema_version": 1,
        "job_id": 42,
        "run_attempt": 3,
        "node": "n1",
        "allocation": {"cpu_ids": [0,1], "memory_mb": 2048, "gpu_devices": [2]},
        "state": "running",
        "created_at_unix_ms": 1700000000000,
        "reject_before_unix_ms": 0,
        "max_launch_expiry_unix_ms": 1700000120000,
        "prolog": "succeeded",
        "lifecycle_owner_step": 4294967294,
        "cleanup": {"phase": "not_started", "epilog": "not_started"},
        "conflict_hold": null,
        "controller_ack": {"release_raft_index": null}
    }"#;

    const FROZEN_PARTICIPANT_V1: &str = r#"{
        "schema_version": 1,
        "job_id": 42,
        "run_attempt": 3,
        "step_id": 4294967294,
        "node": "n1",
        "command_digest": "sha256:abc",
        "allocation_subset": {"cpu_ids": [0], "memory_mb": 1024, "gpu_devices": []},
        "issued_at_unix_ms": 1700000000000,
        "expires_at_unix_ms": 1700000120000,
        "lifecycle": "running",
        "supervisor": {"pid": 991, "start_ticks": 7788, "boot_id": "b-1"},
        "final_report": {"required": true, "acknowledged": false}
    }"#;

    #[test]
    fn frozen_records_still_load() {
        let run: RunAdmission = serde_json::from_str(FROZEN_RUN_V1).unwrap();
        assert_eq!(run.job_id, 42);
        assert_eq!(run.state, RunState::Running);
        assert_eq!(run.prolog, HookState::Succeeded);
        assert_eq!(run.allocation.gpu_devices, vec![2]);

        let participant: ParticipantAdmission =
            serde_json::from_str(FROZEN_PARTICIPANT_V1).unwrap();
        assert_eq!(participant.lifecycle, ParticipantLifecycle::Running);
        assert_eq!(
            participant.supervisor.unwrap().boot_id.as_deref(),
            Some("b-1")
        );
        assert!(participant.final_report.required);
    }

    /// The minimum a pre-upgrade or partially-written record can carry. Every
    /// optional field must default rather than fail the load.
    #[test]
    fn a_record_with_only_required_fields_loads() {
        let run: RunAdmission =
            serde_json::from_str(r#"{"schema_version":1,"job_id":1,"run_attempt":0,"node":"n1"}"#)
                .unwrap();
        assert_eq!(run.state, RunState::Admitted);
        assert_eq!(run.prolog, HookState::NotStarted);
        assert!(run.conflict_hold.is_none());

        let participant: ParticipantAdmission = serde_json::from_str(
            r#"{"schema_version":1,"job_id":1,"run_attempt":0,"step_id":1,"node":"n1"}"#,
        )
        .unwrap();
        assert_eq!(participant.lifecycle, ParticipantLifecycle::Admitted);
        assert!(participant.supervisor.is_none());
    }
}
