use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use allocdb_core::command::{ClientRequest, Command as AllocCommand};
use allocdb_core::ids::ClientId;
use allocdb_core::ids::OperationId;
use allocdb_core::ids::Slot;
use allocdb_node::jepsen::JepsenRunSpec;

use super::runs::effective_minimum_fault_window_secs;
use super::{
    JEPSEN_LATEST_STATUS_FILE_NAME, JEPSEN_RUN_STATUS_VERSION, NEXT_PROBE_RESOURCE_ID,
    append_text_line, current_time_millis, parse_optional_bool, parse_optional_path,
    parse_optional_string, parse_optional_u64, parse_optional_usize, parse_required_u32,
    parse_required_u64, parse_required_u128, required_field, sanitize_run_id,
    write_text_atomically,
};

#[derive(Clone, Copy)]
pub(super) struct RequestNamespace {
    client_id: ClientId,
    slot_base: u64,
}

impl RequestNamespace {
    pub(super) fn new() -> Self {
        let millis = u64::try_from(current_time_millis()).unwrap_or(u64::MAX / 1024);
        let nonce = NEXT_PROBE_RESOURCE_ID.fetch_add(1, Ordering::Relaxed);
        Self::from_parts(
            ClientId((u128::from(millis) << 32) | u128::from(nonce)),
            millis
                .saturating_mul(1024)
                .saturating_add(nonce.saturating_mul(128)),
        )
    }

    pub(super) fn from_parts(client_id: ClientId, slot_base: u64) -> Self {
        Self {
            client_id,
            slot_base,
        }
    }

    #[cfg(test)]
    pub(super) fn client_id(self) -> ClientId {
        self.client_id
    }

    pub(super) fn slot(self, offset: u64) -> Slot {
        Slot(self.slot_base.saturating_add(offset))
    }

    pub(super) fn client_request(
        self,
        operation_id: OperationId,
        command: AllocCommand,
    ) -> ClientRequest {
        ClientRequest {
            operation_id,
            client_id: self.client_id,
            command,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RunTrackerState {
    Running,
    Passed,
    Failed,
}

impl RunTrackerState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "running" => Ok(Self::Running),
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("invalid run state `{other}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RunTrackerPhase {
    Waiting,
    VerifyingSurface,
    Executing,
    Analyzing,
    Archiving,
    Completed,
    Failed,
}

impl RunTrackerPhase {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::VerifyingSurface => "verifying_surface",
            Self::Executing => "executing",
            Self::Analyzing => "analyzing",
            Self::Archiving => "archiving",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "waiting" => Ok(Self::Waiting),
            "verifying_surface" => Ok(Self::VerifyingSurface),
            "executing" => Ok(Self::Executing),
            "analyzing" => Ok(Self::Analyzing),
            "archiving" => Ok(Self::Archiving),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("invalid run phase `{other}`")),
        }
    }
}

#[derive(Clone)]
pub(super) struct RunTracker {
    backend_name: String,
    run_id: String,
    status_path: PathBuf,
    latest_status_path: PathBuf,
    events_path: PathBuf,
    started_at_millis: u128,
    minimum_fault_window_secs: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RunStatusSnapshot {
    pub(super) backend_name: String,
    pub(super) run_id: String,
    pub(super) state: RunTrackerState,
    pub(super) phase: RunTrackerPhase,
    pub(super) detail: String,
    pub(super) started_at_millis: u128,
    pub(super) updated_at_millis: u128,
    pub(super) elapsed_secs: u64,
    pub(super) minimum_fault_window_secs: Option<u64>,
    pub(super) history_events: u64,
    pub(super) history_file: Option<PathBuf>,
    pub(super) artifact_bundle: Option<PathBuf>,
    pub(super) logs_archive: Option<PathBuf>,
    pub(super) release_gate_passed: Option<bool>,
    pub(super) blockers: Option<usize>,
    pub(super) last_error: Option<String>,
}

impl RunTracker {
    pub(super) fn new(
        backend_name: &str,
        run_spec: &JepsenRunSpec,
        output_root: &Path,
    ) -> Result<Self, String> {
        fs::create_dir_all(output_root).map_err(|error| {
            format!(
                "failed to create Jepsen output root {}: {error}",
                output_root.display()
            )
        })?;
        let run_id = sanitize_run_id(&run_spec.run_id);
        let tracker = Self {
            backend_name: String::from(backend_name),
            run_id: run_spec.run_id.clone(),
            status_path: output_root.join(format!("{run_id}-status.txt")),
            latest_status_path: output_root.join(JEPSEN_LATEST_STATUS_FILE_NAME),
            events_path: output_root.join(format!("{run_id}-events.log")),
            started_at_millis: current_time_millis(),
            minimum_fault_window_secs: effective_minimum_fault_window_secs(run_spec),
        };
        tracker.write_status(&RunStatusSnapshot {
            backend_name: tracker.backend_name.clone(),
            run_id: tracker.run_id.clone(),
            state: RunTrackerState::Running,
            phase: RunTrackerPhase::Waiting,
            detail: String::from("initializing"),
            started_at_millis: tracker.started_at_millis,
            updated_at_millis: tracker.started_at_millis,
            elapsed_secs: 0,
            minimum_fault_window_secs: tracker.minimum_fault_window_secs,
            history_events: 0,
            history_file: None,
            artifact_bundle: None,
            logs_archive: None,
            release_gate_passed: None,
            blockers: None,
            last_error: None,
        })?;
        tracker.append_event("run initialized")?;
        Ok(tracker)
    }

    pub(super) fn snapshot(&self) -> Result<RunStatusSnapshot, String> {
        load_run_status_snapshot(&self.status_path)
    }

    pub(super) fn set_phase(&self, phase: RunTrackerPhase, detail: &str) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.phase = phase;
        snapshot.detail = String::from(detail);
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        self.write_status(&snapshot)?;
        self.append_event(detail)
    }

    pub(super) fn set_history_events(&self, history_events: u64) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.history_events = history_events;
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        self.write_status(&snapshot)
    }

    pub(super) fn complete(
        &self,
        history_file: &Path,
        artifact_bundle: &Path,
        logs_archive: &Path,
        report: &allocdb_node::jepsen::JepsenAnalysisReport,
    ) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.state = if report.release_gate_passed() {
            RunTrackerState::Passed
        } else {
            RunTrackerState::Failed
        };
        snapshot.phase = RunTrackerPhase::Completed;
        snapshot.detail = String::from("run completed");
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        snapshot.history_file = Some(history_file.to_path_buf());
        snapshot.artifact_bundle = Some(artifact_bundle.to_path_buf());
        snapshot.logs_archive = Some(logs_archive.to_path_buf());
        snapshot.release_gate_passed = Some(report.release_gate_passed());
        snapshot.blockers = Some(report.blockers.len());
        snapshot.last_error = None;
        self.write_status(&snapshot)?;
        self.append_event("run completed")
    }

    pub(super) fn fail(&self, phase: RunTrackerPhase, error: &str) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.state = RunTrackerState::Failed;
        snapshot.phase = phase;
        snapshot.detail = String::from("run failed");
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        snapshot.last_error = Some(String::from(error));
        self.write_status(&snapshot)?;
        self.append_event(&format!("error: {error}"))
    }

    pub(super) fn append_event(&self, detail: &str) -> Result<(), String> {
        let line = format!(
            "time_millis={} detail={}\n",
            current_time_millis(),
            encode_tracker_field(detail)
        );
        append_text_line(&self.events_path, &line)
    }

    fn write_status(&self, snapshot: &RunStatusSnapshot) -> Result<(), String> {
        let bytes = encode_run_status_snapshot(snapshot);
        write_text_atomically(&self.status_path, &bytes)?;
        write_text_atomically(&self.latest_status_path, &bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WatchEvent {
    pub(super) time_millis: u128,
    pub(super) detail: String,
}

pub(super) fn run_status_path(output_root: &Path, run_id: &str) -> PathBuf {
    output_root.join(format!("{}-status.txt", sanitize_run_id(run_id)))
}

pub(super) fn run_events_path(output_root: &Path, run_id: &str) -> PathBuf {
    output_root.join(format!("{}-events.log", sanitize_run_id(run_id)))
}

pub(super) fn elapsed_secs_since(started_at_millis: u128, updated_at_millis: u128) -> u64 {
    let elapsed_millis = updated_at_millis.saturating_sub(started_at_millis);
    u64::try_from(elapsed_millis / 1_000).unwrap_or(u64::MAX)
}

pub(super) fn encode_run_status_snapshot(snapshot: &RunStatusSnapshot) -> String {
    let mut lines = vec![
        format!("version={JEPSEN_RUN_STATUS_VERSION}"),
        format!("backend={}", snapshot.backend_name),
        format!("run_id={}", snapshot.run_id),
        format!("state={}", snapshot.state.as_str()),
        format!("phase={}", snapshot.phase.as_str()),
        format!("detail={}", encode_tracker_field(&snapshot.detail)),
        format!("started_at_millis={}", snapshot.started_at_millis),
        format!("updated_at_millis={}", snapshot.updated_at_millis),
        format!("elapsed_secs={}", snapshot.elapsed_secs),
        format!(
            "minimum_fault_window_secs={}",
            snapshot
                .minimum_fault_window_secs
                .map_or(String::from("none"), |value| value.to_string())
        ),
        format!("history_events={}", snapshot.history_events),
        format!(
            "history_file={}",
            snapshot
                .history_file
                .as_ref()
                .map_or_else(|| String::from("none"), |path| path.display().to_string())
        ),
        format!(
            "artifact_bundle={}",
            snapshot
                .artifact_bundle
                .as_ref()
                .map_or_else(|| String::from("none"), |path| path.display().to_string())
        ),
        format!(
            "logs_archive={}",
            snapshot
                .logs_archive
                .as_ref()
                .map_or_else(|| String::from("none"), |path| path.display().to_string())
        ),
        format!(
            "release_gate_passed={}",
            snapshot.release_gate_passed.map_or_else(
                || String::from("none"),
                |value| if value {
                    String::from("true")
                } else {
                    String::from("false")
                }
            )
        ),
        format!(
            "blockers={}",
            snapshot
                .blockers
                .map_or_else(|| String::from("none"), |value| value.to_string())
        ),
        format!(
            "last_error={}",
            snapshot
                .last_error
                .as_ref()
                .map_or_else(|| String::from("none"), |value| encode_tracker_field(value))
        ),
    ];
    lines.push(String::new());
    lines.join("\n")
}

pub(super) fn load_run_status_snapshot(path: &Path) -> Result<RunStatusSnapshot, String> {
    let mut bytes = String::new();
    File::open(path)
        .map_err(|error| format!("failed to open run status {}: {error}", path.display()))?
        .read_to_string(&mut bytes)
        .map_err(|error| format!("failed to read run status {}: {error}", path.display()))?;
    decode_run_status_snapshot(&bytes)
}

pub(super) fn maybe_load_run_status_snapshot(
    path: &Path,
) -> Result<Option<RunStatusSnapshot>, String> {
    match File::open(path) {
        Ok(mut file) => {
            let mut bytes = String::new();
            file.read_to_string(&mut bytes).map_err(|error| {
                format!("failed to read run status {}: {error}", path.display())
            })?;
            decode_run_status_snapshot(&bytes).map(Some)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!(
            "failed to open run status {}: {error}",
            path.display()
        )),
    }
}

pub(super) fn decode_run_status_snapshot(bytes: &str) -> Result<RunStatusSnapshot, String> {
    let mut fields = BTreeMap::new();
    for line in bytes.lines().filter(|line| !line.trim().is_empty()) {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("invalid run status line `{line}`"))?;
        fields.insert(String::from(key), String::from(value));
    }

    let version = parse_required_u32(&fields, "version")?;
    if version != JEPSEN_RUN_STATUS_VERSION {
        return Err(format!("unsupported run status version `{version}`"));
    }

    Ok(RunStatusSnapshot {
        backend_name: required_field(&fields, "backend")?.to_owned(),
        run_id: required_field(&fields, "run_id")?.to_owned(),
        state: RunTrackerState::parse(required_field(&fields, "state")?)?,
        phase: RunTrackerPhase::parse(required_field(&fields, "phase")?)?,
        detail: decode_tracker_field(required_field(&fields, "detail")?),
        started_at_millis: parse_required_u128(&fields, "started_at_millis")?,
        updated_at_millis: parse_required_u128(&fields, "updated_at_millis")?,
        elapsed_secs: parse_required_u64(&fields, "elapsed_secs")?,
        minimum_fault_window_secs: parse_optional_u64(required_field(
            &fields,
            "minimum_fault_window_secs",
        )?)?,
        history_events: parse_required_u64(&fields, "history_events")?,
        history_file: parse_optional_path(required_field(&fields, "history_file")?),
        artifact_bundle: parse_optional_path(required_field(&fields, "artifact_bundle")?),
        logs_archive: parse_optional_path(required_field(&fields, "logs_archive")?),
        release_gate_passed: parse_optional_bool(required_field(&fields, "release_gate_passed")?)?,
        blockers: parse_optional_usize(required_field(&fields, "blockers")?)?,
        last_error: parse_optional_string(required_field(&fields, "last_error")?)
            .map(|value| decode_tracker_field(&value)),
    })
}

pub(super) fn encode_tracker_field(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\n', "\\n")
}

pub(super) fn decode_tracker_field(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => output.push('\n'),
                Some(other) => {
                    if other == '\\' {
                        output.push('\\');
                    } else {
                        output.push(other);
                    }
                }
                None => output.push('\\'),
            }
        } else {
            output.push(ch);
        }
    }
    output
}
