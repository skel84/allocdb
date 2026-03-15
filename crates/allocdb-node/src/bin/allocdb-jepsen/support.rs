use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command as AllocCommand};
use allocdb_core::ids::{OperationId, Slot};
use allocdb_node::jepsen::{JepsenEventOutcome, JepsenHistoryEvent, JepsenOperation};
use allocdb_node::{ReplicaId, ReplicaPaths};

use crate::remote::sanitize_run_id;
use crate::tracker::{RequestNamespace, RunTracker};
use crate::{
    ExternalTestbed, NEXT_STAGING_WORKSPACE_ID, REMOTE_CONTROL_SCRIPT_PATH, current_time_millis,
};

#[derive(Clone)]
pub(super) struct RunExecutionContext {
    pub(super) namespace: RequestNamespace,
    pub(super) tracker: RunTracker,
    pub(super) history_sequence_start: u64,
}

impl RunExecutionContext {
    pub(super) fn new(tracker: RunTracker, history_sequence_start: u64) -> Self {
        Self {
            namespace: RequestNamespace::new(),
            tracker,
            history_sequence_start,
        }
    }

    pub(super) fn slot(&self, offset: u64) -> Slot {
        self.namespace.slot(offset)
    }

    pub(super) fn client_request(
        &self,
        operation_id: OperationId,
        command: AllocCommand,
    ) -> ClientRequest {
        self.namespace.client_request(operation_id, command)
    }
}

pub(super) struct HistoryBuilder {
    next_sequence: u64,
    events: Vec<JepsenHistoryEvent>,
    tracker: Option<RunTracker>,
}

impl HistoryBuilder {
    pub(super) fn new(tracker: Option<RunTracker>, next_sequence: u64) -> Self {
        Self {
            next_sequence,
            events: Vec::new(),
            tracker,
        }
    }

    pub(super) fn push(
        &mut self,
        process: impl Into<String>,
        operation: JepsenOperation,
        outcome: JepsenEventOutcome,
    ) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push(JepsenHistoryEvent {
            sequence: self.next_sequence,
            process: process.into(),
            time_millis: current_time_millis(),
            operation,
            outcome,
        });
        if let Some(tracker) = &self.tracker {
            if let Err(error) = tracker.set_history_events(self.next_sequence) {
                log::warn!(
                    "failed to update history event count at sequence {}: {}",
                    self.next_sequence,
                    error
                );
            }
            if let Err(error) =
                tracker.append_event(&format!("history sequence={}", self.next_sequence))
            {
                log::warn!(
                    "failed to append history event at sequence {}: {}",
                    self.next_sequence,
                    error
                );
            }
        }
    }

    pub(super) fn finish(self) -> Vec<JepsenHistoryEvent> {
        self.events
    }
}

pub(super) struct StagedReplicaWorkspace {
    pub(super) root: PathBuf,
    pub(super) replica_id: ReplicaId,
    pub(super) paths: ReplicaPaths,
}

impl StagedReplicaWorkspace {
    pub(super) fn new<T: ExternalTestbed>(
        layout: &T,
        replica_id: ReplicaId,
    ) -> Result<Self, String> {
        let root = temp_staging_dir(&format!(
            "staged-{}-{}-replica-{}",
            layout.backend_name(),
            sanitized_workspace_label(layout.workspace_root()),
            replica_id.get()
        ))?;
        let workspace_dir = root.join(format!("replica-{}", replica_id.get()));
        fs::create_dir_all(&workspace_dir).map_err(|error| {
            format!(
                "failed to create staged workspace {}: {error}",
                workspace_dir.display()
            )
        })?;
        let paths = ReplicaPaths::new(
            workspace_dir.join("replica.metadata"),
            workspace_dir.join("state.snapshot"),
            workspace_dir.join("state.wal"),
        );
        Ok(Self {
            root,
            replica_id,
            paths,
        })
    }

    pub(super) fn from_export<T: ExternalTestbed>(
        layout: &T,
        replica_id: ReplicaId,
    ) -> Result<Self, String> {
        let staged = Self::new(layout, replica_id)?;
        let archive = run_remote_control_command(
            layout,
            &[String::from("export-replica"), replica_id.get().to_string()],
            None,
        )?;
        run_local_tar_extract(&staged.root, &archive)?;
        Ok(staged)
    }

    pub(super) fn import_to_remote<T: ExternalTestbed>(&self, layout: &T) -> Result<(), String> {
        let archive =
            run_local_tar_archive(&self.root, &format!("replica-{}", self.replica_id.get()))?;
        let _ = run_remote_control_command(
            layout,
            &[
                String::from("import-replica"),
                self.replica_id.get().to_string(),
            ],
            Some(&archive),
        )?;
        Ok(())
    }
}

impl Drop for StagedReplicaWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub(super) fn temp_staging_dir(prefix: &str) -> Result<PathBuf, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pid = std::process::id();
    let sequence = NEXT_STAGING_WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
    let root =
        std::env::temp_dir().join(format!("allocdb-{prefix}-pid{pid}-seq{sequence}-{millis}"));
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create temp staging dir {}: {error}",
            root.display()
        )
    })?;
    Ok(root)
}

pub(super) fn run_remote_control_command<T: ExternalTestbed>(
    layout: &T,
    args: &[String],
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let remote_command = if args.is_empty() {
        format!("sudo {REMOTE_CONTROL_SCRIPT_PATH}")
    } else {
        format!("sudo {REMOTE_CONTROL_SCRIPT_PATH} {}", args.join(" "))
    };
    layout.run_remote_host_command(&remote_command, stdin_bytes)
}

#[cfg(target_os = "macos")]
pub(super) fn disable_local_tar_copyfile_metadata(command: &mut Command) {
    command.env("COPYFILE_DISABLE", "1");
}

#[cfg(not(target_os = "macos"))]
pub(super) fn disable_local_tar_copyfile_metadata(_: &mut Command) {}

pub(super) fn run_local_tar_extract(destination_root: &Path, archive: &[u8]) -> Result<(), String> {
    let mut command = Command::new("tar");
    disable_local_tar_copyfile_metadata(&mut command);
    let mut child = command
        .arg("xzf")
        .arg("-")
        .arg("-C")
        .arg(destination_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to spawn local tar extract: {error}"))?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| String::from("local tar extract stdin was unavailable"))?
        .write_all(archive)
        .map_err(|error| format!("failed to write local tar extract stdin: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("failed to wait for local tar extract: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "failed to extract staged replica archive into {}: status={} stderr={}",
            destination_root.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

pub(super) fn run_local_tar_archive(root: &Path, entry_name: &str) -> Result<Vec<u8>, String> {
    let mut command = Command::new("tar");
    disable_local_tar_copyfile_metadata(&mut command);
    let output = command
        .arg("czf")
        .arg("-")
        .arg("-C")
        .arg(root)
        .arg(entry_name)
        .output()
        .map_err(|error| format!("failed to run local tar archive: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "failed to build staged replica archive from {}: status={} stderr={}",
            root.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

pub(super) fn sanitized_workspace_label(path: &Path) -> String {
    sanitize_run_id(
        path.file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("workspace"),
    )
}

pub(super) fn prepare_log_path_for(metadata_path: &Path) -> PathBuf {
    let mut path = metadata_path.to_path_buf();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(
            || String::from("prepare"),
            |value| format!("{value}.prepare"),
        );
    path.set_extension(extension);
    path
}

pub(super) fn copy_file_or_remove(source: &Path, destination: &Path) -> Result<(), String> {
    match fs::copy(source, destination) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if destination.exists() {
                fs::remove_file(destination).map_err(|remove_error| {
                    format!(
                        "failed to remove stale copied file {}: {remove_error}",
                        destination.display()
                    )
                })?;
            }
            Ok(())
        }
        Err(error) => Err(format!(
            "failed to copy {} -> {}: {error}",
            source.display(),
            destination.display()
        )),
    }
}
