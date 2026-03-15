use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use allocdb_node::jepsen::{
    JepsenHistoryEvent, JepsenNemesisFamily, JepsenRunSpec, analyze_history,
    create_artifact_bundle, load_history, persist_history, release_gate_plan,
    render_analysis_report,
};

use super::kubevirt::{load_kubevirt_layout, prepare_kubevirt_helper};
use super::tracker::{RunTracker, RunTrackerPhase};
use super::{
    ExternalTestbed, REMOTE_CONTROL_SCRIPT_PATH, RunExecutionContext, execute_control_run,
    execute_crash_restart_run, execute_mixed_failover_run, execute_partition_heal_run,
    load_qemu_layout, sanitize_run_id,
};
use crate::cluster::ensure_runtime_cluster_ready;

pub(super) fn run_qemu(
    workspace_root: &Path,
    run_id: &str,
    output_root: &Path,
) -> Result<(), String> {
    let layout = load_qemu_layout(workspace_root)?;
    run_external(&layout, run_id, output_root)
}

pub(super) fn run_kubevirt(
    workspace_root: &Path,
    run_id: &str,
    output_root: &Path,
) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    run_external(&layout, run_id, output_root)
}

pub(super) fn archive_qemu_run(
    workspace_root: &Path,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let layout = load_qemu_layout(workspace_root)?;
    archive_external_run(&layout, run_id, history_file, output_root)
}

pub(super) fn archive_kubevirt_run(
    workspace_root: &Path,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    archive_external_run(&layout, run_id, history_file, output_root)
}

pub(super) fn resolve_run_spec(run_id: &str) -> Result<JepsenRunSpec, String> {
    release_gate_plan()
        .into_iter()
        .find(|candidate| candidate.run_id == run_id)
        .ok_or_else(|| format!("unknown Jepsen run id `{run_id}`"))
}

pub(super) fn enforce_minimum_fault_window(
    run_spec: &JepsenRunSpec,
    elapsed: Duration,
) -> Result<(), String> {
    let Some(minimum) = minimum_fault_window_duration(run_spec) else {
        return Ok(());
    };
    if elapsed >= minimum {
        Ok(())
    } else {
        Err(format!(
            "run `{}` finished after {}s, below required fault window {}s",
            run_spec.run_id,
            elapsed.as_secs(),
            minimum.as_secs()
        ))
    }
}

pub(super) fn minimum_fault_window_duration(run_spec: &JepsenRunSpec) -> Option<Duration> {
    effective_minimum_fault_window_secs(run_spec).map(Duration::from_secs)
}

pub(super) fn fault_window_complete(
    minimum_fault_window: Option<Duration>,
    elapsed: Duration,
) -> bool {
    minimum_fault_window.is_none_or(|minimum| elapsed >= minimum)
}

pub(super) fn render_fault_window_iteration_event(
    run_spec: &JepsenRunSpec,
    iteration: u64,
    elapsed: Duration,
    history_events: u64,
) -> String {
    match minimum_fault_window_duration(run_spec) {
        Some(minimum) => format!(
            "fault window iteration={iteration} elapsed={}s remaining={}s history_events={history_events}",
            elapsed.as_secs(),
            minimum.saturating_sub(elapsed).as_secs()
        ),
        None => format!(
            "control iteration={iteration} elapsed={}s history_events={history_events}",
            elapsed.as_secs()
        ),
    }
}

pub(super) fn effective_minimum_fault_window_secs(run_spec: &JepsenRunSpec) -> Option<u64> {
    run_spec.minimum_fault_window_secs?;
    Some(effective_minimum_fault_window_secs_with_override(
        run_spec.minimum_fault_window_secs,
        debug_fault_window_override_secs(),
    ))
}

fn effective_minimum_fault_window_secs_with_override(
    minimum_fault_window_secs: Option<u64>,
    override_secs: Option<u64>,
) -> u64 {
    override_secs.unwrap_or_else(|| {
        minimum_fault_window_secs.expect("fault-window override helper requires a faulted run")
    })
}

fn debug_fault_window_override_secs() -> Option<u64> {
    let raw = std::env::var(super::FAULT_WINDOW_OVERRIDE_ENV).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<u64>().ok().filter(|secs| *secs > 0)
}

fn run_external<T: ExternalTestbed>(
    layout: &T,
    run_id: &str,
    output_root: &Path,
) -> Result<(), String> {
    let run_spec = resolve_run_spec(run_id)?;
    let tracker = RunTracker::new(layout.backend_name(), &run_spec, output_root)?;
    let started_at = Instant::now();
    tracker.set_phase(
        RunTrackerPhase::VerifyingSurface,
        "verifying external surface",
    )?;
    if let Err(error) = super::verify_external_surface(layout) {
        let _ = tracker.fail(RunTrackerPhase::VerifyingSurface, &error);
        return Err(error);
    }
    tracker.set_phase(RunTrackerPhase::Executing, "executing Jepsen scenario")?;
    let history = match execute_external_run(layout, &run_spec, &tracker) {
        Ok(history) => history,
        Err(error) => {
            let _ = tracker.fail(RunTrackerPhase::Executing, &error);
            return Err(error);
        }
    };
    if let Err(error) = fs::create_dir_all(output_root).map_err(|error| {
        format!(
            "failed to create output root {}: {error}",
            output_root.display()
        )
    }) {
        let _ = tracker.fail(RunTrackerPhase::Executing, &error);
        return Err(error);
    }
    let history_path = output_root.join(format!("{}-history.txt", sanitize_run_id(run_id)));
    if let Err(error) = persist_history(&history_path, &history)
        .map_err(|error| format!("failed to persist Jepsen history: {error}"))
    {
        let _ = tracker.fail(RunTrackerPhase::Executing, &error);
        return Err(error);
    }

    tracker.set_phase(RunTrackerPhase::Analyzing, "analyzing Jepsen history")?;
    let report = analyze_history(&history);
    tracker.set_phase(RunTrackerPhase::Archiving, "collecting logs and artifacts")?;
    let logs_archive = match fetch_external_logs_archive(layout, run_id, output_root) {
        Ok(path) => path,
        Err(error) => {
            let _ = tracker.fail(RunTrackerPhase::Archiving, &error);
            return Err(error);
        }
    };
    let bundle_dir = create_artifact_bundle(
        output_root,
        &run_spec,
        &history,
        &report,
        Some(&logs_archive),
    )
    .map_err(|error| format!("failed to create Jepsen artifact bundle: {error}"));
    let bundle_dir = match bundle_dir {
        Ok(path) => path,
        Err(error) => {
            let _ = tracker.fail(RunTrackerPhase::Archiving, &error);
            return Err(error);
        }
    };

    println!("history_file={}", history_path.display());
    println!("artifact_bundle={}", bundle_dir.display());
    println!(
        "{}_logs_archive={}",
        layout.backend_name(),
        logs_archive.display()
    );
    print!("{}", render_analysis_report(&report));
    if let Err(error) = enforce_minimum_fault_window(&run_spec, started_at.elapsed()) {
        let _ = tracker.fail(RunTrackerPhase::Completed, &error);
        return Err(error);
    }
    tracker.complete(&history_path, &bundle_dir, &logs_archive, &report)?;

    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

fn execute_external_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    tracker: &RunTracker,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let started_at = Instant::now();
    let minimum_fault_window = minimum_fault_window_duration(run_spec);
    let mut all_events = Vec::new();
    let mut next_sequence = 0_u64;
    let mut iteration = 0_u64;

    loop {
        iteration = iteration.saturating_add(1);
        if !matches!(run_spec.nemesis, JepsenNemesisFamily::None) {
            ensure_runtime_cluster_ready(layout)?;
        }
        // Surface verification already performed one committed write. Start each scenario slice
        // with a fresh request namespace after that probe so later iterations cannot rewind slots.
        let context = RunExecutionContext::new(tracker.clone(), next_sequence);
        let mut iteration_events = match run_spec.nemesis {
            JepsenNemesisFamily::None => execute_control_run(layout, run_spec, &context),
            JepsenNemesisFamily::CrashRestart => {
                execute_crash_restart_run(layout, run_spec, &context)
            }
            JepsenNemesisFamily::PartitionHeal => {
                execute_partition_heal_run(layout, run_spec, &context)
            }
            JepsenNemesisFamily::MixedFailover => {
                execute_mixed_failover_run(layout, run_spec, &context)
            }
        }?;
        if iteration_events.is_empty() {
            return Err(format!(
                "run `{}` iteration {} produced no Jepsen history events",
                run_spec.run_id, iteration
            ));
        }
        next_sequence = iteration_events
            .last()
            .map_or(next_sequence, |event| event.sequence);
        all_events.append(&mut iteration_events);

        let elapsed = started_at.elapsed();
        let _ = tracker.append_event(&render_fault_window_iteration_event(
            run_spec,
            iteration,
            elapsed,
            next_sequence,
        ));
        if fault_window_complete(minimum_fault_window, elapsed) {
            break;
        }
    }

    Ok(all_events)
}

fn archive_external_run<T: ExternalTestbed>(
    layout: &T,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let run_spec = resolve_run_spec(run_id)?;
    let history =
        load_history(history_file).map_err(|error| format!("failed to load history: {error}"))?;
    let report = analyze_history(&history);
    let logs_archive = fetch_external_logs_archive(layout, run_id, output_root)?;
    let bundle_dir = create_artifact_bundle(
        output_root,
        &run_spec,
        &history,
        &report,
        Some(&logs_archive),
    )
    .map_err(|error| format!("failed to create Jepsen artifact bundle: {error}"))?;
    println!("artifact_bundle={}", bundle_dir.display());
    println!(
        "{}_logs_archive={}",
        layout.backend_name(),
        logs_archive.display()
    );
    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

fn fetch_external_logs_archive<T: ExternalTestbed>(
    layout: &T,
    run_id: &str,
    output_root: &Path,
) -> Result<PathBuf, String> {
    fs::create_dir_all(output_root).map_err(|error| {
        format!(
            "failed to create output root {}: {error}",
            output_root.display()
        )
    })?;
    let sanitized_run_id = sanitize_run_id(run_id);
    let created_at_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let remote_dir = format!(
        "/var/lib/allocdb-qemu/log-bundles/allocdb-jepsen-{sanitized_run_id}-{created_at_millis}"
    );
    let remote_parent = Path::new(&remote_dir)
        .parent()
        .and_then(Path::to_str)
        .ok_or_else(|| format!("remote log bundle path {remote_dir} has no parent"))?;
    let remote_name = Path::new(&remote_dir)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("remote log bundle path {remote_dir} has no file name"))?;
    let archive_path = output_root.join(format!(
        "{}-{}-logs.tar.gz",
        sanitized_run_id,
        layout.backend_name()
    ));

    let output = layout.run_remote_host_command(
        &format!(
            "sudo {REMOTE_CONTROL_SCRIPT_PATH} collect-logs {remote_dir} >/dev/null && sudo tar czf - -C {remote_parent} {remote_name}"
        ),
        None,
    )?;

    let mut file = File::create(&archive_path)
        .map_err(|error| format!("failed to create {}: {error}", archive_path.display()))?;
    file.write_all(&output)
        .map_err(|error| format!("failed to write {}: {error}", archive_path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", archive_path.display()))?;
    Ok(archive_path)
}

#[cfg(test)]
mod tests {
    use super::{
        effective_minimum_fault_window_secs, effective_minimum_fault_window_secs_with_override,
    };
    use allocdb_node::jepsen::{JepsenNemesisFamily, JepsenRunSpec, JepsenWorkloadFamily};

    #[test]
    fn fault_window_override_does_not_change_control_runs() {
        let run_spec = JepsenRunSpec {
            run_id: String::from("reservation_contention-control"),
            workload: JepsenWorkloadFamily::ReservationContention,
            nemesis: JepsenNemesisFamily::None,
            minimum_fault_window_secs: None,
            release_blocking: true,
        };
        assert_eq!(effective_minimum_fault_window_secs(&run_spec), None);
    }

    #[test]
    fn fault_window_override_changes_faulted_runs() {
        assert_eq!(
            effective_minimum_fault_window_secs_with_override(Some(1_800), Some(180)),
            180
        );
    }
}
