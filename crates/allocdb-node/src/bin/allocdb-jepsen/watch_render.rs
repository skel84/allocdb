use std::io::{IsTerminal, Write};
use std::path::Path;

use allocdb_node::ReplicaRole;
use allocdb_node::local_cluster::ReplicaRuntimeState;

use super::tracker::{RunStatusSnapshot, RunTrackerPhase, RunTrackerState, WatchEvent};
use super::watch::{KubevirtWatchLaneSnapshot, ReplicaWatchSnapshot};
use super::{
    ANSI_BLUE, ANSI_BOLD, ANSI_CYAN, ANSI_DIM, ANSI_GREEN, ANSI_MAGENTA, ANSI_ORANGE, ANSI_RED,
    ANSI_RESET, ANSI_WHITE, ANSI_YELLOW, WATCH_PULSE_FRAMES, WATCH_RULE_WIDTH,
    WATCH_SPINNER_FRAMES, current_time_millis,
};

pub(super) fn render_kubevirt_watch(
    status_path: &Path,
    snapshot: Option<&RunStatusSnapshot>,
    replicas: &[ReplicaWatchSnapshot],
    recent_events: &[WatchEvent],
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let color = watch_color_enabled();
    let spinner = watch_spinner_frame(snapshot, refresh_millis);
    let pulse = watch_pulse_frame(snapshot);
    print!("\x1B[2J\x1B[H");
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_CYAN],
            &format!("{spinner} AllocDB Jepsen KubeVirt Watch {pulse}"),
        )
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            &format!(
                "status_file={}   refresh={}ms   mode={}",
                status_path.display(),
                refresh_millis,
                if follow { "follow" } else { "one-shot" }
            ),
        )
    );
    println!("{}", watch_rule(color));
    render_run_summary(snapshot, color);
    println!();
    render_replica_watch_rows(replicas, color);
    println!();
    render_recent_events(snapshot, recent_events, color);
    std::io::stdout()
        .flush()
        .map_err(|error| format!("failed to flush watcher output: {error}"))
}

pub(super) fn render_kubevirt_fleet_watch(
    lanes: &[KubevirtWatchLaneSnapshot],
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let color = watch_color_enabled();
    let anchor = lanes
        .iter()
        .find_map(|lane| {
            lane.snapshot
                .as_ref()
                .filter(|snapshot| snapshot.state == RunTrackerState::Running)
        })
        .or_else(|| lanes.iter().find_map(|lane| lane.snapshot.as_ref()));
    let spinner = watch_spinner_frame(anchor, refresh_millis);
    let pulse = watch_pulse_frame(anchor);
    print!("\x1B[2J\x1B[H");
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_CYAN],
            &format!("{spinner} AllocDB Jepsen KubeVirt Fleet Watch {pulse}"),
        )
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            &format!(
                "lanes={}   refresh={}ms   mode={}   use one output root per lane",
                lanes.len(),
                refresh_millis,
                if follow { "follow" } else { "one-shot" }
            ),
        )
    );
    println!("{}", watch_rule(color));
    render_kubevirt_fleet_lane_rows(lanes, color);
    println!();
    render_kubevirt_fleet_replica_rows(lanes, color);
    println!();
    render_kubevirt_fleet_recent_events(lanes, color);
    std::io::stdout()
        .flush()
        .map_err(|error| format!("failed to flush fleet watcher output: {error}"))
}

pub(super) fn compact_fault_window_progress(snapshot: &RunStatusSnapshot) -> String {
    match snapshot.minimum_fault_window_secs {
        None => String::from("ctrl"),
        Some(total) => {
            let current = snapshot.elapsed_secs.min(total);
            let percent = if total == 0 {
                100
            } else {
                current.saturating_mul(100) / total
            };
            format!("{} {:>3}%", progress_bar(current, total, 8), percent)
        }
    }
}

pub(super) fn parse_watch_event_line(line: &str) -> Result<WatchEvent, String> {
    let (time_prefix, detail) = line
        .split_once(" detail=")
        .ok_or_else(|| format!("invalid watch event line `{line}`"))?;
    let time_millis = time_prefix
        .strip_prefix("time_millis=")
        .ok_or_else(|| format!("invalid watch event timestamp `{line}`"))?
        .parse::<u128>()
        .map_err(|error| format!("invalid watch event timestamp `{line}`: {error}"))?;
    Ok(WatchEvent {
        time_millis,
        detail: String::from(detail),
    })
}

pub(super) fn progress_bar(current: u64, total: u64, width: usize) -> String {
    if total == 0 {
        return "█".repeat(width.max(1));
    }
    let width = width.max(1);
    let clamped = current.min(total);
    let filled = usize::try_from(
        (u128::from(clamped) * u128::try_from(width).unwrap_or(0)) / u128::from(total),
    )
    .unwrap_or(width)
    .min(width);
    format!(
        "{}{}",
        "█".repeat(filled),
        "░".repeat(width.saturating_sub(filled))
    )
}

pub(super) fn compact_counter(value: u64) -> String {
    match value {
        0..=999 => value.to_string(),
        1_000..=999_999 => format_compact_decimal(value, 1_000, "K"),
        1_000_000..=999_999_999 => format_compact_decimal(value, 1_000_000, "M"),
        1_000_000_000..=999_999_999_999 => format_compact_decimal(value, 1_000_000_000, "B"),
        _ => format_compact_decimal(value, 1_000_000_000_000, "T"),
    }
}

fn render_run_summary(snapshot: Option<&RunStatusSnapshot>, color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Run Summary")
    );
    if let Some(snapshot) = snapshot {
        render_summary_row(
            color,
            "Run",
            &watch_style(color, &[ANSI_BOLD, ANSI_WHITE], &snapshot.run_id),
        );
        render_summary_row(color, "State", &run_state_badge(snapshot, color));
        render_summary_row(color, "Phase", &run_phase_badge(snapshot.phase, color));
        render_summary_row(
            color,
            "Elapsed",
            &format_human_duration(snapshot.elapsed_secs),
        );
        render_summary_row(
            color,
            "Window",
            &format_fault_window_summary(snapshot, color),
        );
        render_summary_row(
            color,
            "History",
            &format!(
                "📜 {} {}",
                compact_counter(snapshot.history_events),
                pluralize(snapshot.history_events, "event", "events")
            ),
        );
        render_summary_row(color, "Gate", &format_release_gate_status(snapshot, color));
        render_summary_row(
            color,
            "Blockers",
            &format_blocker_count(snapshot.blockers, color),
        );
        render_summary_row(color, "Detail", &snapshot.detail);
        if let Some(error) = &snapshot.last_error {
            render_summary_row(
                color,
                "Last Error",
                &watch_style(
                    color,
                    &[ANSI_RED],
                    &truncate_for_watch(error, WATCH_RULE_WIDTH.saturating_sub(18)),
                ),
            );
        }
    } else {
        render_summary_row(
            color,
            "Run",
            &watch_style(
                color,
                &[ANSI_YELLOW],
                "💤 waiting for first run status file",
            ),
        );
        render_summary_row(
            color,
            "Hint",
            "start `run-kubevirt` in another terminal and this view will lock on",
        );
    }
}

fn render_replica_watch_rows(replicas: &[ReplicaWatchSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Replica Vibes")
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            "id  health   role       view   commit    wr   queue     lag       due   note"
        )
    );
    for replica in replicas {
        println!("{}", format_replica_watch_row(replica, color));
    }
}

fn render_kubevirt_fleet_lane_rows(lanes: &[KubevirtWatchLaneSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Lane Pulse")
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            "lane  run                              state      phase        elapsed  window             hist   lead  note"
        )
    );
    for (index, lane) in lanes.iter().enumerate() {
        println!("{}", format_kubevirt_fleet_lane_row(index, lane, color));
    }
}

fn format_replica_watch_row(replica: &ReplicaWatchSnapshot, color: bool) -> String {
    let health = replica.status.as_ref().map_or("down", |status| {
        if status.state == ReplicaRuntimeState::Active {
            "up"
        } else {
            "faulted"
        }
    });
    let view = replica
        .status
        .as_ref()
        .map_or(String::from("-"), |status| status.current_view.to_string());
    let commit_lsn = replica.status.as_ref().map_or_else(
        || String::from("-"),
        |status| {
            status
                .commit_lsn
                .map_or_else(|| String::from("-"), |value| compact_counter(value.get()))
        },
    );
    let writes = replica.accepting_writes.map_or(String::from("-"), |value| {
        if value {
            String::from("on")
        } else {
            String::from("off")
        }
    });
    let queue = replica
        .queue_depth
        .map_or_else(|| String::from("-"), compact_counter);
    let lag = replica
        .logical_slot_lag
        .map_or_else(|| String::from("-"), compact_counter);
    let backlog = replica
        .expiration_backlog
        .map_or_else(|| String::from("-"), compact_counter);
    let note = truncate_for_watch(&replica_watch_note(replica), 46);
    let health_cell = pad_watch_cell(health, 8);
    let role_cell = pad_watch_cell(&role_label(replica), 10);
    let view_cell = pad_watch_cell_right(&view, 6);
    let commit_cell = pad_watch_cell_right(&commit_lsn, 9);
    let writes_cell = pad_watch_cell(&writes, 4);
    let queue_cell = pad_watch_cell_right(&queue, 9);
    let lag_cell = pad_watch_cell_right(&lag, 9);
    let backlog_cell = pad_watch_cell_right(&backlog, 5);

    format!(
        "{} {} {} {} {} {} {} {} {} {}",
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_WHITE],
            &pad_watch_cell_right(&replica.replica_id.get().to_string(), 2)
        ),
        style_health_cell(color, health, &health_cell),
        style_role_cell(color, replica, &role_cell),
        watch_style(color, &[ANSI_WHITE], &view_cell),
        watch_style(color, &[ANSI_WHITE], &commit_cell),
        style_writes_cell(color, &writes, &writes_cell),
        watch_style(color, &[ANSI_WHITE], &queue_cell),
        watch_style(color, &[ANSI_WHITE], &lag_cell),
        style_backlog_cell(color, replica.expiration_backlog, &backlog_cell),
        style_note_cell(color, replica, &note)
    )
}

fn format_kubevirt_fleet_lane_row(
    lane_index: usize,
    lane: &KubevirtWatchLaneSnapshot,
    color: bool,
) -> String {
    let lane_cell = pad_watch_cell(&lane.name, 5);
    let run = lane.snapshot.as_ref().map_or_else(
        || String::from("<waiting>"),
        |snapshot| truncate_for_watch(&snapshot.run_id, 32),
    );
    let run_cell = pad_watch_cell(&run, 32);
    let state_cell = lane.snapshot.as_ref().map_or_else(
        || watch_style(color, &[ANSI_DIM], &pad_watch_cell("waiting", 10)),
        |snapshot| run_state_badge(snapshot, color),
    );
    let phase_cell = lane.snapshot.as_ref().map_or_else(
        || watch_style(color, &[ANSI_DIM], &pad_watch_cell("waiting", 12)),
        |snapshot| run_phase_badge(snapshot.phase, color),
    );
    let elapsed_cell = pad_watch_cell_right(
        &lane.snapshot.as_ref().map_or_else(
            || String::from("-"),
            |snapshot| format_human_duration(snapshot.elapsed_secs),
        ),
        7,
    );
    let window_cell = pad_watch_cell(
        &lane
            .snapshot
            .as_ref()
            .map_or_else(|| String::from("warming up"), compact_fault_window_progress),
        18,
    );
    let history_cell = pad_watch_cell_right(
        &lane.snapshot.as_ref().map_or_else(
            || String::from("-"),
            |snapshot| compact_counter(snapshot.history_events),
        ),
        6,
    );
    let lead_cell = pad_watch_cell(&fleet_primary_label(&lane.replicas), 5);
    let note = truncate_for_watch(
        &lane.snapshot.as_ref().map_or_else(
            || {
                lane.lane_error
                    .clone()
                    .unwrap_or_else(|| String::from("waiting for first run status file"))
            },
            |snapshot| {
                snapshot
                    .last_error
                    .clone()
                    .filter(|error| error != "none")
                    .unwrap_or_else(|| snapshot.detail.clone())
            },
        ),
        28,
    );
    let note_cell = pad_watch_cell(&note, 28);

    format!(
        "{} {} {} {} {} {} {} {} {}",
        style_lane_name_cell(color, lane_index, &lane_cell),
        watch_style(color, &[ANSI_WHITE], &run_cell),
        state_cell,
        phase_cell,
        watch_style(color, &[ANSI_WHITE], &elapsed_cell),
        watch_style(color, &[ANSI_WHITE], &window_cell),
        watch_style(color, &[ANSI_WHITE], &history_cell),
        watch_style(color, &[ANSI_WHITE], &lead_cell),
        style_lane_note_cell(color, lane, &note_cell)
    )
}

fn render_kubevirt_fleet_replica_rows(lanes: &[KubevirtWatchLaneSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Replica Vibes")
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            "lane  id  health   role       view   commit    wr   queue     lag       due   note"
        )
    );
    for (index, lane) in lanes.iter().enumerate() {
        for replica in &lane.replicas {
            println!(
                "{}",
                format_kubevirt_fleet_replica_row(index, &lane.name, replica, color)
            );
        }
    }
}

fn format_kubevirt_fleet_replica_row(
    lane_index: usize,
    lane_name: &str,
    replica: &ReplicaWatchSnapshot,
    color: bool,
) -> String {
    let health = replica.status.as_ref().map_or("down", |status| {
        if status.state == ReplicaRuntimeState::Active {
            "up"
        } else {
            "faulted"
        }
    });
    let view = replica
        .status
        .as_ref()
        .map_or(String::from("-"), |status| status.current_view.to_string());
    let commit_lsn = replica.status.as_ref().map_or_else(
        || String::from("-"),
        |status| {
            status
                .commit_lsn
                .map_or_else(|| String::from("-"), |value| compact_counter(value.get()))
        },
    );
    let writes = replica.accepting_writes.map_or(String::from("-"), |value| {
        if value {
            String::from("on")
        } else {
            String::from("off")
        }
    });
    let queue = replica
        .queue_depth
        .map_or_else(|| String::from("-"), compact_counter);
    let lag = replica
        .logical_slot_lag
        .map_or_else(|| String::from("-"), compact_counter);
    let backlog = replica
        .expiration_backlog
        .map_or_else(|| String::from("-"), compact_counter);
    let note = truncate_for_watch(&replica_watch_note(replica), 40);
    let health_cell = pad_watch_cell(health, 8);
    let role_cell = pad_watch_cell(&role_label(replica), 10);
    let view_cell = pad_watch_cell_right(&view, 6);
    let commit_cell = pad_watch_cell_right(&commit_lsn, 9);
    let writes_cell = pad_watch_cell(&writes, 4);
    let queue_cell = pad_watch_cell_right(&queue, 9);
    let lag_cell = pad_watch_cell_right(&lag, 9);
    let backlog_cell = pad_watch_cell_right(&backlog, 5);

    format!(
        "{} {} {} {} {} {} {} {} {} {} {}",
        style_lane_name_cell(color, lane_index, &pad_watch_cell(lane_name, 5)),
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_WHITE],
            &pad_watch_cell_right(&replica.replica_id.get().to_string(), 2)
        ),
        style_health_cell(color, health, &health_cell),
        style_role_cell(color, replica, &role_cell),
        watch_style(color, &[ANSI_WHITE], &view_cell),
        watch_style(color, &[ANSI_WHITE], &commit_cell),
        style_writes_cell(color, &writes, &writes_cell),
        watch_style(color, &[ANSI_WHITE], &queue_cell),
        watch_style(color, &[ANSI_WHITE], &lag_cell),
        style_backlog_cell(color, replica.expiration_backlog, &backlog_cell),
        style_note_cell(color, replica, &note)
    )
}

fn render_recent_events(
    snapshot: Option<&RunStatusSnapshot>,
    recent_events: &[WatchEvent],
    color: bool,
) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Recent Events")
    );
    if recent_events.is_empty() {
        println!("{}", watch_style(color, &[ANSI_DIM], "  <none yet>"));
        return;
    }
    let base_time = snapshot
        .map(|snapshot| snapshot.started_at_millis)
        .or_else(|| recent_events.first().map(|event| event.time_millis))
        .unwrap_or(0);
    for event in recent_events {
        let offset = event.time_millis.saturating_sub(base_time);
        let icon = event_icon(&event.detail);
        let elapsed = format_relative_millis(offset);
        println!(
            "  {} {} {}",
            icon,
            watch_style(color, &[ANSI_DIM], &pad_watch_cell(&elapsed, 8)),
            style_event_detail(color, &event.detail)
        );
    }
}

fn render_kubevirt_fleet_recent_events(lanes: &[KubevirtWatchLaneSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Lane Feed")
    );
    for (index, lane) in lanes.iter().enumerate() {
        println!(
            "  {} {}",
            style_lane_name_cell(color, index, &pad_watch_cell(&lane.name, 5)),
            watch_style(
                color,
                &[ANSI_DIM],
                lane.snapshot
                    .as_ref()
                    .map_or("<waiting>", |snapshot| snapshot.run_id.as_str())
            )
        );
        if lane.recent_events.is_empty() {
            println!("{}", watch_style(color, &[ANSI_DIM], "    <none yet>"));
            continue;
        }
        let base_time = lane
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.started_at_millis)
            .or_else(|| lane.recent_events.first().map(|event| event.time_millis))
            .unwrap_or(0);
        for event in &lane.recent_events {
            let offset = event.time_millis.saturating_sub(base_time);
            let icon = event_icon(&event.detail);
            let elapsed = format_relative_millis(offset);
            println!(
                "    {} {} {}",
                icon,
                watch_style(color, &[ANSI_DIM], &pad_watch_cell(&elapsed, 8)),
                style_event_detail(color, &truncate_for_watch(&event.detail, 72))
            );
        }
    }
}

fn fleet_primary_label(replicas: &[ReplicaWatchSnapshot]) -> String {
    replicas
        .iter()
        .find(|replica| {
            replica.status.as_ref().is_some_and(|status| {
                status.state == ReplicaRuntimeState::Active && status.role == ReplicaRole::Primary
            })
        })
        .map_or_else(
            || String::from("-"),
            |replica| format!("r{}", replica.replica_id.get()),
        )
}

fn lane_style_codes(lane_index: usize) -> &'static [&'static str] {
    match lane_index % 3 {
        0 => &[ANSI_BOLD, ANSI_MAGENTA],
        1 => &[ANSI_BOLD, ANSI_CYAN],
        _ => &[ANSI_BOLD, ANSI_GREEN],
    }
}

fn style_lane_name_cell(color: bool, lane_index: usize, cell: &str) -> String {
    watch_style(color, lane_style_codes(lane_index), cell)
}

fn style_lane_note_cell(color: bool, lane: &KubevirtWatchLaneSnapshot, cell: &str) -> String {
    match lane.snapshot.as_ref() {
        Some(snapshot)
            if snapshot
                .last_error
                .as_ref()
                .is_some_and(|error| error != "none") =>
        {
            watch_style(color, &[ANSI_RED], cell)
        }
        Some(snapshot) if snapshot.state == RunTrackerState::Passed => {
            watch_style(color, &[ANSI_GREEN], cell)
        }
        Some(snapshot) if snapshot.state == RunTrackerState::Failed => {
            watch_style(color, &[ANSI_RED], cell)
        }
        None if lane.lane_error.is_some() => watch_style(color, &[ANSI_ORANGE], cell),
        Some(_) | None => watch_style(color, &[ANSI_DIM], cell),
    }
}

fn watch_color_enabled() -> bool {
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
}

fn watch_style(color: bool, codes: &[&str], text: &str) -> String {
    if !color {
        return String::from(text);
    }
    let mut styled = String::new();
    for code in codes {
        styled.push_str(code);
    }
    styled.push_str(text);
    styled.push_str(ANSI_RESET);
    styled
}

fn watch_rule(color: bool) -> String {
    watch_style(color, &[ANSI_DIM], &"━".repeat(WATCH_RULE_WIDTH))
}

fn watch_spinner_frame(snapshot: Option<&RunStatusSnapshot>, refresh_millis: u64) -> &'static str {
    let frame_seed = snapshot
        .map_or_else(current_time_millis, |snapshot| snapshot.updated_at_millis)
        / u128::from((refresh_millis.max(250) / 2).max(1));
    WATCH_SPINNER_FRAMES[(usize::try_from(frame_seed).unwrap_or(0)) % WATCH_SPINNER_FRAMES.len()]
}

fn watch_pulse_frame(snapshot: Option<&RunStatusSnapshot>) -> &'static str {
    let pulse_seed = snapshot.map_or(0, |snapshot| {
        usize::try_from(snapshot.elapsed_secs).unwrap_or(usize::MAX)
    });
    WATCH_PULSE_FRAMES[pulse_seed % WATCH_PULSE_FRAMES.len()]
}

fn render_summary_row(color: bool, label: &str, value: &str) {
    let label = pad_watch_cell(label, 10);
    println!("  {} {}", watch_style(color, &[ANSI_DIM], &label), value);
}

fn run_state_badge(snapshot: &RunStatusSnapshot, color: bool) -> String {
    match snapshot.state {
        RunTrackerState::Running => watch_style(color, &[ANSI_BOLD, ANSI_GREEN], "🟢 RUNNING"),
        RunTrackerState::Passed => watch_style(color, &[ANSI_BOLD, ANSI_GREEN], "✅ PASSED"),
        RunTrackerState::Failed => watch_style(color, &[ANSI_BOLD, ANSI_RED], "💥 FAILED"),
    }
}

fn run_phase_badge(phase: RunTrackerPhase, color: bool) -> String {
    let (icon, label, codes): (&str, &str, &[&str]) = match phase {
        RunTrackerPhase::Waiting => ("💤", "waiting", &[ANSI_DIM]),
        RunTrackerPhase::VerifyingSurface => ("🛰", "verifying", &[ANSI_BOLD, ANSI_CYAN]),
        RunTrackerPhase::Executing => ("⚙", "executing", &[ANSI_BOLD, ANSI_YELLOW]),
        RunTrackerPhase::Analyzing => ("🔎", "analyzing", &[ANSI_BOLD, ANSI_MAGENTA]),
        RunTrackerPhase::Archiving => ("📦", "archiving", &[ANSI_BOLD, ANSI_BLUE]),
        RunTrackerPhase::Completed => ("🏁", "completed", &[ANSI_BOLD, ANSI_GREEN]),
        RunTrackerPhase::Failed => ("🔥", "failed", &[ANSI_BOLD, ANSI_RED]),
    };
    watch_style(color, codes, &format!("{icon} {label}"))
}

fn format_fault_window_summary(snapshot: &RunStatusSnapshot, color: bool) -> String {
    match snapshot.minimum_fault_window_secs {
        Some(target_secs) => {
            let progress = progress_bar(snapshot.elapsed_secs, target_secs, 18);
            let percent_tenths = fault_window_percent_tenths(snapshot.elapsed_secs, target_secs);
            format!(
                "{} {} / {} ({}.{:01}%)",
                watch_style(color, &[ANSI_CYAN], &progress),
                format_human_duration(snapshot.elapsed_secs),
                format_human_duration(target_secs),
                percent_tenths / 10,
                percent_tenths % 10,
            )
        }
        None => watch_style(
            color,
            &[ANSI_GREEN],
            "🎮 control run • no minimum fault window",
        ),
    }
}

fn format_release_gate_status(snapshot: &RunStatusSnapshot, color: bool) -> String {
    match snapshot.release_gate_passed {
        Some(true) => watch_style(color, &[ANSI_BOLD, ANSI_GREEN], "🟢 clear"),
        Some(false) => watch_style(color, &[ANSI_BOLD, ANSI_RED], "⛔ blocked"),
        None => watch_style(color, &[ANSI_YELLOW], "🫧 pending"),
    }
}

fn format_blocker_count(blockers: Option<usize>, color: bool) -> String {
    match blockers {
        Some(0) => watch_style(color, &[ANSI_GREEN], "0"),
        Some(value) => watch_style(color, &[ANSI_RED], &value.to_string()),
        None => watch_style(color, &[ANSI_DIM], "none"),
    }
}

fn fault_window_percent_tenths(current: u64, total: u64) -> u64 {
    if total == 0 {
        return 1_000;
    }
    u64::try_from((u128::from(current.min(total)) * 1_000) / u128::from(total)).unwrap_or(1_000)
}

fn format_human_duration(secs: u64) -> String {
    let hours = secs / 3_600;
    let minutes = (secs % 3_600) / 60;
    let seconds = secs % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn format_relative_millis(millis: u128) -> String {
    if millis >= 60_000 {
        let secs = u64::try_from(millis / 1_000).unwrap_or(u64::MAX);
        format!("+{}", format_human_duration(secs))
    } else {
        let tenths = millis / 100;
        let whole = tenths / 10;
        let frac = tenths % 10;
        format!("+{whole}.{frac}s")
    }
}

fn format_compact_decimal(value: u64, divisor: u64, suffix: &str) -> String {
    let whole = value / divisor;
    if whole >= 100 {
        return format!("{whole}{suffix}");
    }
    let tenth = ((value % divisor) * 10) / divisor;
    format!("{whole}.{tenth}{suffix}")
}

fn pluralize<'a>(count: u64, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

fn pad_watch_cell(text: &str, width: usize) -> String {
    format!("{text:<width$}")
}

fn pad_watch_cell_right(text: &str, width: usize) -> String {
    format!("{text:>width$}")
}

fn truncate_for_watch(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return String::from(text);
    }
    let keep = max_chars.saturating_sub(1);
    let prefix = text.chars().take(keep).collect::<String>();
    format!("{prefix}…")
}

fn replica_watch_note(replica: &ReplicaWatchSnapshot) -> String {
    if let Some(error) = &replica.error {
        return error.clone();
    }
    let Some(status) = &replica.status else {
        return String::from("☠ control link unavailable");
    };
    if status.state == ReplicaRuntimeState::Faulted {
        return status.fault_reason.clone().map_or_else(
            || String::from("💥 replica faulted"),
            |reason| format!("💥 {reason}"),
        );
    }
    if replica.accepting_writes == Some(false) {
        return String::from("🧯 writes paused");
    }
    if let Some(backlog) = replica.expiration_backlog.filter(|value| *value > 0) {
        return format!(
            "⏰ {} due expiration {}",
            compact_counter(backlog),
            pluralize(backlog, "tick", "ticks")
        );
    }
    if let Some(queue_depth) = replica.queue_depth.filter(|value| *value > 0) {
        return format!("📥 {} queued ops", compact_counter(queue_depth));
    }
    match status.role {
        ReplicaRole::Primary => String::from("👑 serving traffic"),
        ReplicaRole::Backup => String::from("🛡 hot standby"),
        ReplicaRole::ViewUncertain => String::from("🫥 view uncertain"),
        ReplicaRole::Recovering => String::from("🩹 recovering"),
        ReplicaRole::Faulted => String::from("💥 faulted"),
    }
}

pub(super) fn replica_role_label(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Primary => "primary",
        ReplicaRole::Backup => "backup",
        ReplicaRole::ViewUncertain => "uncertain",
        ReplicaRole::Recovering => "recover",
        ReplicaRole::Faulted => "faulted",
    }
}

fn role_label(replica: &ReplicaWatchSnapshot) -> String {
    match replica.status.as_ref().map(|status| status.role) {
        Some(role) => String::from(replica_role_label(role)),
        None => String::from("down"),
    }
}

fn style_health_cell(color: bool, health: &str, cell: &str) -> String {
    let codes = match health {
        "up" => &[ANSI_GREEN][..],
        "faulted" => &[ANSI_RED][..],
        _ => &[ANSI_ORANGE][..],
    };
    watch_style(color, codes, cell)
}

fn style_role_cell(color: bool, replica: &ReplicaWatchSnapshot, cell: &str) -> String {
    let codes = match replica.status.as_ref().map(|status| status.role) {
        Some(ReplicaRole::Primary) => &[ANSI_BOLD, ANSI_YELLOW][..],
        Some(ReplicaRole::Backup) => &[ANSI_CYAN][..],
        Some(ReplicaRole::ViewUncertain) => &[ANSI_ORANGE][..],
        Some(ReplicaRole::Recovering) => &[ANSI_MAGENTA][..],
        Some(ReplicaRole::Faulted) => &[ANSI_RED][..],
        None => &[ANSI_DIM][..],
    };
    watch_style(color, codes, cell)
}

fn style_writes_cell(color: bool, writes: &str, cell: &str) -> String {
    let codes = match writes {
        "on" => &[ANSI_GREEN][..],
        "off" => &[ANSI_YELLOW][..],
        _ => &[ANSI_DIM][..],
    };
    watch_style(color, codes, cell)
}

fn style_backlog_cell(color: bool, backlog: Option<u64>, cell: &str) -> String {
    let codes = match backlog {
        Some(0) => &[ANSI_GREEN][..],
        Some(_) => &[ANSI_ORANGE][..],
        None => &[ANSI_DIM][..],
    };
    watch_style(color, codes, cell)
}

fn style_note_cell(color: bool, replica: &ReplicaWatchSnapshot, note: &str) -> String {
    if replica.error.is_some() {
        return watch_style(color, &[ANSI_RED], note);
    }
    if replica
        .status
        .as_ref()
        .is_some_and(|status| status.role == ReplicaRole::Primary)
    {
        return watch_style(color, &[ANSI_BOLD, ANSI_YELLOW], note);
    }
    watch_style(color, &[ANSI_DIM], note)
}

fn event_icon(detail: &str) -> &'static str {
    if detail.starts_with("run initialized") {
        "🌱"
    } else if detail.starts_with("verifying external surface") {
        "🛰"
    } else if detail.starts_with("executing Jepsen scenario") {
        "⚙"
    } else if detail.starts_with("history sequence=") {
        "📜"
    } else if detail.starts_with("analyzing Jepsen history") {
        "🔎"
    } else if detail.starts_with("collecting logs and artifacts") {
        "📦"
    } else if detail.starts_with("run completed") {
        "🏁"
    } else if detail.starts_with("error:") {
        "💥"
    } else {
        "•"
    }
}

fn style_event_detail(color: bool, detail: &str) -> String {
    if detail.starts_with("error:") {
        return watch_style(color, &[ANSI_RED], detail);
    }
    if detail.starts_with("run completed") {
        return watch_style(color, &[ANSI_GREEN], detail);
    }
    if detail.starts_with("history sequence=") {
        return watch_style(color, &[ANSI_CYAN], detail);
    }
    watch_style(color, &[ANSI_WHITE], detail)
}
