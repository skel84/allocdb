use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use allocdb_core::ids::Lsn;
use allocdb_node::local_cluster::LocalClusterReplicaConfig;
use allocdb_node::{
    ReplicaId, ReplicaIdentity, ReplicaMetadata, ReplicaMetadataFile, ReplicaNode, ReplicaRole,
};

use crate::runtime::{
    live_runtime_replica_matching, render_runtime_probe_summary, runtime_probe_is_active,
    runtime_replica_probes, summarize_runtime_probes,
};
use crate::support::{
    StagedReplicaWorkspace, copy_file_or_remove, prepare_log_path_for, run_remote_control_command,
};
use crate::watch_render::replica_role_label;
use crate::{
    EXTERNAL_RUNTIME_DISCOVERY_RETRY_DELAY, EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT, ExternalTestbed,
};

pub(super) fn wait_for_runtime_replica_role<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
    role: ReplicaRole,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(
        layout,
        &format!(
            "replica {} as {}",
            replica_id.get(),
            replica_role_label(role)
        ),
        |replica| replica.replica_id == replica_id && replica.role == role,
    )
}

pub(super) fn ensure_runtime_cluster_ready<T: ExternalTestbed>(layout: &T) -> Result<(), String> {
    for replica in &layout.replica_layout().replicas {
        let _ = heal_replica(layout, replica.replica_id);
    }

    let expected_replica_count = layout.replica_layout().replicas.len();
    let started_at = Instant::now();
    loop {
        let probes = runtime_replica_probes(layout);
        let topology = summarize_runtime_probes(&probes);
        if topology.active == expected_replica_count
            && topology.primaries == 1
            && topology.backups == expected_replica_count.saturating_sub(1)
        {
            return Ok(());
        }

        for probe in &probes {
            if !runtime_probe_is_active(probe) {
                let _ = restart_replica(layout, probe.replica.replica_id);
            }
        }

        if started_at.elapsed() >= EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT {
            return Err(format!(
                "{} cluster did not converge to a healthy {}-replica topology within {}s; last_probe={}",
                layout.backend_name(),
                expected_replica_count,
                EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT.as_secs(),
                render_runtime_probe_summary(&probes)
            ));
        }
        thread::sleep(EXTERNAL_RUNTIME_DISCOVERY_RETRY_DELAY);
    }
}

pub(super) fn wait_for_live_runtime_replica<T, F>(
    layout: &T,
    description: &str,
    predicate: F,
) -> Result<LocalClusterReplicaConfig, String>
where
    T: ExternalTestbed,
    F: Fn(&LocalClusterReplicaConfig) -> bool,
{
    let started_at = Instant::now();
    loop {
        let probes = runtime_replica_probes(layout);
        if let Some(replica) = live_runtime_replica_matching(&probes, &predicate) {
            return Ok(replica);
        }
        if started_at.elapsed() >= EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT {
            return Err(format!(
                "{} cluster did not surface {description} within {}s; last_probe={}",
                layout.backend_name(),
                EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT.as_secs(),
                render_runtime_probe_summary(&probes)
            ));
        }
        thread::sleep(EXTERNAL_RUNTIME_DISCOVERY_RETRY_DELAY);
    }
}

pub(super) fn primary_replica<T: ExternalTestbed>(
    layout: &T,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(layout, "a live primary", |replica| {
        replica.role == ReplicaRole::Primary
    })
}

pub(super) fn first_backup_replica<T: ExternalTestbed>(
    layout: &T,
    exclude: Option<ReplicaId>,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(layout, "a live backup", |replica| {
        replica.role == ReplicaRole::Backup
            && exclude.is_none_or(|excluded| replica.replica_id != excluded)
    })
}

pub(super) fn runtime_replica_by_id<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(
        layout,
        &format!("replica {}", replica_id.get()),
        |replica| replica.replica_id == replica_id,
    )
}

pub(super) fn maybe_crash_replica<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=crash_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    match run_remote_control_command(
        layout,
        &[String::from("crash"), replica_id.get().to_string()],
        None,
    ) {
        Ok(_) => Ok(()),
        Err(error) if error.contains("already stopped") => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn restart_replica<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=restart_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    let _ = run_remote_control_command(
        layout,
        &[String::from("restart"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

pub(super) fn isolate_replica<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=isolate_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    let _ = run_remote_control_command(
        layout,
        &[String::from("isolate"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

pub(super) fn heal_replica<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=heal_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    let _ = run_remote_control_command(
        layout,
        &[String::from("heal"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn staged_replica_summary<T: ExternalTestbed>(
    layout: &T,
    staged: &StagedReplicaWorkspace,
) -> Result<(u64, Option<Lsn>, Option<Lsn>), String> {
    let node = ReplicaNode::recover(
        layout.replica_layout().core_config.clone(),
        layout.replica_layout().engine_config,
        ReplicaIdentity {
            replica_id: staged.replica_id,
            shard_id: layout.replica_layout().core_config.shard_id,
        },
        staged.paths.clone(),
    )
    .map_err(|error| {
        format!(
            "failed to recover staged replica {} summary: {error:?}",
            staged.replica_id.get()
        )
    })?;
    Ok((
        node.metadata().current_view,
        node.metadata().commit_lsn,
        node.highest_prepared_lsn(),
    ))
}

fn format_staged_summary(summary: (u64, Option<Lsn>, Option<Lsn>)) -> String {
    format!(
        "view={} commit_lsn={} highest_prepared_lsn={}",
        summary.0,
        summary
            .1
            .map_or_else(|| String::from("none"), |lsn| lsn.get().to_string()),
        summary
            .2
            .map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
    )
}

fn format_optional_lsn(value: Option<Lsn>) -> String {
    value.map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
}

fn load_staged_source_metadata<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
) -> Result<(ReplicaMetadata, PathBuf), String> {
    let source_node = ReplicaNode::recover(
        layout.replica_layout().core_config.clone(),
        layout.replica_layout().engine_config,
        ReplicaIdentity {
            replica_id: source.replica_id,
            shard_id: layout.replica_layout().core_config.shard_id,
        },
        source.paths.clone(),
    )
    .map_err(|error| {
        format!(
            "failed to recover staged source replica {}: {error:?}",
            source.replica_id.get()
        )
    })?;
    let source_metadata = *source_node.metadata();
    let source_prepare_log_path = source_node.prepare_log_path().to_path_buf();
    Ok((source_metadata, source_prepare_log_path))
}

fn log_rewrite_replica_begin<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
    target: &StagedReplicaWorkspace,
    source_metadata: ReplicaMetadata,
    target_commit_lsn: Option<Lsn>,
    new_view: u64,
    new_role: ReplicaRole,
) {
    eprintln!(
        "backend={} event=rewrite_replica_from_source workspace={} source_replica={} target_replica={} source_role={:?} source_view={} source_commit_lsn={} source_snapshot_lsn={} target_commit_lsn={} new_view={} new_role={:?}",
        layout.backend_name(),
        layout.workspace_root().display(),
        source.replica_id.get(),
        target.replica_id.get(),
        source_metadata.role,
        source_metadata.current_view,
        format_optional_lsn(source_metadata.commit_lsn),
        format_optional_lsn(source_metadata.active_snapshot_lsn),
        format_optional_lsn(target_commit_lsn),
        new_view,
        new_role
    );
}

fn log_rewrite_replica_complete<T: ExternalTestbed>(
    layout: &T,
    target: &StagedReplicaWorkspace,
    target_node: &ReplicaNode,
) {
    eprintln!(
        "backend={} event=rewrite_replica_complete workspace={} target_replica={} final_role={:?} final_view={} final_commit_lsn={} final_snapshot_lsn={} final_highest_prepared_lsn={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        target.replica_id.get(),
        target_node.metadata().role,
        target_node.metadata().current_view,
        format_optional_lsn(target_node.metadata().commit_lsn),
        format_optional_lsn(target_node.metadata().active_snapshot_lsn),
        format_optional_lsn(target_node.highest_prepared_lsn())
    );
}

fn rewrite_replica_from_source<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
    target: &StagedReplicaWorkspace,
    target_commit_lsn: Option<Lsn>,
    new_view: u64,
    new_role: ReplicaRole,
) -> Result<(), String> {
    let (source_metadata, source_prepare_log_path) = load_staged_source_metadata(layout, source)?;
    log_rewrite_replica_begin(
        layout,
        source,
        target,
        source_metadata,
        target_commit_lsn,
        new_view,
        new_role,
    );

    if source.replica_id != target.replica_id {
        copy_file_or_remove(&source.paths.snapshot_path, &target.paths.snapshot_path)?;
        copy_file_or_remove(&source.paths.wal_path, &target.paths.wal_path)?;
        copy_file_or_remove(
            &source_prepare_log_path,
            &prepare_log_path_for(&target.paths.metadata_path),
        )?;
    }

    let target_identity = ReplicaIdentity {
        replica_id: target.replica_id,
        shard_id: layout.replica_layout().core_config.shard_id,
    };
    ReplicaMetadataFile::new(&target.paths.metadata_path)
        .write_metadata(&ReplicaMetadata {
            identity: target_identity,
            current_view: source_metadata.current_view,
            role: ReplicaRole::Backup,
            commit_lsn: source_metadata.commit_lsn,
            active_snapshot_lsn: source_metadata.active_snapshot_lsn,
            last_normal_view: source_metadata.last_normal_view,
            durable_vote: None,
        })
        .map_err(|error| {
            format!(
                "failed to write staged metadata for replica {}: {error:?}",
                target.replica_id.get()
            )
        })?;

    let mut target_node = ReplicaNode::recover(
        layout.replica_layout().core_config.clone(),
        layout.replica_layout().engine_config,
        target_identity,
        target.paths.clone(),
    )
    .map_err(|error| {
        format!(
            "failed to recover rewritten staged replica {}: {error:?}",
            target.replica_id.get()
        )
    })?;
    if let Some(target_commit_lsn) = target_commit_lsn {
        if target_node
            .metadata()
            .commit_lsn
            .is_none_or(|commit_lsn| commit_lsn.get() < target_commit_lsn.get())
        {
            target_node
                .enter_view_uncertain()
                .map_err(|error| format!("failed to enter view uncertain: {error:?}"))?;
            target_node
                .reconstruct_committed_prefix_through(target_commit_lsn)
                .map_err(|error| format!("failed to reconstruct committed prefix: {error:?}"))?;
        }
    }
    target_node
        .discard_uncommitted_suffix()
        .map_err(|error| format!("failed to discard staged suffix: {error:?}"))?;
    target_node
        .configure_normal_role(new_view, new_role)
        .map_err(|error| format!("failed to configure staged role: {error:?}"))?;
    log_rewrite_replica_complete(layout, target, &target_node);
    Ok(())
}

pub(super) fn perform_failover<T: ExternalTestbed>(
    layout: &T,
    old_primary: ReplicaId,
    new_primary: ReplicaId,
    supporting_backup: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=perform_failover_begin workspace={} old_primary={} new_primary={} supporting_backup={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        old_primary.get(),
        new_primary.get(),
        supporting_backup.get()
    );
    maybe_crash_replica(layout, old_primary)?;
    maybe_crash_replica(layout, new_primary)?;
    maybe_crash_replica(layout, supporting_backup)?;

    let new_primary_stage = StagedReplicaWorkspace::from_export(layout, new_primary)?;
    let supporting_stage = StagedReplicaWorkspace::from_export(layout, supporting_backup)?;
    let new_primary_summary = staged_replica_summary(layout, &new_primary_stage)?;
    let supporting_summary = staged_replica_summary(layout, &supporting_stage)?;

    let target_commit_lsn = new_primary_summary
        .1
        .max(supporting_summary.1)
        .or_else(|| new_primary_summary.2.max(supporting_summary.2));
    let new_view = new_primary_summary
        .0
        .max(supporting_summary.0)
        .saturating_add(1);

    let source = if supporting_summary.1.unwrap_or(Lsn(0)).get()
        > new_primary_summary.1.unwrap_or(Lsn(0)).get()
        || (supporting_summary.1 == new_primary_summary.1
            && supporting_summary.2.unwrap_or(Lsn(0)).get()
                > new_primary_summary.2.unwrap_or(Lsn(0)).get())
    {
        &supporting_stage
    } else {
        &new_primary_stage
    };
    eprintln!(
        "backend={} event=perform_failover_plan workspace={} old_primary={} new_primary={} supporting_backup={} new_primary_summary=\"{}\" supporting_summary=\"{}\" chosen_source={} target_commit_lsn={} new_view={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        old_primary.get(),
        new_primary.get(),
        supporting_backup.get(),
        format_staged_summary(new_primary_summary),
        format_staged_summary(supporting_summary),
        source.replica_id.get(),
        target_commit_lsn.map_or_else(|| String::from("none"), |lsn| lsn.get().to_string()),
        new_view
    );

    rewrite_replica_from_source(
        layout,
        source,
        &new_primary_stage,
        target_commit_lsn,
        new_view,
        ReplicaRole::Primary,
    )?;
    rewrite_replica_from_source(
        layout,
        source,
        &supporting_stage,
        target_commit_lsn,
        new_view,
        ReplicaRole::Backup,
    )?;

    supporting_stage.import_to_remote(layout)?;
    new_primary_stage.import_to_remote(layout)?;
    restart_replica(layout, supporting_backup)?;
    restart_replica(layout, new_primary)?;
    wait_for_runtime_replica_role(layout, new_primary, ReplicaRole::Primary)?;
    wait_for_runtime_replica_role(layout, supporting_backup, ReplicaRole::Backup)?;
    eprintln!(
        "backend={} event=perform_failover_complete workspace={} old_primary={} new_primary={} supporting_backup={} new_view={} target_commit_lsn={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        old_primary.get(),
        new_primary.get(),
        supporting_backup.get(),
        new_view,
        target_commit_lsn.map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
    );
    Ok(())
}

pub(super) fn perform_rejoin<T: ExternalTestbed>(
    layout: &T,
    current_primary: ReplicaId,
    target_replica: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=perform_rejoin_begin workspace={} current_primary={} target_replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        current_primary.get(),
        target_replica.get()
    );
    maybe_crash_replica(layout, target_replica)?;
    let source_stage = StagedReplicaWorkspace::from_export(layout, current_primary)?;
    let source_summary = staged_replica_summary(layout, &source_stage)?;
    let target_stage = StagedReplicaWorkspace::new(layout, target_replica)?;
    eprintln!(
        "backend={} event=perform_rejoin_plan workspace={} current_primary={} target_replica={} source_summary=\"{}\"",
        layout.backend_name(),
        layout.workspace_root().display(),
        current_primary.get(),
        target_replica.get(),
        format_staged_summary(source_summary)
    );
    rewrite_replica_from_source(
        layout,
        &source_stage,
        &target_stage,
        source_summary.1,
        source_summary.0,
        ReplicaRole::Backup,
    )?;
    target_stage.import_to_remote(layout)?;
    restart_replica(layout, target_replica)?;
    wait_for_runtime_replica_role(layout, target_replica, ReplicaRole::Backup)?;
    eprintln!(
        "backend={} event=perform_rejoin_complete workspace={} current_primary={} target_replica={} target_view={} target_commit_lsn={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        current_primary.get(),
        target_replica.get(),
        source_summary.0,
        source_summary
            .1
            .map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
    );
    Ok(())
}
