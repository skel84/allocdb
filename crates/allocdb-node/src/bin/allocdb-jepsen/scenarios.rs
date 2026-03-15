use allocdb_core::ids::{HolderId, OperationId, ResourceId, Slot};
use allocdb_node::jepsen::{
    JepsenExpiredReservation, JepsenHistoryEvent, JepsenRunSpec, JepsenWorkloadFamily,
};
use allocdb_node::local_cluster::LocalClusterReplicaConfig;

use crate::cluster::{
    first_backup_replica, maybe_crash_replica, perform_failover, perform_rejoin, primary_replica,
    runtime_replica_by_id,
};
use crate::events::{
    ExpirationDrainPlan, ReserveEventSpec, backup_process_name, create_qemu_resource,
    primary_process_name, record_resource_available_after_expiration, reservation_read_event,
    reserve_event, tick_expirations_event,
};
use crate::support::{HistoryBuilder, RunExecutionContext};
use crate::{ExternalTestbed, unique_probe_resource_id};

pub(super) fn execute_control_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let backup = first_backup_replica(layout, Some(primary.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => {
            run_reservation_contention(layout, &primary, base_id, context)
        }
        JepsenWorkloadFamily::AmbiguousWriteRetry => {
            run_ambiguous_write_retry(layout, &primary, base_id, context)
        }
        JepsenWorkloadFamily::FailoverReadFences => {
            run_failover_read_fences(layout, &primary, &backup, base_id, context)
        }
        JepsenWorkloadFamily::ExpirationAndRecovery => {
            run_expiration_and_recovery(layout, &primary, base_id, context)
        }
    }
}

pub(super) fn execute_crash_restart_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let failover_target = first_backup_replica(layout, Some(primary.replica_id))?;
    let supporting_backup = first_backup_replica(layout, Some(failover_target.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => run_crash_restart_reservation_contention(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_crash_restart_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_crash_restart_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_crash_restart_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
    }
}

fn run_crash_restart_reservation_contention<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 100),
    )?;

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 1),
            resource_id,
            holder_id: HolderId(101),
            request_slot: Slot(10),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), first.0, first.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;

    let second = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 2),
            resource_id,
            holder_id: HolderId(202),
            request_slot: Slot(11),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), second.0, second.1);
    Ok(history.finish())
}

fn run_crash_restart_ambiguous_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 1);
    let operation_id = OperationId(base_id + 10);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 101),
    )?;

    maybe_crash_replica(layout, failover_target.replica_id)?;
    maybe_crash_replica(layout, supporting_backup.replica_id)?;
    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let retry = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_crash_restart_failover_reads<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 2);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 102),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 20),
            resource_id,
            holder_id: HolderId(404),
            request_slot: Slot(30),
            ttl_slots: 6,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("crash-restart failover_read_fences expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let primary_read = reservation_read_event(
        layout,
        &current_primary,
        context,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        primary_read.0,
        primary_read.1,
    );

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    let old_primary = runtime_replica_by_id(layout, primary.replica_id)?;
    let stale_read = reservation_read_event(
        layout,
        &old_primary,
        context,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        backup_process_name(&old_primary),
        stale_read.0,
        stale_read.1,
    );
    Ok(history.finish())
}

fn run_crash_restart_expiration_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 3);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 103),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 30),
            resource_id,
            holder_id: HolderId(505),
            request_slot: Slot(40),
            ttl_slots: 2,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("crash-restart expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let tick = tick_expirations_event(
        layout,
        &current_primary,
        context,
        OperationId(base_id + 31),
        Slot(42),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 505,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = tick.2.ok_or_else(|| {
        String::from("crash-restart expiration_and_recovery expected one committed expiration tick")
    })?;
    history.push(primary_process_name(&current_primary), tick.0, tick.1);
    let settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        &current_primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[JepsenExpiredReservation {
                resource_id,
                holder_id: 505,
                reservation_id: reserve_commit.reservation_id.get(),
                released_lsn: None,
            }],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 32,
            slot_base: 43,
        },
    )?;
    let reservation_read = reservation_read_event(
        layout,
        &current_primary,
        context,
        reserve_commit.reservation_id,
        Slot(42),
        Some(settled_tick_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        reservation_read.0,
        reservation_read.1,
    );

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_reservation_contention<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 100),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 1),
            resource_id,
            holder_id: HolderId(101),
            request_slot: Slot(10),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    let second = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 2),
            resource_id,
            holder_id: HolderId(202),
            request_slot: Slot(11),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(primary), second.0, second.1);
    Ok(history.finish())
}

fn run_ambiguous_write_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 1);
    let operation_id = OperationId(base_id + 10);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 101),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    let retry = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), retry.0, retry.1);
    Ok(history.finish())
}

fn run_failover_read_fences<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 2);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 102),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 20),
            resource_id,
            holder_id: HolderId(404),
            request_slot: Slot(30),
            ttl_slots: 6,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("failover_read_fences control run expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    let primary_read = reservation_read_event(
        layout,
        primary,
        context,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(primary),
        primary_read.0,
        primary_read.1,
    );

    let backup_read = reservation_read_event(
        layout,
        backup,
        context,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(backup_process_name(backup), backup_read.0, backup_read.1);
    Ok(history.finish())
}

fn run_expiration_and_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 3);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 103),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 30),
            resource_id,
            holder_id: HolderId(505),
            request_slot: Slot(40),
            ttl_slots: 2,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("expiration_and_recovery control run expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    let tick = tick_expirations_event(
        layout,
        primary,
        context,
        OperationId(base_id + 31),
        Slot(42),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 505,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = tick.2.ok_or_else(|| {
        String::from("expiration_and_recovery control run expected one committed expiration tick")
    })?;
    history.push(primary_process_name(primary), tick.0, tick.1);
    let settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[JepsenExpiredReservation {
                resource_id,
                holder_id: 505,
                reservation_id: reserve_commit.reservation_id.get(),
                released_lsn: None,
            }],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 32,
            slot_base: 43,
        },
    )?;

    let reservation_read = reservation_read_event(
        layout,
        primary,
        context,
        reserve_commit.reservation_id,
        Slot(42),
        Some(settled_tick_lsn),
    )?;
    history.push(
        primary_process_name(primary),
        reservation_read.0,
        reservation_read.1,
    );
    Ok(history.finish())
}
