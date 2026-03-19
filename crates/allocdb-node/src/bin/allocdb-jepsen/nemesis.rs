use allocdb_core::ids::{HolderId, OperationId, ResourceId, Slot};
use allocdb_node::jepsen::{
    JepsenExpiredReservation, JepsenHistoryEvent, JepsenRunSpec, JepsenWorkloadFamily,
};
use allocdb_node::local_cluster::LocalClusterReplicaConfig;

use crate::cluster::{
    first_backup_replica, heal_replica, isolate_replica, maybe_crash_replica, perform_failover,
    perform_rejoin, primary_replica, runtime_replica_by_id,
};
use crate::events::{
    ExpirationDrainPlan, ReserveEventSpec, backup_process_name, create_qemu_resource,
    primary_process_name, record_resource_available_after_expiration, reservation_read_event,
    reserve_event, tick_expirations_event,
};
use crate::support::{HistoryBuilder, RunExecutionContext};
use crate::{ExternalTestbed, unique_probe_resource_id};

pub(super) fn execute_partition_heal_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let failover_target = first_backup_replica(layout, Some(primary.replica_id))?;
    let supporting_backup = first_backup_replica(layout, Some(failover_target.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => run_partition_heal_reservation_contention(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_partition_heal_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_partition_heal_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_partition_heal_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::LeaseSafety => Err(String::from(
            "partition-heal runs are not defined for lease_safety",
        )),
    }
}

pub(super) fn execute_mixed_failover_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let failover_target = first_backup_replica(layout, Some(primary.replica_id))?;
    let supporting_backup = first_backup_replica(layout, Some(failover_target.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => Err(String::from(
            "mixed failover runs are not defined for reservation_contention",
        )),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_mixed_failover_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_mixed_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_mixed_failover_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::LeaseSafety => Err(String::from(
            "mixed failover runs are not defined for lease_safety",
        )),
    }
}

fn run_partition_heal_reservation_contention<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 10);
    let operation_id = OperationId(base_id + 11);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 110),
    )?;
    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

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
            holder_id: HolderId(601),
            request_slot: Slot(50),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);
    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
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
            holder_id: HolderId(601),
            request_slot: Slot(50),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    let second = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 12),
            resource_id,
            holder_id: HolderId(602),
            request_slot: Slot(51),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), second.0, second.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_ambiguous_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 11);
    let operation_id = OperationId(base_id + 20);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 120),
    )?;

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

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
            holder_id: HolderId(603),
            request_slot: Slot(52),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
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
            holder_id: HolderId(603),
            request_slot: Slot(52),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_failover_reads<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 12);
    let minority_operation_id = OperationId(base_id + 22);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 130),
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
            operation_id: OperationId(base_id + 21),
            resource_id,
            holder_id: HolderId(604),
            request_slot: Slot(53),
            ttl_slots: 6,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("partition-heal failover_read_fences expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;
    let minority_write = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: minority_operation_id,
            resource_id,
            holder_id: HolderId(605),
            request_slot: Slot(54),
            ttl_slots: 2,
        },
    )?;
    history.push(
        primary_process_name(primary),
        minority_write.0,
        minority_write.1,
    );

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
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
        Slot(54),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        primary_read.0,
        primary_read.1,
    );

    retry_partition_heal_failover_minority_write(
        layout,
        &current_primary,
        context,
        &mut history,
        minority_operation_id,
        resource_id,
    )?;

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    let old_primary = runtime_replica_by_id(layout, primary.replica_id)?;
    let stale_read = reservation_read_event(
        layout,
        &old_primary,
        context,
        reserve_commit.reservation_id,
        Slot(54),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        backup_process_name(&old_primary),
        stale_read.0,
        stale_read.1,
    );
    Ok(history.finish())
}

fn retry_partition_heal_failover_minority_write<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    history: &mut HistoryBuilder,
    operation_id: OperationId,
    resource_id: ResourceId,
) -> Result<(), String> {
    let retry = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(605),
            request_slot: Slot(54),
            ttl_slots: 2,
        },
    )?;
    history.push(primary_process_name(primary), retry.0, retry.1);
    Ok(())
}

fn run_partition_heal_expiration_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 13);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 140),
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
            operation_id: OperationId(base_id + 23),
            resource_id,
            holder_id: HolderId(606),
            request_slot: Slot(55),
            ttl_slots: 2,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("partition-heal expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );
    let expired_reservation = JepsenExpiredReservation {
        resource_id,
        holder_id: 606,
        reservation_id: reserve_commit.reservation_id.get(),
        released_lsn: None,
    };

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;
    let failed_tick = tick_expirations_event(
        layout,
        primary,
        context,
        OperationId(base_id + 24),
        Slot(57),
        &[expired_reservation],
    )?;
    history.push(primary_process_name(primary), failed_tick.0, failed_tick.1);

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let successful_tick = tick_expirations_event(
        layout,
        &current_primary,
        context,
        OperationId(base_id + 24),
        Slot(57),
        &[expired_reservation],
    )?;
    let tick_lsn = successful_tick.2.ok_or_else(|| {
        String::from(
            "partition-heal expiration_and_recovery expected one committed expiration tick",
        )
    })?;
    history.push(
        primary_process_name(&current_primary),
        successful_tick.0,
        successful_tick.1,
    );
    let _settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        &current_primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[expired_reservation],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 25,
            slot_base: 58,
        },
    )?;
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_mixed_failover_ambiguous_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 20);
    let operation_id = OperationId(base_id + 30);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 150),
    )?;

    maybe_crash_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

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
            holder_id: HolderId(701),
            request_slot: Slot(60),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    maybe_crash_replica(layout, primary.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
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
            holder_id: HolderId(701),
            request_slot: Slot(60),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_mixed_failover_reads<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 21);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 160),
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
            operation_id: OperationId(base_id + 31),
            resource_id,
            holder_id: HolderId(702),
            request_slot: Slot(61),
            ttl_slots: 6,
        },
    )?;
    let reserve_commit = reserve_commit
        .ok_or_else(|| String::from("mixed failover read fences expected one committed reserve"))?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
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
        Slot(61),
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
        Slot(61),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        backup_process_name(&old_primary),
        stale_read.0,
        stale_read.1,
    );
    Ok(history.finish())
}

fn run_mixed_failover_expiration_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 22);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 170),
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
            operation_id: OperationId(base_id + 32),
            resource_id,
            holder_id: HolderId(703),
            request_slot: Slot(62),
            ttl_slots: 2,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("mixed failover expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
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
        OperationId(base_id + 33),
        Slot(64),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 703,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = tick.2.ok_or_else(|| {
        String::from(
            "mixed failover expiration_and_recovery expected one committed expiration tick",
        )
    })?;
    history.push(primary_process_name(&current_primary), tick.0, tick.1);
    let _settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        &current_primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[JepsenExpiredReservation {
                resource_id,
                holder_id: 703,
                reservation_id: reserve_commit.reservation_id.get(),
                released_lsn: None,
            }],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 34,
            slot_base: 65,
        },
    )?;
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}
