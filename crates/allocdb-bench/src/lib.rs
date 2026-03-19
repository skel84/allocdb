use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_core::state_machine::ResourceState;
use allocdb_node::engine::{EngineConfig, EngineOpenError, SingleNodeEngine, SubmissionError};

#[cfg(test)]
mod tests;

const DEFAULT_HOTSPOT_ROUNDS: u32 = 256;
const DEFAULT_HOTSPOT_CONTENDERS: u32 = 32;
const DEFAULT_RETRY_TABLE_CAPACITY: u32 = 128;
const DEFAULT_RETRY_DUPLICATE_FANOUT: u32 = 4;
const DEFAULT_RETRY_FULL_REJECTION_ATTEMPTS: u32 = 32;
const HOTSPOT_TTL_SLOTS: u64 = 4;
const HOTSPOT_RETRY_WINDOW_SLOTS: u64 = 8;
const HOTSPOT_HISTORY_WINDOW_SLOTS: u64 = 4;
const RETRY_TTL_SLOTS: u64 = 4;
const RETRY_WINDOW_SLOTS: u64 = 4;
const RETRY_HISTORY_WINDOW_SLOTS: u64 = 2;
const RETRY_RESOURCE_HEADROOM: u32 = 8;
const DEFAULT_MAX_COMMAND_BYTES: usize = 256;
const DEFAULT_QUEUE_CAPACITY: u32 = 8;
const DEFAULT_EXPIRATIONS_PER_TICK: u32 = 8;
// Keep the benchmark harness safely local. Larger fixed-capacity tables can turn a bad CLI
// invocation into multi-gigabyte allocations before the engine ever reports a useful error.
const MAX_DERIVED_TABLE_CAPACITY: u32 = 65_536;
// Keep retry-pressure runs reviewable and bounded in runtime/WAL churn as well as in memory.
const MAX_RETRY_PRESSURE_TOTAL_OPERATIONS: u64 = 1_000_000;
const BENCH_CLIENT_ID: ClientId = ClientId(7);
const RETRY_CONFLICT_RESOURCE_OFFSET: u128 = 1_000_000;
static NEXT_WORKSPACE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioSelection {
    All,
    OneResourceManyContenders,
    HighRetryPressure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioName {
    OneResourceManyContenders,
    HighRetryPressure,
}

impl ScenarioName {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OneResourceManyContenders => "one-resource-many-contenders",
            Self::HighRetryPressure => "high-retry-pressure",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkOptions {
    pub scenario: ScenarioSelection,
    pub hotspot_rounds: u32,
    pub hotspot_contenders: u32,
    pub retry_table_capacity: u32,
    pub retry_duplicate_fanout: u32,
    pub retry_full_rejection_attempts: u32,
}

impl Default for BenchmarkOptions {
    fn default() -> Self {
        Self {
            scenario: ScenarioSelection::All,
            hotspot_rounds: DEFAULT_HOTSPOT_ROUNDS,
            hotspot_contenders: DEFAULT_HOTSPOT_CONTENDERS,
            retry_table_capacity: DEFAULT_RETRY_TABLE_CAPACITY,
            retry_duplicate_fanout: DEFAULT_RETRY_DUPLICATE_FANOUT,
            retry_full_rejection_attempts: DEFAULT_RETRY_FULL_REJECTION_ATTEMPTS,
        }
    }
}

impl BenchmarkOptions {
    /// Validates that CLI-provided benchmark knobs keep both scenarios meaningful.
    ///
    /// # Errors
    ///
    /// Returns [`BenchmarkError::InvalidOption`] when one configured bound would make a scenario
    /// degenerate.
    pub fn validate(self) -> Result<(), BenchmarkError> {
        if self.hotspot_rounds == 0 {
            return Err(invalid_option(
                "hotspot_rounds",
                "must be greater than zero",
            ));
        }

        if self.hotspot_contenders < 2 {
            return Err(invalid_option(
                "hotspot_contenders",
                "must be at least two contenders per round",
            ));
        }

        if self.retry_table_capacity == 0 {
            return Err(invalid_option(
                "retry_table_capacity",
                "must be greater than zero",
            ));
        }

        if self.retry_duplicate_fanout == 0 {
            return Err(invalid_option(
                "retry_duplicate_fanout",
                "must be greater than zero",
            ));
        }

        if self.retry_full_rejection_attempts == 0 {
            return Err(invalid_option(
                "retry_full_rejection_attempts",
                "must be greater than zero",
            ));
        }

        validate_hotspot_derived_capacities(self)?;
        validate_retry_derived_capacities(self)?;

        Ok(())
    }
}

fn validate_hotspot_derived_capacities(options: BenchmarkOptions) -> Result<(), BenchmarkError> {
    let reservation_capacity = checked_hotspot_reservation_capacity(options.hotspot_rounds)
        .ok_or_else(|| {
            invalid_option(
                "hotspot_rounds",
                "overflows the derived reservation-table capacity",
            )
        })?;
    if reservation_capacity > MAX_DERIVED_TABLE_CAPACITY {
        return Err(invalid_option(
            "hotspot_rounds",
            format!(
                "derives reservation-table capacity {reservation_capacity} beyond safe benchmark limit {MAX_DERIVED_TABLE_CAPACITY}"
            ),
        ));
    }

    let operation_capacity =
        checked_hotspot_operation_capacity(options.hotspot_rounds, options.hotspot_contenders)
            .ok_or_else(|| {
                invalid_option(
                    "hotspot_rounds/hotspot_contenders",
                    "overflows the derived operation-table capacity",
                )
            })?;
    if operation_capacity > MAX_DERIVED_TABLE_CAPACITY {
        return Err(invalid_option(
            "hotspot_rounds/hotspot_contenders",
            format!(
                "derives operation-table capacity {operation_capacity} beyond safe benchmark limit {MAX_DERIVED_TABLE_CAPACITY}"
            ),
        ));
    }

    Ok(())
}

fn validate_retry_derived_capacities(options: BenchmarkOptions) -> Result<(), BenchmarkError> {
    let resource_capacity = checked_retry_resource_capacity(options.retry_table_capacity)
        .ok_or_else(|| {
            invalid_option(
                "retry_table_capacity",
                "overflows the derived resource-table capacity",
            )
        })?;
    if resource_capacity > MAX_DERIVED_TABLE_CAPACITY {
        return Err(invalid_option(
            "retry_table_capacity",
            format!(
                "must be at most {} so the derived resource-table capacity stays within safe benchmark limit {MAX_DERIVED_TABLE_CAPACITY}",
                MAX_DERIVED_TABLE_CAPACITY - RETRY_RESOURCE_HEADROOM
            ),
        ));
    }

    let total_operations = checked_high_retry_total_operations(options).ok_or_else(|| {
        invalid_option(
            "retry_table_capacity/retry_duplicate_fanout/retry_full_rejection_attempts",
            "overflowed the derived high-retry total operation count",
        )
    })?;
    if total_operations > MAX_RETRY_PRESSURE_TOTAL_OPERATIONS {
        return Err(invalid_option(
            "retry_table_capacity/retry_duplicate_fanout/retry_full_rejection_attempts",
            format!(
                "derive {total_operations} total high-retry operations beyond safe benchmark limit {MAX_RETRY_PRESSURE_TOTAL_OPERATIONS}"
            ),
        ));
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub queue_depth: u32,
    pub queue_capacity: u32,
    pub logical_slot_lag: u64,
    pub expiration_backlog: u32,
    pub operation_table_used: u32,
    pub operation_table_capacity: u32,
    pub operation_table_utilization_pct: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OneResourceManyContendersReport {
    pub elapsed: Duration,
    pub total_operations: u64,
    pub rounds: u32,
    pub contenders_per_round: u32,
    pub successful_reservations: u32,
    pub busy_reservations: u32,
    pub successful_releases: u32,
    pub peak_operation_table_used: u32,
    pub peak_operation_table_utilization_pct: u8,
    pub final_metrics: MetricsSnapshot,
    pub wal_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HighRetryPressureReport {
    pub elapsed: Duration,
    pub total_operations: u64,
    pub table_capacity: u32,
    pub duplicate_fanout: u32,
    pub cached_duplicate_hits: u32,
    pub cached_conflict_hits: u32,
    pub operation_table_full_hits: u32,
    pub recovered_after_retirement: u32,
    pub peak_operation_table_used: u32,
    pub peak_operation_table_utilization_pct: u8,
    pub pre_retire_metrics: MetricsSnapshot,
    pub final_metrics: MetricsSnapshot,
    pub wal_bytes_after_fill: u64,
    pub wal_bytes_after_retry_pressure: u64,
    pub wal_bytes_after_full_rejections: u64,
    pub wal_bytes_after_retirement: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct HotspotProgress {
    successful_reservations: u32,
    busy_reservations: u32,
    successful_releases: u32,
    peak_operation_table_used: u32,
    peak_operation_table_utilization_pct: u8,
}

impl HotspotProgress {
    fn record_round(&mut self, contenders: u32, metrics: MetricsSnapshot) {
        self.successful_reservations += 1;
        self.busy_reservations += contenders.saturating_sub(1);
        self.successful_releases += 1;
        self.peak_operation_table_used = self
            .peak_operation_table_used
            .max(metrics.operation_table_used);
        self.peak_operation_table_utilization_pct = self
            .peak_operation_table_utilization_pct
            .max(metrics.operation_table_utilization_pct);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RetryPressureProgress {
    duplicate_cached: u32,
    conflict_cached: u32,
    full_rejections: u32,
}

#[derive(Clone, Copy, Debug)]
struct SubmissionExpectation {
    scenario: ScenarioName,
    step: &'static str,
    result_code: ResultCode,
    retry_cache: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioReport {
    OneResourceManyContenders(OneResourceManyContendersReport),
    HighRetryPressure(HighRetryPressureReport),
}

impl ScenarioReport {
    #[must_use]
    pub const fn scenario_name(self) -> ScenarioName {
        match self {
            Self::OneResourceManyContenders(_) => ScenarioName::OneResourceManyContenders,
            Self::HighRetryPressure(_) => ScenarioName::HighRetryPressure,
        }
    }

    #[must_use]
    pub const fn total_operations(self) -> u64 {
        match self {
            Self::OneResourceManyContenders(report) => report.total_operations,
            Self::HighRetryPressure(report) => report.total_operations,
        }
    }

    #[must_use]
    pub const fn elapsed(self) -> Duration {
        match self {
            Self::OneResourceManyContenders(report) => report.elapsed,
            Self::HighRetryPressure(report) => report.elapsed,
        }
    }
}

impl fmt::Display for ScenarioReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OneResourceManyContenders(report) => {
                format_one_resource_many_contenders_report(*report, formatter)
            }
            Self::HighRetryPressure(report) => {
                format_high_retry_pressure_report(*report, formatter)
            }
        }
    }
}

#[derive(Debug)]
pub enum BenchmarkError {
    InvalidOption {
        option: &'static str,
        message: String,
    },
    EngineOpen(EngineOpenError),
    Submission(SubmissionError),
    Io(std::io::Error),
    InvariantViolation {
        scenario: ScenarioName,
        step: &'static str,
        message: String,
    },
}

impl fmt::Display for BenchmarkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOption { option, message } => {
                write!(formatter, "invalid option `{option}`: {message}")
            }
            Self::EngineOpen(error) => write!(formatter, "engine open failed: {error:?}"),
            Self::Submission(error) => write!(formatter, "engine submission failed: {error:?}"),
            Self::Io(error) => write!(formatter, "i/o failed: {error}"),
            Self::InvariantViolation {
                scenario,
                step,
                message,
            } => write!(
                formatter,
                "scenario `{}` violated `{step}`: {message}",
                scenario.as_str()
            ),
        }
    }
}

impl std::error::Error for BenchmarkError {}

fn invalid_option(option: &'static str, message: impl Into<String>) -> BenchmarkError {
    BenchmarkError::InvalidOption {
        option,
        message: message.into(),
    }
}

impl From<std::io::Error> for BenchmarkError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<EngineOpenError> for BenchmarkError {
    fn from(error: EngineOpenError) -> Self {
        Self::EngineOpen(error)
    }
}

impl From<SubmissionError> for BenchmarkError {
    fn from(error: SubmissionError) -> Self {
        Self::Submission(error)
    }
}

/// Runs one or both deterministic benchmark scenarios against the single-node engine.
///
/// # Errors
///
/// Returns [`BenchmarkError`] if the provided options are invalid or if a scenario observes a
/// result that no longer matches the documented engine semantics.
pub fn run_benchmarks(options: BenchmarkOptions) -> Result<Vec<ScenarioReport>, BenchmarkError> {
    options.validate()?;

    let mut reports = Vec::with_capacity(match options.scenario {
        ScenarioSelection::All => 2,
        ScenarioSelection::OneResourceManyContenders | ScenarioSelection::HighRetryPressure => 1,
    });

    match options.scenario {
        ScenarioSelection::All => {
            reports.push(ScenarioReport::OneResourceManyContenders(
                run_one_resource_many_contenders(options)?,
            ));
            reports.push(ScenarioReport::HighRetryPressure(run_high_retry_pressure(
                options,
            )?));
        }
        ScenarioSelection::OneResourceManyContenders => {
            reports.push(ScenarioReport::OneResourceManyContenders(
                run_one_resource_many_contenders(options)?,
            ));
        }
        ScenarioSelection::HighRetryPressure => {
            reports.push(ScenarioReport::HighRetryPressure(run_high_retry_pressure(
                options,
            )?));
        }
    }

    Ok(reports)
}

fn run_one_resource_many_contenders(
    options: BenchmarkOptions,
) -> Result<OneResourceManyContendersReport, BenchmarkError> {
    let scenario = ScenarioName::OneResourceManyContenders;
    let workspace = ScenarioWorkspace::new(scenario)?;
    let mut engine = open_hotspot_engine(options, &workspace.wal_path)?;
    let mut next_operation_id = 1_u128;

    let started = Instant::now();
    seed_hotspot_resource(&mut engine, &mut next_operation_id)?;
    let progress = run_hotspot_rounds(&mut engine, options, &mut next_operation_id)?;
    let elapsed = started.elapsed();

    let final_slot = Slot(u64::from(options.hotspot_rounds));
    let final_metrics = snapshot_metrics(&engine, final_slot);
    ensure_hotspot_resource_is_available(&engine)?;

    Ok(OneResourceManyContendersReport {
        elapsed,
        total_operations: u64::try_from(next_operation_id - 1)
            .expect("hot-spot operation count must fit u64"),
        rounds: options.hotspot_rounds,
        contenders_per_round: options.hotspot_contenders,
        successful_reservations: progress.successful_reservations,
        busy_reservations: progress.busy_reservations,
        successful_releases: progress.successful_releases,
        peak_operation_table_used: progress.peak_operation_table_used,
        peak_operation_table_utilization_pct: progress.peak_operation_table_utilization_pct,
        final_metrics,
        wal_bytes: wal_len(&workspace.wal_path)?,
    })
}

fn run_high_retry_pressure(
    options: BenchmarkOptions,
) -> Result<HighRetryPressureReport, BenchmarkError> {
    let scenario = ScenarioName::HighRetryPressure;
    let workspace = ScenarioWorkspace::new(scenario)?;
    let mut engine = open_retry_engine(options, &workspace.wal_path)?;

    let started = Instant::now();
    fill_retry_operation_table(&mut engine, options.retry_table_capacity)?;
    let pre_retire_metrics = snapshot_metrics(&engine, Slot(1));
    let wal_bytes_after_fill = wal_len(&workspace.wal_path)?;

    let mut progress = exercise_retry_cache(
        &mut engine,
        options.retry_table_capacity,
        options.retry_duplicate_fanout,
    )?;
    let wal_bytes_after_retry_pressure = wal_len(&workspace.wal_path)?;

    let mut next_operation_id = u128::from(options.retry_table_capacity) + 1;
    progress.full_rejections = exercise_full_rejections(
        &mut engine,
        options.retry_full_rejection_attempts,
        &mut next_operation_id,
    )?;
    let wal_bytes_after_full_rejections = wal_len(&workspace.wal_path)?;

    let retirement_slot = recover_after_retry_window(&mut engine, next_operation_id)?;
    let elapsed = started.elapsed();
    let final_metrics = snapshot_metrics(&engine, retirement_slot);
    let wal_bytes_after_retirement = wal_len(&workspace.wal_path)?;
    ensure_high_retry_pressure_invariants(
        options,
        pre_retire_metrics,
        final_metrics,
        wal_bytes_after_fill,
        wal_bytes_after_retry_pressure,
        wal_bytes_after_full_rejections,
        wal_bytes_after_retirement,
    )?;

    Ok(HighRetryPressureReport {
        elapsed,
        total_operations: checked_high_retry_total_operations(options)
            .expect("validated benchmark options must keep high-retry operation count in range"),
        table_capacity: options.retry_table_capacity,
        duplicate_fanout: options.retry_duplicate_fanout,
        cached_duplicate_hits: progress.duplicate_cached,
        cached_conflict_hits: progress.conflict_cached,
        operation_table_full_hits: progress.full_rejections,
        recovered_after_retirement: 1,
        peak_operation_table_used: pre_retire_metrics.operation_table_used,
        peak_operation_table_utilization_pct: pre_retire_metrics.operation_table_utilization_pct,
        pre_retire_metrics,
        final_metrics,
        wal_bytes_after_fill,
        wal_bytes_after_retry_pressure,
        wal_bytes_after_full_rejections,
        wal_bytes_after_retirement,
    })
}

fn checked_hotspot_reservation_capacity(rounds: u32) -> Option<u32> {
    rounds.checked_add(4)
}

fn hotspot_reservation_capacity(rounds: u32) -> u32 {
    checked_hotspot_reservation_capacity(rounds)
        .expect("validated benchmark options must keep hotspot reservation capacity in range")
}

fn checked_hotspot_operation_capacity(rounds: u32, contenders_per_round: u32) -> Option<u32> {
    let ops_per_round = contenders_per_round.checked_add(1)?;
    rounds.checked_mul(ops_per_round)?.checked_add(9)
}

fn hotspot_operation_capacity(rounds: u32, contenders_per_round: u32) -> u32 {
    checked_hotspot_operation_capacity(rounds, contenders_per_round)
        .expect("validated benchmark options must keep hotspot operation capacity in range")
}

fn checked_retry_resource_capacity(table_capacity: u32) -> Option<u32> {
    table_capacity.checked_add(RETRY_RESOURCE_HEADROOM)
}

fn retry_resource_capacity(table_capacity: u32) -> u32 {
    checked_retry_resource_capacity(table_capacity)
        .expect("validated benchmark options must keep retry resource capacity in range")
}

fn checked_high_retry_total_operations(options: BenchmarkOptions) -> Option<u64> {
    let capacity = u64::from(options.retry_table_capacity);
    let duplicate_fanout = u64::from(options.retry_duplicate_fanout);
    let rejection_attempts = u64::from(options.retry_full_rejection_attempts);
    capacity
        .checked_add(capacity.checked_mul(duplicate_fanout)?.checked_mul(2)?)?
        .checked_add(rejection_attempts)?
        .checked_add(1)
}

fn format_one_resource_many_contenders_report(
    report: OneResourceManyContendersReport,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    writeln!(
        formatter,
        "scenario: {}",
        ScenarioName::OneResourceManyContenders.as_str()
    )?;
    writeln!(
        formatter,
        "elapsed_ms: {}",
        format_duration_millis(report.elapsed)
    )?;
    writeln!(formatter, "total_operations: {}", report.total_operations)?;
    writeln!(
        formatter,
        "throughput_ops_per_sec: {}",
        format_throughput(report.total_operations, report.elapsed)
    )?;
    writeln!(formatter, "rounds: {}", report.rounds)?;
    writeln!(
        formatter,
        "contenders_per_round: {}",
        report.contenders_per_round
    )?;
    writeln!(
        formatter,
        "successful_reservations: {}",
        report.successful_reservations
    )?;
    writeln!(formatter, "busy_reservations: {}", report.busy_reservations)?;
    writeln!(
        formatter,
        "successful_releases: {}",
        report.successful_releases
    )?;
    writeln!(
        formatter,
        "peak_operation_table_used: {}/{}",
        report.peak_operation_table_used, report.final_metrics.operation_table_capacity
    )?;
    writeln!(
        formatter,
        "peak_operation_table_utilization_pct: {}",
        report.peak_operation_table_utilization_pct
    )?;
    format_metrics("final", report.final_metrics, formatter)?;
    writeln!(formatter, "wal_bytes: {}", report.wal_bytes)
}

fn format_high_retry_pressure_report(
    report: HighRetryPressureReport,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    writeln!(
        formatter,
        "scenario: {}",
        ScenarioName::HighRetryPressure.as_str()
    )?;
    writeln!(
        formatter,
        "elapsed_ms: {}",
        format_duration_millis(report.elapsed)
    )?;
    writeln!(formatter, "total_operations: {}", report.total_operations)?;
    writeln!(
        formatter,
        "throughput_ops_per_sec: {}",
        format_throughput(report.total_operations, report.elapsed)
    )?;
    writeln!(formatter, "table_capacity: {}", report.table_capacity)?;
    writeln!(formatter, "duplicate_fanout: {}", report.duplicate_fanout)?;
    writeln!(
        formatter,
        "cached_duplicate_hits: {}",
        report.cached_duplicate_hits
    )?;
    writeln!(
        formatter,
        "cached_conflict_hits: {}",
        report.cached_conflict_hits
    )?;
    writeln!(
        formatter,
        "operation_table_full_hits: {}",
        report.operation_table_full_hits
    )?;
    writeln!(
        formatter,
        "recovered_after_retirement: {}",
        report.recovered_after_retirement
    )?;
    writeln!(
        formatter,
        "peak_operation_table_used: {}/{}",
        report.peak_operation_table_used, report.table_capacity
    )?;
    writeln!(
        formatter,
        "peak_operation_table_utilization_pct: {}",
        report.peak_operation_table_utilization_pct
    )?;
    format_metrics("pre_retire", report.pre_retire_metrics, formatter)?;
    format_metrics("final", report.final_metrics, formatter)?;
    writeln!(
        formatter,
        "wal_bytes_after_fill: {}",
        report.wal_bytes_after_fill
    )?;
    writeln!(
        formatter,
        "wal_bytes_after_retry_pressure: {}",
        report.wal_bytes_after_retry_pressure
    )?;
    writeln!(
        formatter,
        "wal_bytes_after_full_rejections: {}",
        report.wal_bytes_after_full_rejections
    )?;
    writeln!(
        formatter,
        "wal_bytes_after_retirement: {}",
        report.wal_bytes_after_retirement
    )
}

fn open_hotspot_engine(
    options: BenchmarkOptions,
    wal_path: &PathBuf,
) -> Result<SingleNodeEngine, BenchmarkError> {
    let config = Config {
        shard_id: 0,
        max_resources: 1,
        max_reservations: hotspot_reservation_capacity(options.hotspot_rounds),
        max_bundle_size: 1,
        max_operations: hotspot_operation_capacity(
            options.hotspot_rounds,
            options.hotspot_contenders,
        ),
        max_ttl_slots: HOTSPOT_TTL_SLOTS,
        max_client_retry_window_slots: HOTSPOT_RETRY_WINDOW_SLOTS,
        reservation_history_window_slots: HOTSPOT_HISTORY_WINDOW_SLOTS,
        max_expiration_bucket_len: 1,
    };
    open_engine(config, wal_path)
}

fn open_retry_engine(
    options: BenchmarkOptions,
    wal_path: &PathBuf,
) -> Result<SingleNodeEngine, BenchmarkError> {
    let config = Config {
        shard_id: 0,
        max_resources: retry_resource_capacity(options.retry_table_capacity),
        max_reservations: 1,
        max_bundle_size: 1,
        max_operations: options.retry_table_capacity,
        max_ttl_slots: RETRY_TTL_SLOTS,
        max_client_retry_window_slots: RETRY_WINDOW_SLOTS,
        reservation_history_window_slots: RETRY_HISTORY_WINDOW_SLOTS,
        max_expiration_bucket_len: 1,
    };
    open_engine(config, wal_path)
}

fn open_engine(config: Config, wal_path: &PathBuf) -> Result<SingleNodeEngine, BenchmarkError> {
    SingleNodeEngine::open(
        config,
        EngineConfig {
            max_submission_queue: DEFAULT_QUEUE_CAPACITY,
            max_command_bytes: DEFAULT_MAX_COMMAND_BYTES,
            max_expirations_per_tick: DEFAULT_EXPIRATIONS_PER_TICK,
        },
        wal_path,
    )
    .map_err(BenchmarkError::from)
}

fn seed_hotspot_resource(
    engine: &mut SingleNodeEngine,
    next_operation_id: &mut u128,
) -> Result<(), BenchmarkError> {
    submit_create_resource(
        engine,
        Slot(1),
        ResourceId(1),
        OperationId(*next_operation_id),
        SubmissionExpectation {
            scenario: ScenarioName::OneResourceManyContenders,
            step: "create-resource",
            result_code: ResultCode::Ok,
            retry_cache: false,
        },
    )?;
    *next_operation_id += 1;
    Ok(())
}

fn run_hotspot_rounds(
    engine: &mut SingleNodeEngine,
    options: BenchmarkOptions,
    next_operation_id: &mut u128,
) -> Result<HotspotProgress, BenchmarkError> {
    let mut progress = HotspotProgress::default();
    for round in 0..options.hotspot_rounds {
        let metrics =
            run_hotspot_round(engine, round, options.hotspot_contenders, next_operation_id)?;
        progress.record_round(options.hotspot_contenders, metrics);
    }
    Ok(progress)
}

fn run_hotspot_round(
    engine: &mut SingleNodeEngine,
    round: u32,
    contenders: u32,
    next_operation_id: &mut u128,
) -> Result<MetricsSnapshot, BenchmarkError> {
    let round_slot = Slot(u64::from(round) + 1);
    let winner_holder = HolderId(u128::from(round) + 1);
    let reserve_result = submit_hotspot_reserve(
        engine,
        round_slot,
        winner_holder,
        OperationId(*next_operation_id),
        "reserve-winner",
        ResultCode::Ok,
    )?;
    *next_operation_id += 1;
    let reservation_id = reserve_result.outcome.reservation_id.ok_or_else(|| {
        BenchmarkError::InvariantViolation {
            scenario: ScenarioName::OneResourceManyContenders,
            step: "reserve-winner",
            message: String::from("successful reserve did not return a reservation id"),
        }
    })?;

    for contender in 1..contenders {
        let holder_id =
            HolderId((u128::from(round) * u128::from(contenders)) + u128::from(contender) + 1);
        submit_hotspot_reserve(
            engine,
            round_slot,
            holder_id,
            OperationId(*next_operation_id),
            "reserve-contender",
            ResultCode::ResourceBusy,
        )?;
        *next_operation_id += 1;
    }

    submit_hotspot_release(
        engine,
        round_slot,
        reservation_id,
        winner_holder,
        OperationId(*next_operation_id),
    )?;
    *next_operation_id += 1;
    Ok(snapshot_metrics(engine, round_slot))
}

fn submit_hotspot_reserve(
    engine: &mut SingleNodeEngine,
    round_slot: Slot,
    holder_id: HolderId,
    operation_id: OperationId,
    step: &'static str,
    expected_code: ResultCode,
) -> Result<allocdb_node::engine::SubmissionResult, BenchmarkError> {
    let request = reserve_request(ResourceId(1), holder_id, HOTSPOT_TTL_SLOTS, operation_id);
    let result = engine.submit(round_slot, request)?;
    expect_submission(
        ScenarioName::OneResourceManyContenders,
        step,
        result,
        expected_code,
        false,
    )
}

fn submit_hotspot_release(
    engine: &mut SingleNodeEngine,
    round_slot: Slot,
    reservation_id: ReservationId,
    holder_id: HolderId,
    operation_id: OperationId,
) -> Result<(), BenchmarkError> {
    let result = engine.submit(
        round_slot,
        release_request(reservation_id, holder_id, operation_id),
    )?;
    expect_submission(
        ScenarioName::OneResourceManyContenders,
        "release-winner",
        result,
        ResultCode::Ok,
        false,
    )?;
    Ok(())
}

fn ensure_hotspot_resource_is_available(engine: &SingleNodeEngine) -> Result<(), BenchmarkError> {
    let resource =
        engine
            .db()
            .resource(ResourceId(1))
            .ok_or_else(|| BenchmarkError::InvariantViolation {
                scenario: ScenarioName::OneResourceManyContenders,
                step: "final-resource",
                message: String::from("benchmarked resource disappeared"),
            })?;
    if resource.current_state != ResourceState::Available {
        return Err(BenchmarkError::InvariantViolation {
            scenario: ScenarioName::OneResourceManyContenders,
            step: "final-resource",
            message: format!(
                "expected resource to be available, got {:?}",
                resource.current_state
            ),
        });
    }
    Ok(())
}

fn fill_retry_operation_table(
    engine: &mut SingleNodeEngine,
    capacity: u32,
) -> Result<(), BenchmarkError> {
    for resource_index in 0..capacity {
        let id = u128::from(resource_index) + 1;
        submit_create_resource(
            engine,
            Slot(1),
            ResourceId(id),
            OperationId(id),
            SubmissionExpectation {
                scenario: ScenarioName::HighRetryPressure,
                step: "fill-operation-table",
                result_code: ResultCode::Ok,
                retry_cache: false,
            },
        )?;
    }
    Ok(())
}

fn exercise_retry_cache(
    engine: &mut SingleNodeEngine,
    capacity: u32,
    duplicate_fanout: u32,
) -> Result<RetryPressureProgress, BenchmarkError> {
    let mut progress = RetryPressureProgress::default();
    for resource_index in 0..capacity {
        let id = u128::from(resource_index) + 1;
        for _ in 0..duplicate_fanout {
            submit_create_resource(
                engine,
                Slot(1),
                ResourceId(id),
                OperationId(id),
                SubmissionExpectation {
                    scenario: ScenarioName::HighRetryPressure,
                    step: "duplicate-retry",
                    result_code: ResultCode::Ok,
                    retry_cache: true,
                },
            )?;
            progress.duplicate_cached += 1;

            submit_create_resource(
                engine,
                Slot(1),
                ResourceId(id + RETRY_CONFLICT_RESOURCE_OFFSET),
                OperationId(id),
                SubmissionExpectation {
                    scenario: ScenarioName::HighRetryPressure,
                    step: "conflicting-retry",
                    result_code: ResultCode::OperationConflict,
                    retry_cache: true,
                },
            )?;
            progress.conflict_cached += 1;
        }
    }
    Ok(progress)
}

fn exercise_full_rejections(
    engine: &mut SingleNodeEngine,
    attempts: u32,
    next_operation_id: &mut u128,
) -> Result<u32, BenchmarkError> {
    let mut rejections = 0_u32;
    for _ in 0..attempts {
        let id = *next_operation_id;
        submit_create_resource(
            engine,
            Slot(1),
            ResourceId(id),
            OperationId(id),
            SubmissionExpectation {
                scenario: ScenarioName::HighRetryPressure,
                step: "operation-table-full",
                result_code: ResultCode::OperationTableFull,
                retry_cache: false,
            },
        )?;
        *next_operation_id += 1;
        rejections += 1;
    }
    Ok(rejections)
}

fn recover_after_retry_window(
    engine: &mut SingleNodeEngine,
    next_operation_id: u128,
) -> Result<Slot, BenchmarkError> {
    let retirement_slot = Slot(RETRY_TTL_SLOTS + RETRY_WINDOW_SLOTS + 2);
    submit_create_resource(
        engine,
        retirement_slot,
        ResourceId(next_operation_id),
        OperationId(next_operation_id),
        SubmissionExpectation {
            scenario: ScenarioName::HighRetryPressure,
            step: "post-retirement-recovery",
            result_code: ResultCode::Ok,
            retry_cache: false,
        },
    )?;
    Ok(retirement_slot)
}

fn ensure_high_retry_pressure_invariants(
    options: BenchmarkOptions,
    pre_retire_metrics: MetricsSnapshot,
    final_metrics: MetricsSnapshot,
    wal_bytes_after_fill: u64,
    wal_bytes_after_retry_pressure: u64,
    wal_bytes_after_full_rejections: u64,
    wal_bytes_after_retirement: u64,
) -> Result<(), BenchmarkError> {
    let scenario = ScenarioName::HighRetryPressure;
    if pre_retire_metrics.operation_table_used != options.retry_table_capacity {
        return Err(BenchmarkError::InvariantViolation {
            scenario,
            step: "fill-operation-table",
            message: format!(
                "expected the operation table to fill to {}, got {}",
                options.retry_table_capacity, pre_retire_metrics.operation_table_used
            ),
        });
    }

    if wal_bytes_after_retry_pressure != wal_bytes_after_fill {
        return Err(BenchmarkError::InvariantViolation {
            scenario,
            step: "retry-pressure-wal",
            message: format!(
                "expected retry-cache pressure not to grow the WAL, got {wal_bytes_after_fill} -> {wal_bytes_after_retry_pressure}"
            ),
        });
    }

    if wal_bytes_after_full_rejections <= wal_bytes_after_retry_pressure {
        return Err(BenchmarkError::InvariantViolation {
            scenario,
            step: "operation-table-full-wal",
            message: String::from("expected fresh full-table rejections to append to the WAL"),
        });
    }

    if final_metrics.operation_table_used != 1 {
        return Err(BenchmarkError::InvariantViolation {
            scenario,
            step: "post-retirement-utilization",
            message: format!(
                "expected only the post-window recovery operation to remain live, got {} entries",
                final_metrics.operation_table_used
            ),
        });
    }

    if wal_bytes_after_retirement <= wal_bytes_after_full_rejections {
        return Err(BenchmarkError::InvariantViolation {
            scenario,
            step: "post-retirement-wal",
            message: format!(
                "expected post-window recovery to append to the WAL, got {wal_bytes_after_full_rejections} -> {wal_bytes_after_retirement}"
            ),
        });
    }

    Ok(())
}

fn submit_create_resource(
    engine: &mut SingleNodeEngine,
    slot: Slot,
    resource_id: ResourceId,
    operation_id: OperationId,
    expectation: SubmissionExpectation,
) -> Result<allocdb_node::engine::SubmissionResult, BenchmarkError> {
    let request = create_resource_request(resource_id, operation_id, BENCH_CLIENT_ID);
    let result = engine.submit(slot, request)?;
    expect_submission(
        expectation.scenario,
        expectation.step,
        result,
        expectation.result_code,
        expectation.retry_cache,
    )
}

fn create_resource_request(
    resource_id: ResourceId,
    operation_id: OperationId,
    client_id: ClientId,
) -> ClientRequest {
    ClientRequest {
        operation_id,
        client_id,
        command: Command::CreateResource { resource_id },
    }
}

fn reserve_request(
    resource_id: ResourceId,
    holder_id: HolderId,
    ttl_slots: u64,
    operation_id: OperationId,
) -> ClientRequest {
    ClientRequest {
        operation_id,
        client_id: ClientId(holder_id.get()),
        command: Command::Reserve {
            resource_id,
            holder_id,
            ttl_slots,
        },
    }
}

fn release_request(
    reservation_id: ReservationId,
    holder_id: HolderId,
    operation_id: OperationId,
) -> ClientRequest {
    ClientRequest {
        operation_id,
        client_id: ClientId(holder_id.get()),
        command: Command::Release {
            reservation_id,
            holder_id,
        },
    }
}

fn expect_submission(
    scenario: ScenarioName,
    step: &'static str,
    result: allocdb_node::engine::SubmissionResult,
    expected_code: ResultCode,
    expected_retry_cache: bool,
) -> Result<allocdb_node::engine::SubmissionResult, BenchmarkError> {
    if result.outcome.result_code != expected_code
        || result.from_retry_cache != expected_retry_cache
    {
        return Err(BenchmarkError::InvariantViolation {
            scenario,
            step,
            message: format!(
                "expected result_code={expected_code:?} retry_cache={expected_retry_cache}, got result_code={:?} retry_cache={}",
                result.outcome.result_code, result.from_retry_cache
            ),
        });
    }

    Ok(result)
}

fn snapshot_metrics(engine: &SingleNodeEngine, current_slot: Slot) -> MetricsSnapshot {
    let metrics = engine.metrics(current_slot);
    MetricsSnapshot {
        queue_depth: metrics.queue_depth,
        queue_capacity: metrics.queue_capacity,
        logical_slot_lag: metrics.core.logical_slot_lag,
        expiration_backlog: metrics.core.expiration_backlog,
        operation_table_used: metrics.core.operation_table_used,
        operation_table_capacity: metrics.core.operation_table_capacity,
        operation_table_utilization_pct: metrics.core.operation_table_utilization_pct,
    }
}

fn wal_len(path: &PathBuf) -> Result<u64, BenchmarkError> {
    Ok(fs::metadata(path)?.len())
}

fn format_metrics(
    prefix: &str,
    metrics: MetricsSnapshot,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    writeln!(
        formatter,
        "{prefix}_queue_depth: {}/{}",
        metrics.queue_depth, metrics.queue_capacity
    )?;
    writeln!(
        formatter,
        "{prefix}_logical_slot_lag: {}",
        metrics.logical_slot_lag
    )?;
    writeln!(
        formatter,
        "{prefix}_expiration_backlog: {}",
        metrics.expiration_backlog
    )?;
    writeln!(
        formatter,
        "{prefix}_operation_table_used: {}/{}",
        metrics.operation_table_used, metrics.operation_table_capacity
    )?;
    writeln!(
        formatter,
        "{prefix}_operation_table_utilization_pct: {}",
        metrics.operation_table_utilization_pct
    )
}

fn format_duration_millis(duration: Duration) -> String {
    let total_micros = duration.as_micros();
    let whole_millis = total_micros / 1_000;
    let fractional_millis = total_micros % 1_000;
    format!("{whole_millis}.{fractional_millis:03}")
}

fn format_throughput(total_operations: u64, duration: Duration) -> String {
    if duration.is_zero() {
        return String::from("inf");
    }

    let ops_x100 = u128::from(total_operations)
        .saturating_mul(100)
        .saturating_mul(1_000_000_000)
        / duration.as_nanos();
    let whole = ops_x100 / 100;
    let fraction = ops_x100 % 100;
    format!("{whole}.{fraction:02}")
}

struct ScenarioWorkspace {
    wal_path: PathBuf,
}

impl ScenarioWorkspace {
    fn new(scenario: ScenarioName) -> Result<Self, BenchmarkError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| BenchmarkError::InvariantViolation {
                scenario,
                step: "workspace-clock",
                message: error.to_string(),
            })?
            .as_nanos();
        let workspace_id = NEXT_WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
        let wal_path = std::env::temp_dir().join(format!(
            "allocdb-bench-{}-{}-{}-{}.wal",
            scenario.as_str(),
            std::process::id(),
            workspace_id,
            timestamp
        ));
        Ok(Self { wal_path })
    }
}

impl Drop for ScenarioWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.wal_path);
        let _ = fs::remove_file(self.wal_path.with_extension("snapshot"));
    }
}
