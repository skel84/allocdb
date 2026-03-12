use super::{
    BenchmarkError, BenchmarkOptions, MAX_DERIVED_TABLE_CAPACITY, ScenarioReport,
    ScenarioSelection, run_benchmarks,
};

#[test]
fn benchmark_options_reject_degenerate_values() {
    let zero_rounds = BenchmarkOptions {
        hotspot_rounds: 0,
        ..BenchmarkOptions::default()
    };
    assert!(zero_rounds.validate().is_err());

    let one_contender = BenchmarkOptions {
        hotspot_contenders: 1,
        ..BenchmarkOptions::default()
    };
    assert!(one_contender.validate().is_err());

    let zero_retry_capacity = BenchmarkOptions {
        retry_table_capacity: 0,
        ..BenchmarkOptions::default()
    };
    assert!(zero_retry_capacity.validate().is_err());

    let zero_duplicate_fanout = BenchmarkOptions {
        retry_duplicate_fanout: 0,
        ..BenchmarkOptions::default()
    };
    assert!(zero_duplicate_fanout.validate().is_err());

    let zero_rejection_attempts = BenchmarkOptions {
        retry_full_rejection_attempts: 0,
        ..BenchmarkOptions::default()
    };
    assert!(zero_rejection_attempts.validate().is_err());
}

#[test]
fn benchmark_options_reject_oversized_derived_capacities() {
    let oversized_hotspot = BenchmarkOptions {
        hotspot_rounds: 1_024,
        hotspot_contenders: 1_024,
        ..BenchmarkOptions::default()
    };
    assert!(matches!(
        oversized_hotspot.validate(),
        Err(BenchmarkError::InvalidOption {
            option: "hotspot_rounds/hotspot_contenders",
            ..
        })
    ));

    let overflowing_hotspot = BenchmarkOptions {
        hotspot_contenders: u32::MAX,
        ..BenchmarkOptions::default()
    };
    assert!(matches!(
        overflowing_hotspot.validate(),
        Err(BenchmarkError::InvalidOption {
            option: "hotspot_rounds/hotspot_contenders",
            ..
        })
    ));

    let oversized_retry_capacity = BenchmarkOptions {
        retry_table_capacity: MAX_DERIVED_TABLE_CAPACITY,
        ..BenchmarkOptions::default()
    };
    assert!(matches!(
        oversized_retry_capacity.validate(),
        Err(BenchmarkError::InvalidOption {
            option: "retry_table_capacity",
            ..
        })
    ));
}

#[test]
fn one_resource_many_contenders_reports_expected_contention() {
    let reports = run_benchmarks(BenchmarkOptions {
        scenario: ScenarioSelection::OneResourceManyContenders,
        hotspot_rounds: 6,
        hotspot_contenders: 4,
        ..BenchmarkOptions::default()
    })
    .unwrap();

    let [ScenarioReport::OneResourceManyContenders(report)] = reports.as_slice() else {
        panic!("expected exactly one one-resource-many-contenders report");
    };

    assert_eq!(report.rounds, 6);
    assert_eq!(report.contenders_per_round, 4);
    assert_eq!(report.successful_reservations, 6);
    assert_eq!(report.busy_reservations, 18);
    assert_eq!(report.successful_releases, 6);
    assert_eq!(report.final_metrics.queue_depth, 0);
    assert_eq!(report.final_metrics.expiration_backlog, 0);
    assert_eq!(report.final_metrics.logical_slot_lag, 0);
    assert!(report.peak_operation_table_used <= report.final_metrics.operation_table_capacity);
    assert!(report.wal_bytes > 0);
}

#[test]
fn high_retry_pressure_reports_retry_cache_and_retirement_recovery() {
    let reports = run_benchmarks(BenchmarkOptions {
        scenario: ScenarioSelection::HighRetryPressure,
        retry_table_capacity: 6,
        retry_duplicate_fanout: 2,
        retry_full_rejection_attempts: 3,
        ..BenchmarkOptions::default()
    })
    .unwrap();

    let [ScenarioReport::HighRetryPressure(report)] = reports.as_slice() else {
        panic!("expected exactly one high-retry-pressure report");
    };

    assert_eq!(report.table_capacity, 6);
    assert_eq!(report.duplicate_fanout, 2);
    assert_eq!(report.cached_duplicate_hits, 12);
    assert_eq!(report.cached_conflict_hits, 12);
    assert_eq!(report.operation_table_full_hits, 3);
    assert_eq!(report.recovered_after_retirement, 1);
    assert_eq!(report.pre_retire_metrics.operation_table_used, 6);
    assert_eq!(report.pre_retire_metrics.operation_table_capacity, 6);
    assert_eq!(
        report.pre_retire_metrics.operation_table_utilization_pct,
        100
    );
    assert_eq!(report.final_metrics.operation_table_used, 1);
    assert_eq!(
        report.wal_bytes_after_fill,
        report.wal_bytes_after_retry_pressure
    );
    assert!(report.wal_bytes_after_full_rejections > report.wal_bytes_after_fill);
    assert!(report.wal_bytes_after_retirement > report.wal_bytes_after_full_rejections);
}

#[test]
fn all_selection_runs_both_scenarios() {
    let reports = run_benchmarks(BenchmarkOptions {
        hotspot_rounds: 4,
        hotspot_contenders: 3,
        retry_table_capacity: 4,
        retry_duplicate_fanout: 1,
        retry_full_rejection_attempts: 1,
        ..BenchmarkOptions::default()
    })
    .unwrap();

    assert_eq!(reports.len(), 2);
    assert!(matches!(
        reports[0],
        ScenarioReport::OneResourceManyContenders(_)
    ));
    assert!(matches!(reports[1], ScenarioReport::HighRetryPressure(_)));
}
