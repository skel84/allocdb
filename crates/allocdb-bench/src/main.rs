use std::process::ExitCode;

use allocdb_bench::{BenchmarkOptions, ScenarioSelection, run_benchmarks};

enum ParsedCommand {
    Help,
    Run(BenchmarkOptions),
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let command = parse_args(std::env::args().skip(1))?;
    let ParsedCommand::Run(options) = command else {
        print!("{}", usage());
        return Ok(());
    };
    let reports = run_benchmarks(options).map_err(|error| error.to_string())?;
    for (index, report) in reports.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print!("{report}");
    }
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut options = BenchmarkOptions::default();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(ParsedCommand::Help),
            "--scenario" => {
                let value = args.next().ok_or_else(usage)?;
                options.scenario = parse_scenario(&value)?;
            }
            "--hotspot-rounds" => {
                options.hotspot_rounds = parse_u32("--hotspot-rounds", args.next())?;
            }
            "--hotspot-contenders" => {
                options.hotspot_contenders = parse_u32("--hotspot-contenders", args.next())?;
            }
            "--retry-table-capacity" => {
                options.retry_table_capacity = parse_u32("--retry-table-capacity", args.next())?;
            }
            "--retry-duplicate-fanout" => {
                options.retry_duplicate_fanout =
                    parse_u32("--retry-duplicate-fanout", args.next())?;
            }
            "--retry-full-rejection-attempts" => {
                options.retry_full_rejection_attempts =
                    parse_u32("--retry-full-rejection-attempts", args.next())?;
            }
            _ => return Err(format!("unknown argument `{argument}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::Run(options))
}

fn parse_scenario(value: &str) -> Result<ScenarioSelection, String> {
    match value {
        "all" => Ok(ScenarioSelection::All),
        "one-resource-many-contenders" => Ok(ScenarioSelection::OneResourceManyContenders),
        "high-retry-pressure" => Ok(ScenarioSelection::HighRetryPressure),
        _ => Err(format!("unknown scenario `{value}`\n\n{}", usage())),
    }
}

fn parse_u32(flag: &str, value: Option<String>) -> Result<u32, String> {
    let raw = value.ok_or_else(usage)?;
    raw.parse::<u32>()
        .map_err(|_| format!("invalid value for `{flag}`: `{raw}`\n\n{}", usage()))
}

fn usage() -> String {
    String::from(
        "usage: cargo run -p allocdb-bench -- [options]\n\
         \n\
         options:\n\
         \x20\x20--scenario <all|one-resource-many-contenders|high-retry-pressure>\n\
         \x20\x20--hotspot-rounds <u32>\n\
         \x20\x20--hotspot-contenders <u32>\n\
         \x20\x20--retry-table-capacity <u32>\n\
         \x20\x20--retry-duplicate-fanout <u32>\n\
         \x20\x20--retry-full-rejection-attempts <u32>\n\
         \x20\x20--help\n",
    )
}
