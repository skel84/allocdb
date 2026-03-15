use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use super::NEXT_PROBE_RESOURCE_ID;

pub(super) fn required_field<'a>(
    fields: &'a std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<&'a str, String> {
    fields
        .get(field)
        .map(String::as_str)
        .ok_or_else(|| format!("missing run status field `{field}`"))
}

pub(super) fn parse_required_u32(
    fields: &std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<u32, String> {
    required_field(fields, field)?
        .parse::<u32>()
        .map_err(|error| format!("invalid `{field}`: {error}"))
}

pub(super) fn parse_required_u64(
    fields: &std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<u64, String> {
    required_field(fields, field)?
        .parse::<u64>()
        .map_err(|error| format!("invalid `{field}`: {error}"))
}

pub(super) fn parse_required_u128(
    fields: &std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<u128, String> {
    required_field(fields, field)?
        .parse::<u128>()
        .map_err(|error| format!("invalid `{field}`: {error}"))
}

pub(super) fn parse_optional_u64(value: &str) -> Result<Option<u64>, String> {
    if value == "none" {
        Ok(None)
    } else {
        value
            .parse::<u64>()
            .map(Some)
            .map_err(|error| format!("invalid optional u64 `{value}`: {error}"))
    }
}

pub(super) fn parse_optional_usize(value: &str) -> Result<Option<usize>, String> {
    if value == "none" {
        Ok(None)
    } else {
        value
            .parse::<usize>()
            .map(Some)
            .map_err(|error| format!("invalid optional usize `{value}`: {error}"))
    }
}

pub(super) fn parse_optional_bool(value: &str) -> Result<Option<bool>, String> {
    match value {
        "none" => Ok(None),
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        other => Err(format!("invalid optional bool `{other}`")),
    }
}

pub(super) fn parse_optional_path(value: &str) -> Option<PathBuf> {
    if value == "none" {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

pub(super) fn parse_optional_string(value: &str) -> Option<String> {
    if value == "none" {
        None
    } else {
        Some(String::from(value))
    }
}

pub(super) fn write_text_atomically(path: &Path, bytes: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let temp_path = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = File::create(&temp_path)
        .map_err(|error| format!("failed to create {}: {error}", temp_path.display()))?;
    file.write_all(bytes.as_bytes())
        .map_err(|error| format!("failed to write {}: {error}", temp_path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", temp_path.display()))?;
    drop(file);
    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        format!("failed to replace {}: {error}", path.display())
    })
}

pub(super) fn append_text_line(path: &Path, line: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    file.write_all(line.as_bytes())
        .map_err(|error| format!("failed to append {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", path.display()))
}

pub(super) fn current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub(super) fn unique_probe_resource_id() -> u128 {
    let millis = current_time_millis();
    let nonce = u128::from(NEXT_PROBE_RESOURCE_ID.fetch_add(1, Ordering::Relaxed));
    (millis << 32) | nonce
}
