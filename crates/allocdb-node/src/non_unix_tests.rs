use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::replica::{
    ReplicaId, ReplicaIdentity, ReplicaMetadata, ReplicaMetadataFile, ReplicaMetadataFileError,
    ReplicaRole,
};

fn test_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-replica-{name}-{nanos}.{extension}"))
}

#[test]
fn replica_metadata_write_is_unsupported_off_unix() {
    let path = test_path("metadata-write-unsupported", "replica");
    let file = ReplicaMetadataFile::new(&path);
    let metadata = ReplicaMetadata {
        identity: ReplicaIdentity {
            replica_id: ReplicaId(1),
            shard_id: 0,
        },
        current_view: 0,
        role: ReplicaRole::Backup,
        commit_lsn: None,
        active_snapshot_lsn: None,
        last_normal_view: None,
        durable_vote: None,
    };

    let error = file.write_metadata(&metadata).unwrap_err();
    assert!(matches!(
        error,
        ReplicaMetadataFileError::Io(error) if error.kind() == ErrorKind::Unsupported
    ));

    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(file.path().with_extension("replica.tmp"));
}
