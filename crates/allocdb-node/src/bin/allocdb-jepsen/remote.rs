use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use allocdb_core::ids::{Lsn, ResourceId};
use allocdb_node::qemu_testbed::{QemuTestbedLayout, qemu_testbed_layout_path};
use allocdb_node::{
    ApiRequest, ApiResponse, ReplicaId, ResourceResponse, SubmitResponse, decode_response,
    encode_request,
};

use crate::{CONTROL_HOST_SSH_PORT, EXTERNAL_REMOTE_TCP_TIMEOUT_SECS, ExternalTestbed};

pub(super) fn load_qemu_layout(workspace_root: &Path) -> Result<QemuTestbedLayout, String> {
    let path = qemu_testbed_layout_path(workspace_root);
    QemuTestbedLayout::load(path)
        .map_err(|error| format!("failed to load qemu testbed layout: {error}"))
}

pub(super) fn send_remote_api_request<T: ExternalTestbed>(
    layout: &T,
    host: &str,
    port: u16,
    request: &ApiRequest,
) -> Result<Vec<u8>, String> {
    let request_bytes = encode_request(request)
        .map_err(|error| format!("failed to encode api request: {error:?}"))?;
    run_remote_tcp_request(layout, host, port, &request_bytes)
}

pub(super) fn run_remote_tcp_request<T: ExternalTestbed>(
    layout: &T,
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    layout.run_remote_tcp_request(host, port, request_bytes)
}

pub(super) fn build_remote_tcp_probe_command(host: &str, port: u16, request_hex: &str) -> String {
    let request_hex = if request_hex.is_empty() {
        String::from("-")
    } else {
        request_hex.to_string()
    };
    format!(
        "python3 - {host} {port} {request_hex} {EXTERNAL_REMOTE_TCP_TIMEOUT_SECS} <<'PY'\nimport socket, sys\nhost = sys.argv[1]\nport = int(sys.argv[2])\npayload = b'' if sys.argv[3] == '-' else bytes.fromhex(sys.argv[3])\ntimeout_secs = float(sys.argv[4])\nwith socket.create_connection((host, port), timeout=timeout_secs) as stream:\n    stream.settimeout(timeout_secs)\n    if payload:\n        stream.sendall(payload)\n    stream.shutdown(socket.SHUT_WR)\n    chunks = []\n    while True:\n        chunk = stream.recv(4096)\n        if not chunk:\n            break\n        chunks.append(chunk)\n    sys.stdout.buffer.write(b''.join(chunks))\nPY"
    )
}

pub(super) fn validate_protocol_probe_response(
    backend_name: &str,
    replica_id: ReplicaId,
    response_bytes: &[u8],
) -> Result<(), String> {
    let protocol_text = String::from_utf8_lossy(response_bytes);
    if protocol_text.contains("protocol transport not implemented")
        || protocol_text.contains("network isolated by local harness")
    {
        return Err(format!(
            "{} protocol surface is not ready for Jepsen: replica {} returned `{protocol_text}`",
            backend_name,
            replica_id.get()
        ));
    }
    Ok(())
}

pub(super) fn extract_probe_commit_lsn(
    backend_name: &str,
    response: ApiResponse,
) -> Result<Lsn, String> {
    match response {
        ApiResponse::Submit(SubmitResponse::Committed(result)) => Ok(result.applied_lsn),
        other => Err(format!(
            "{backend_name} client surface is not ready for Jepsen: primary submit probe did not commit: {other:?}"
        )),
    }
}

pub(super) fn validate_probe_read_response(
    backend_name: &str,
    resource_id: u128,
    response: &ApiResponse,
) -> Result<(), String> {
    if matches!(
        response,
        ApiResponse::GetResource(ResourceResponse::Found(resource))
            if resource.resource_id == ResourceId(resource_id)
    ) {
        Ok(())
    } else {
        Err(format!(
            "{backend_name} client surface is not ready for Jepsen: primary read probe returned unexpected response {response:?}"
        ))
    }
}

pub(super) fn decode_external_api_response<T: ExternalTestbed>(
    layout: &T,
    bytes: &[u8],
) -> Result<ApiResponse, String> {
    decode_response(bytes).map_err(|error| {
        let text = String::from_utf8_lossy(bytes);
        format!(
            "invalid {} api response: {error:?}; raw_response={text}",
            layout.backend_name()
        )
    })
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub(super) fn sanitize_run_id(run_id: &str) -> String {
    let mut sanitized = String::new();
    for character in run_id.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
            sanitized.push(character);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        String::from("run")
    } else {
        sanitized
    }
}

pub(super) fn run_qemu_remote_host_command(
    layout: &QemuTestbedLayout,
    remote_command: &str,
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let mut remote_args = qemu_ssh_args(layout);
    remote_args.push(String::from(remote_command));

    let output = if let Some(stdin_bytes) = stdin_bytes {
        let mut child = Command::new("ssh")
            .args(&remote_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("failed to spawn qemu remote ssh command: {error}"))?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| String::from("qemu remote ssh stdin was unavailable"))?
            .write_all(stdin_bytes)
            .map_err(|error| format!("failed to write qemu remote ssh stdin: {error}"))?;
        child
            .wait_with_output()
            .map_err(|error| format!("failed to wait for qemu remote ssh command: {error}"))?
    } else {
        Command::new("ssh")
            .args(&remote_args)
            .output()
            .map_err(|error| format!("failed to run qemu remote ssh command: {error}"))?
    };

    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "qemu remote command failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

pub(super) fn run_qemu_remote_tcp_request(
    layout: &QemuTestbedLayout,
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let request_hex = encode_hex(request_bytes);
    run_qemu_remote_host_command(
        layout,
        &build_remote_tcp_probe_command(host, port, &request_hex),
        None,
    )
}

fn qemu_ssh_args(layout: &QemuTestbedLayout) -> Vec<String> {
    vec![
        String::from("-i"),
        layout.ssh_private_key_path().display().to_string(),
        String::from("-o"),
        String::from("BatchMode=yes"),
        String::from("-o"),
        String::from("ConnectTimeout=5"),
        String::from("-o"),
        String::from("LogLevel=ERROR"),
        String::from("-o"),
        String::from("StrictHostKeyChecking=no"),
        String::from("-o"),
        String::from("UserKnownHostsFile=/dev/null"),
        String::from("-p"),
        CONTROL_HOST_SSH_PORT.to_string(),
        String::from("allocdb@127.0.0.1"),
    ]
}
