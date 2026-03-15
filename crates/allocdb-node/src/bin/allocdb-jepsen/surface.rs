use std::path::Path;

use allocdb_core::ids::ResourceId;
use allocdb_node::jepsen::{
    analyze_history, load_history, release_gate_plan, render_analysis_report,
};
use allocdb_node::kubevirt_testbed::KubevirtTestbedLayout;
use allocdb_node::local_cluster::LocalClusterLayout;
use allocdb_node::qemu_testbed::QemuTestbedLayout;
use allocdb_node::{ApiRequest, ApiResponse, MetricsRequest, ResourceRequest, SubmitRequest};

use super::cluster::{ensure_runtime_cluster_ready, primary_replica};
use super::events::probe_create_request;
use super::kubevirt::{
    load_kubevirt_layout, prepare_kubevirt_helper, run_kubevirt_remote_host_command,
    run_kubevirt_remote_tcp_request,
};
use super::remote::{
    decode_external_api_response, extract_probe_commit_lsn, load_qemu_layout,
    run_qemu_remote_host_command, run_qemu_remote_tcp_request, run_remote_tcp_request,
    send_remote_api_request, validate_probe_read_response, validate_protocol_probe_response,
};
use super::tracker::RequestNamespace;
use super::unique_probe_resource_id;

pub(super) trait ExternalTestbed {
    fn backend_name(&self) -> &'static str;
    fn workspace_root(&self) -> &Path;
    fn replica_layout(&self) -> &LocalClusterLayout;
    fn run_remote_tcp_request(
        &self,
        host: &str,
        port: u16,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, String>;
    fn run_remote_host_command(
        &self,
        remote_command: &str,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String>;
}

impl ExternalTestbed for QemuTestbedLayout {
    fn backend_name(&self) -> &'static str {
        "qemu"
    }

    fn workspace_root(&self) -> &Path {
        &self.config.workspace_root
    }

    fn replica_layout(&self) -> &LocalClusterLayout {
        &self.replica_layout
    }

    fn run_remote_tcp_request(
        &self,
        host: &str,
        port: u16,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, String> {
        run_qemu_remote_tcp_request(self, host, port, request_bytes)
    }

    fn run_remote_host_command(
        &self,
        remote_command: &str,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        run_qemu_remote_host_command(self, remote_command, stdin_bytes)
    }
}

impl ExternalTestbed for KubevirtTestbedLayout {
    fn backend_name(&self) -> &'static str {
        "kubevirt"
    }

    fn workspace_root(&self) -> &Path {
        &self.config.workspace_root
    }

    fn replica_layout(&self) -> &LocalClusterLayout {
        &self.replica_layout
    }

    fn run_remote_tcp_request(
        &self,
        host: &str,
        port: u16,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, String> {
        run_kubevirt_remote_tcp_request(self, host, port, request_bytes)
    }

    fn run_remote_host_command(
        &self,
        remote_command: &str,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        run_kubevirt_remote_host_command(self, remote_command, stdin_bytes)
    }
}

pub(super) fn print_release_gate_plan() {
    for run in release_gate_plan() {
        println!(
            "run_id={} workload={} nemesis={} minimum_fault_window_secs={} release_blocking={}",
            run.run_id,
            run.workload.as_str(),
            run.nemesis.as_str(),
            run.minimum_fault_window_secs
                .map_or(String::from("none"), |secs| secs.to_string()),
            run.release_blocking
        );
    }
}

pub(super) fn analyze_history_file(history_file: &Path) -> Result<(), String> {
    let history =
        load_history(history_file).map_err(|error| format!("failed to load history: {error}"))?;
    let report = analyze_history(&history);
    print!("{}", render_analysis_report(&report));
    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

pub(super) fn verify_qemu_surface(workspace_root: &Path) -> Result<(), String> {
    let layout = load_qemu_layout(workspace_root)?;
    verify_external_surface(&layout)?;
    println!("qemu_surface=ready");
    Ok(())
}

pub(super) fn verify_kubevirt_surface(workspace_root: &Path) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    verify_external_surface(&layout)?;
    println!("kubevirt_surface=ready");
    Ok(())
}

pub(super) fn verify_external_surface<T: ExternalTestbed>(layout: &T) -> Result<(), String> {
    ensure_runtime_cluster_ready(layout)?;
    let probe_namespace = RequestNamespace::new();
    for replica in &layout.replica_layout().replicas {
        let response_bytes = send_remote_api_request(
            layout,
            &replica.client_addr.ip().to_string(),
            replica.client_addr.port(),
            &ApiRequest::GetMetrics(MetricsRequest {
                current_wall_clock_slot: probe_namespace.slot(0),
            }),
        )?;
        let response = decode_external_api_response(layout, &response_bytes)?;
        if !matches!(response, ApiResponse::GetMetrics(_)) {
            return Err(format!(
                "{} client surface is not ready for Jepsen: replica {} returned unexpected metrics probe response {response:?}",
                layout.backend_name(),
                replica.replica_id.get()
            ));
        }

        let protocol_response = run_remote_tcp_request(
            layout,
            &replica.protocol_addr.ip().to_string(),
            replica.protocol_addr.port(),
            &[],
        )?;
        validate_protocol_probe_response(
            layout.backend_name(),
            replica.replica_id,
            &protocol_response,
        )?;
    }

    let primary = primary_replica(layout)?;
    let primary_host = primary.client_addr.ip().to_string();
    let resource_id = unique_probe_resource_id();
    let submit_response = decode_external_api_response(
        layout,
        &send_remote_api_request(
            layout,
            &primary_host,
            primary.client_addr.port(),
            &ApiRequest::Submit(SubmitRequest::from_client_request(
                probe_namespace.slot(1),
                probe_create_request(resource_id, probe_namespace),
            )),
        )?,
    )?;
    let applied_lsn = extract_probe_commit_lsn(layout.backend_name(), submit_response)?;

    let read_response = decode_external_api_response(
        layout,
        &send_remote_api_request(
            layout,
            &primary_host,
            primary.client_addr.port(),
            &ApiRequest::GetResource(ResourceRequest {
                resource_id: ResourceId(resource_id),
                required_lsn: Some(applied_lsn),
            }),
        )?,
    )?;
    validate_probe_read_response(layout.backend_name(), resource_id, &read_response)?;
    Ok(())
}
