// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, HashMap};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::Client;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use spur_proto::proto::slurm_controller_client::SlurmControllerClient;
use spur_proto::proto::{HeartbeatRequest, RegisterAgentRequest, RunningJobStatus};

// Matches spurctld's check_node_health(90) timeout and spurd's 30 s interval.
const INTERVAL_SECS: u64 = 30;

/// Tracks the set of active K8s nodes and sends periodic `Heartbeat` RPCs
/// to spurctld on their behalf, mirroring what `spurd`'s `reporter::heartbeat_loop`
/// does for native-host nodes.
///
/// `node_watcher` holds an `Arc<HeartbeatManager>` and calls `track`/`untrack`
/// as nodes appear and disappear; the heartbeat task calls `run` under
/// `retry::run_with_retry`.
pub struct HeartbeatManager {
    registry: RwLock<HashMap<String, RegisterAgentRequest>>,
    controller_addr: String,
    client: Client,
}

impl HeartbeatManager {
    pub fn new(controller_addr: String, client: Client) -> Self {
        Self {
            registry: RwLock::new(HashMap::new()),
            controller_addr,
            client,
        }
    }

    /// Add or update a node in the tracked set.
    pub async fn track(&self, name: String, req: RegisterAgentRequest) {
        self.registry.write().await.insert(name, req);
    }

    /// Remove a node from the tracked set. Safe to call for unknown names.
    pub async fn untrack(&self, name: &str) {
        self.registry.write().await.remove(name);
    }

    async fn running_jobs_by_node(&self) -> anyhow::Result<HashMap<String, Vec<RunningJobStatus>>> {
        let pods: Api<Pod> = Api::all(self.client.clone());
        let listed = pods
            .list(&ListParams::default().labels("spur.amd.com/managed-by=spur-k8s-operator"))
            .await?;
        let mut allocations: HashMap<String, BTreeSet<(u32, u32)>> = HashMap::new();

        for pod in listed {
            let pod_name = pod.metadata.name.as_deref().unwrap_or("<unknown>");
            let labels = pod
                .metadata
                .labels
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("managed Pod {pod_name} has no labels"))?;
            let job_id = labels
                .get("spur.amd.com/job-id")
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| anyhow::anyhow!("managed Pod {pod_name} has no valid job id"))?;
            let run_attempt = labels
                .get("spur.amd.com/run-attempt")
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| {
                    anyhow::anyhow!("managed Pod {pod_name} has no valid run attempt")
                })?;
            let node = pod
                .spec
                .as_ref()
                .and_then(|spec| spec.node_name.as_ref())
                .filter(|name| !name.is_empty())
                .or_else(|| labels.get("spur.amd.com/target-node"));
            let node = node.ok_or_else(|| {
                anyhow::anyhow!("managed Pod {pod_name} has no assigned or target node")
            })?;

            // A deleting Pod still proves cleanup is incomplete until Kubernetes removes it.
            allocations
                .entry(node.clone())
                .or_default()
                .insert((job_id, run_attempt));
        }

        Ok(allocations
            .into_iter()
            .map(|(node, jobs)| {
                let jobs = jobs
                    .into_iter()
                    .map(|(job_id, run_attempt)| RunningJobStatus {
                        job_id,
                        run_attempt,
                        ..Default::default()
                    })
                    .collect();
                (node, jobs)
            })
            .collect())
    }

    /// Send `Heartbeat` RPCs to spurctld for every tracked node.
    pub async fn run(&self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(INTERVAL_SECS));
        loop {
            interval.tick().await;

            let names: Vec<String> = self.registry.read().await.keys().cloned().collect();
            if names.is_empty() {
                continue;
            }

            let running_jobs = self.running_jobs_by_node().await?;

            match connect(&self.controller_addr).await {
                Ok(mut client) => {
                    for name in &names {
                        let req = HeartbeatRequest {
                            hostname: name.clone(),
                            cpu_load: 0,
                            free_memory_mb: 0,
                            running_jobs: running_jobs.get(name).cloned().unwrap_or_default(),
                            node_token: String::new(),
                            wg_pubkey: String::new(), // virtual agents are not on the mesh
                            agent_session_id: String::new(),
                            node_boot_id: String::new(),
                            allocation_inventory: Vec::new(),
                            recovery_complete: true,
                            supports_command_polling: false,
                            supports_attempt_inventory: true,
                        };
                        match client.heartbeat(req).await {
                            Ok(_) => debug!(node = %name, "heartbeat sent"),
                            Err(e) => warn!(node = %name, error = %e, "heartbeat failed"),
                        }
                    }
                }
                Err(e) => warn!(error = %e, "heartbeat: failed to connect to spurctld"),
            }
        }
    }
}

async fn connect(addr: &str) -> anyhow::Result<SlurmControllerClient<tonic::transport::Channel>> {
    let url = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };
    Ok(SlurmControllerClient::connect(url)
        .await?
        .max_decoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, Request, Response, StatusCode};
    use kube::client::Body;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex as StdMutex};
    use tower::service_fn;

    type SeenRequests = Arc<StdMutex<Vec<(Method, String)>>>;

    fn mock_kube_client<F>(respond: F) -> (Client, SeenRequests)
    where
        F: Fn(&Method, &str) -> (StatusCode, serde_json::Value) + Send + Sync + 'static,
    {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let service_seen = seen.clone();
        let respond = Arc::new(respond);
        let service = service_fn(move |request: Request<Body>| {
            let method = request.method().clone();
            let uri = request.uri().to_string();
            service_seen
                .lock()
                .expect("request recorder poisoned")
                .push((method.clone(), uri.clone()));
            let (status, payload) = respond(&method, &uri);
            async move {
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&payload).expect("serialize mock response"),
                        ))
                        .expect("build mock response"),
                )
            }
        });
        (Client::new(service, "default"), seen)
    }

    fn heartbeat_manager() -> HeartbeatManager {
        let (client, _) = mock_kube_client(|_, _| {
            (
                StatusCode::OK,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": {},
                    "items": []
                }),
            )
        });
        HeartbeatManager::new("http://localhost:6817".into(), client)
    }

    fn make_req(hostname: &str) -> RegisterAgentRequest {
        RegisterAgentRequest {
            hostname: hostname.into(),
            resources: None,
            version: "test".into(),
            address: "127.0.0.1".into(),
            port: 6818,
            wg_pubkey: String::new(),
            labels: std::collections::HashMap::new(),
            join_token: String::new(),
            agent_session_id: String::new(),
            node_boot_id: String::new(),
            allocation_inventory: Vec::new(),
            recovery_complete: true,
            supports_command_polling: false,
            supports_attempt_inventory: true,
        }
    }

    #[tokio::test]
    async fn test_new_registry_is_empty() {
        let hb = heartbeat_manager();
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_track_adds_node() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;
        assert!(hb.registry.read().await.contains_key("node-1"));
    }

    #[tokio::test]
    async fn test_untrack_removes_node() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.untrack("node-1").await;
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_untrack_unknown_name_is_safe() {
        let hb = heartbeat_manager();
        hb.untrack("does-not-exist").await;
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_track_idempotent_updates_entry() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;

        let mut updated = make_req("node-1");
        updated.address = "10.0.0.1".into();
        hb.track("node-1".into(), updated).await;

        let guard = hb.registry.read().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard["node-1"].address, "10.0.0.1");
    }

    #[tokio::test]
    async fn test_multiple_nodes_tracked_independently() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        hb.track("node-3".into(), make_req("node-3")).await;
        assert_eq!(hb.registry.read().await.len(), 3);
    }

    #[tokio::test]
    async fn test_untrack_one_of_many_leaves_others() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        hb.untrack("node-1").await;

        let guard = hb.registry.read().await;
        assert_eq!(guard.len(), 1);
        assert!(!guard.contains_key("node-1"));
        assert!(guard.contains_key("node-2"));
    }

    #[tokio::test]
    async fn test_track_after_untrack_re_adds_node() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.untrack("node-1").await;
        hb.track("node-1".into(), make_req("node-1")).await;

        assert_eq!(hb.registry.read().await.len(), 1);
    }

    #[tokio::test]
    async fn test_untrack_all_leaves_empty_registry() {
        let hb = heartbeat_manager();
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        hb.untrack("node-1").await;
        hb.untrack("node-2").await;
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_register_req_preserved_for_reregistration() {
        let hb = heartbeat_manager();
        let req = make_req("node-1");
        hb.track("node-1".into(), req.clone()).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        hb.untrack("node-2").await;

        let guard = hb.registry.read().await;
        let stored = guard.get("node-1").expect("node-1 must still be tracked");
        assert_eq!(stored.hostname, req.hostname);
        assert_eq!(stored.address, req.address);
        assert_eq!(stored.port, req.port);
    }

    #[tokio::test]
    async fn heartbeat_inventory_comes_from_live_pods() {
        let (client, seen) = mock_kube_client(|_, _| {
            (
                StatusCode::OK,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": {},
                    "items": [
                        {
                            "metadata": {
                                "name": "spur-job-17-a2-node-1",
                                "labels": {
                                    "spur.amd.com/job-id": "17",
                                    "spur.amd.com/run-attempt": "2"
                                }
                            },
                            "spec": {"containers": [], "nodeName": "node-1"}
                        },
                        {
                            "metadata": {
                                "name": "spur-job-17-a3-node-1",
                                "labels": {
                                    "spur.amd.com/job-id": "17",
                                    "spur.amd.com/run-attempt": "3"
                                }
                            },
                            "spec": {"containers": [], "nodeName": "node-1"}
                        },
                        {
                            "metadata": {
                                "name": "spur-job-18-a1-node-2",
                                "labels": {
                                    "spur.amd.com/job-id": "18",
                                    "spur.amd.com/run-attempt": "1",
                                    "spur.amd.com/target-node": "node-2"
                                }
                            },
                            "spec": {"containers": []}
                        },
                        {
                            "metadata": {
                                "name": "spur-job-19-a4-node-3",
                                "deletionTimestamp": "2026-08-09T00:00:00Z",
                                "finalizers": ["example.test/slow-delete"],
                                "labels": {
                                    "spur.amd.com/job-id": "19",
                                    "spur.amd.com/run-attempt": "4"
                                }
                            },
                            "spec": {"containers": [], "nodeName": "node-3"}
                        }
                    ]
                }),
            )
        });
        let hb = HeartbeatManager::new("http://localhost:6817".into(), client);

        let inventory = hb.running_jobs_by_node().await.unwrap();

        assert_eq!(inventory["node-1"][0].job_id, 17);
        assert_eq!(inventory["node-1"][0].run_attempt, 2);
        assert_eq!(inventory["node-1"][1].job_id, 17);
        assert_eq!(inventory["node-1"][1].run_attempt, 3);
        assert_eq!(inventory["node-2"][0].job_id, 18);
        assert_eq!(inventory["node-2"][0].run_attempt, 1);
        assert_eq!(inventory["node-3"][0].job_id, 19);
        assert_eq!(inventory["node-3"][0].run_attempt, 4);
        let requests = seen.lock().expect("request recorder poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, Method::GET);
        assert!(requests[0]
            .1
            .contains("spur.amd.com%2Fmanaged-by%3Dspur-k8s-operator"));
    }

    #[tokio::test]
    async fn pod_inventory_failure_is_not_an_empty_inventory() {
        let (client, _) = mock_kube_client(|_, _| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "reason": "ServiceUnavailable",
                    "code": 503
                }),
            )
        });
        let hb = HeartbeatManager::new("http://localhost:6817".into(), client);

        assert!(hb.running_jobs_by_node().await.is_err());
    }

    #[tokio::test]
    async fn pod_without_an_exact_attempt_is_not_an_empty_inventory() {
        let (client, _) = mock_kube_client(|_, _| {
            (
                StatusCode::OK,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": {},
                    "items": [{
                        "metadata": {
                            "name": "spur-job-17-node-1",
                            "labels": {"spur.amd.com/job-id": "17"}
                        },
                        "spec": {"containers": [], "nodeName": "node-1"}
                    }]
                }),
            )
        });
        let hb = HeartbeatManager::new("http://localhost:6817".into(), client);

        assert!(hb.running_jobs_by_node().await.is_err());
    }
}
