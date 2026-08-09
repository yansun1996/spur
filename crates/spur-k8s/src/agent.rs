// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, HostPathVolumeSource, Pod, PodSpec, ResourceRequirements, Service,
    ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, AttachParams, DeleteParams, ListParams, ObjectMeta, PostParams};
use kube::Client;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::crd::SpurJob;
use spur_core::spur_env::SpurEnv;
use spur_proto::proto::slurm_agent_server::SlurmAgent;
use spur_proto::proto::*;

const NS_LOOKUP_BUDGET: Duration = Duration::from_secs(5);

/// Rejects a term older than the highest seen. 0 (legacy/unset) passes only
/// before any real term has been observed — once fencing starts, it can't stop.
struct TermFence(std::sync::atomic::AtomicU64);

impl TermFence {
    fn new() -> Self {
        Self(std::sync::atomic::AtomicU64::new(0))
    }

    fn check(&self, term: u64) -> Result<(), Status> {
        use std::sync::atomic::Ordering;
        if term == 0 {
            return if self.0.load(Ordering::Acquire) == 0 {
                Ok(())
            } else {
                Err(Status::failed_precondition(
                    "unfenced request rejected after a real controller term was observed",
                ))
            };
        }
        let prev = self.0.fetch_max(term, Ordering::AcqRel);
        if term < prev {
            return Err(Status::failed_precondition(format!(
                "stale controller term {term} (highest seen {prev}); a newer leader has taken over"
            )));
        }
        Ok(())
    }
}

/// Virtual SlurmAgent that creates K8s Pods instead of fork/exec.
pub struct VirtualAgent {
    client: Client,
    term_fence: TermFence,
}

impl VirtualAgent {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            term_fence: TermFence::new(),
        }
    }

    /// Look up the namespace of the SpurJob labeled `spur.amd.com/job-id=<id>`.
    /// Fails loudly if not found so pods are never placed in the wrong namespace.
    async fn resolve_namespace(&self, job_id: u32) -> Result<String, Status> {
        let api: Api<SpurJob> = Api::all(self.client.clone());
        let lp = ListParams::default().labels(&format!("spur.amd.com/job-id={}", job_id));

        let result = tokio::time::timeout(
            NS_LOOKUP_BUDGET,
            (|| async {
                let list = tokio::time::timeout(Duration::from_millis(300), api.list(&lp))
                    .await
                    .map_err(|_| Status::unavailable("k8s API timeout"))?
                    .map_err(|e| Status::internal(e.to_string()))?;

                list.items
                    .into_iter()
                    .next()
                    .and_then(|j| j.metadata.namespace)
                    .ok_or_else(|| {
                        Status::not_found(format!(
                            "spur.amd.com/job-id={job_id} label not yet visible"
                        ))
                    })
            })
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(200))
                    .with_max_delay(Duration::from_secs(2))
                    .without_max_times(),
            )
            .when(|e: &Status| {
                matches!(e.code(), tonic::Code::Unavailable | tonic::Code::NotFound)
            }),
        )
        .await;

        match result {
            Ok(Ok(ns)) => Ok(ns),
            Ok(Err(status)) => Err(status),
            Err(_elapsed) => Err(Status::deadline_exceeded(format!(
                "namespace lookup for spur.amd.com/job-id={job_id} timed out after {}s",
                NS_LOOKUP_BUDGET.as_secs()
            ))),
        }
    }

    async fn resolve_current_pod(&self, namespace: &str, job_id: u32) -> Result<String, Status> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let listed = pods
            .list(&ListParams::default().labels(&allocation_selector(job_id, 0)))
            .await
            .map_err(|error| Status::internal(format!("pod lookup failed: {error}")))?;
        current_attempt_pod_name(job_id, &listed.items)
    }

    async fn delete_allocation_resources(
        &self,
        namespace: &str,
        job_id: u32,
        run_attempt: u32,
    ) -> Result<(), Status> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let params = ListParams::default().labels(&allocation_selector(job_id, run_attempt));
        let pod_list = pods.list(&params).await.map_err(|error| {
            Status::internal(format!("failed to list Pods for cancellation: {error}"))
        })?;
        for pod in pod_list {
            let Some(name) = pod.metadata.name else {
                continue;
            };
            match pods.delete(&name, &DeleteParams::default()).await {
                Ok(_) => info!(job_id, run_attempt, pod = %name, "deleted Pod"),
                Err(kube::Error::Api(error)) if error.code == 404 => {
                    debug!(job_id, run_attempt, pod = %name, "Pod already gone");
                }
                Err(error) => {
                    return Err(Status::internal(format!(
                        "failed to delete Pod {name}: {error}"
                    )));
                }
            }
        }

        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let service_names = if run_attempt == 0 {
            services
                .list(&params)
                .await
                .map_err(|error| {
                    Status::internal(format!("failed to list Services for cancellation: {error}"))
                })?
                .into_iter()
                .filter_map(|service| service.metadata.name)
                .collect()
        } else {
            vec![job_resource_name(job_id, run_attempt)]
        };
        for service_name in service_names {
            match services
                .delete(&service_name, &DeleteParams::default())
                .await
            {
                Ok(_) => {
                    debug!(job_id, run_attempt, service = %service_name, "deleted headless Service")
                }
                Err(kube::Error::Api(error)) if error.code == 404 => {}
                Err(error) => {
                    return Err(Status::internal(format!(
                        "failed to delete headless Service {service_name}: {error}"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl SlurmAgent for VirtualAgent {
    type StreamJobOutputStream =
        tokio_stream::wrappers::ReceiverStream<Result<StreamJobOutputChunk, Status>>;
    type InteractiveSessionStream =
        tokio_stream::wrappers::ReceiverStream<Result<InteractiveOutput, Status>>;

    async fn launch_job(
        &self,
        request: Request<LaunchJobRequest>,
    ) -> Result<Response<LaunchJobResponse>, Status> {
        let req = request.into_inner();
        self.term_fence.check(req.term)?;
        let job_id = req.job_id;
        let run_attempt = req.run_attempt;
        let ns = self.resolve_namespace(job_id).await?;
        let target_node = req.target_node.clone();
        let peer_nodes = &req.peer_nodes;
        let num_peers = peer_nodes.len();

        let spec = req
            .spec
            .ok_or_else(|| Status::invalid_argument("missing job spec"))?;

        let pod_name = pod_name(job_id, run_attempt, &target_node);
        let service_name = job_resource_name(job_id, run_attempt);

        let image = if spec.container_image.is_empty() {
            "busybox:latest".to_string()
        } else {
            spec.container_image.clone()
        };

        // Build resource requests
        let mut resource_requests = BTreeMap::new();
        let mut resource_limits = BTreeMap::new();

        if let Some(ref alloc) = req.allocated {
            if alloc.cpus > 0 {
                let cpu_str = alloc.cpus.to_string();
                resource_requests.insert("cpu".to_string(), Quantity(cpu_str.clone()));
                resource_limits.insert("cpu".to_string(), Quantity(cpu_str));
            }
            if alloc.memory_mb > 0 {
                let mem_str = format!("{}Mi", alloc.memory_mb);
                resource_requests.insert("memory".to_string(), Quantity(mem_str.clone()));
                resource_limits.insert("memory".to_string(), Quantity(mem_str));
            }
            let gpu_count = alloc
                .devices
                .get("gpu")
                .map(|d| d.devices.len() as u32)
                .unwrap_or(0);
            if gpu_count > 0 {
                let gpu_str = gpu_count.to_string();
                let gpu_type = spec
                    .gres
                    .iter()
                    .find_map(|g| spur_core::resource::parse_gres(g))
                    .and_then(|(_, t, _)| t);
                let gpu_resource_key = gpu_vendor_resource_key(gpu_type.as_deref());
                resource_limits.insert(gpu_resource_key.to_string(), Quantity(gpu_str.clone()));
                resource_requests.insert(gpu_resource_key.to_string(), Quantity(gpu_str));
            }
        }

        // Compute node rank from task_offset.
        // Issue #69: peer_nodes contains addr:port strings (e.g., "10.0.0.1:6818")
        // while target_node is a hostname — starts_with matching never worked,
        // causing all pods to get rank 0. Instead, derive rank from task_offset
        // which is incremented per-node by the dispatcher.
        let tasks_per_node = spec.tasks_per_node.max(1);
        let node_rank = req.task_offset / tasks_per_node;

        // Build env vars via SpurEnv accumulator
        let mut senv = SpurEnv::new();
        senv.set_with_slurm_twin("SPUR_JOB_ID", job_id);
        senv.set_with_slurm_twin("SPUR_JOBID", job_id);
        senv.set_with_slurm_twin("SPUR_JOB_NAME", &spec.name);
        senv.set_with_slurm_twin("SPUR_JOB_PARTITION", &spec.partition);
        senv.set_with_slurm_twin("SPUR_JOB_ACCOUNT", &spec.account);
        senv.set_with_slurm_twin("SPUR_JOB_QOS", &spec.qos);
        senv.set_with_slurm_twin("SPUR_NNODES", num_peers);
        senv.set_with_slurm_twin("SPUR_JOB_NUM_NODES", num_peers);
        senv.set_with_slurm_twin("SPUR_NTASKS", spec.num_tasks);
        senv.set_with_slurm_twin("SPUR_NPROCS", spec.num_tasks);
        senv.set_with_slurm_twin("SPUR_CPUS_PER_TASK", spec.cpus_per_task);
        senv.set_with_slurm_twin(
            "SPUR_CPUS_ON_NODE",
            tasks_per_node * spec.cpus_per_task.max(1),
        );
        senv.set_with_slurm_twin("SPUR_TASKS_PER_NODE", tasks_per_node);
        senv.set_with_slurm_twin("SPUR_NODEID", node_rank);
        senv.set_with_slurm_twin("SPUR_NODELIST", &spec.nodelist);
        senv.set_with_slurm_twin("SPUR_JOB_NODELIST", &spec.nodelist);
        senv.set_with_slurm_twin("SPURD_NODENAME", &target_node);

        senv.set("SPUR_TASK_OFFSET", req.task_offset);
        senv.set("SPUR_NODE_RANK", node_rank);
        if !peer_nodes.is_empty() {
            senv.set("SPUR_PEER_NODES", peer_nodes.join(","));
        }
        if !target_node.is_empty() {
            senv.set("SPUR_TARGET_NODE", &target_node);
        }

        senv.set("LOCAL_RANK", "0");
        senv.set("LOCAL_WORLD_SIZE", tasks_per_node);
        senv.set("NPROC_PER_NODE", tasks_per_node);
        senv.set("NODE_RANK", node_rank);

        if num_peers > 1 {
            let master_addr = format!("{service_name}.{ns}.svc.cluster.local");
            senv.set("MASTER_ADDR", master_addr);
            senv.set("MASTER_PORT", "29500");
            senv.set("WORLD_SIZE", num_peers);
            senv.set("RANK", node_rank);
        }

        let mut env_vars: Vec<EnvVar> = senv
            .into_map()
            .into_iter()
            .map(|(name, value)| EnvVar {
                name,
                value: Some(value),
                ..Default::default()
            })
            .collect();

        // Set GPU vendor-specific env vars for the runtime
        let gpu_count = req
            .allocated
            .as_ref()
            .and_then(|a| a.devices.get("gpu"))
            .map(|d| d.devices.len())
            .unwrap_or(0);
        if gpu_count > 0 {
            let gpu_type = spec
                .gres
                .iter()
                .find_map(|g| spur_core::resource::parse_gres(g))
                .and_then(|(_, t, _)| t);
            if gpu_type.as_deref().is_none_or(|t| !is_nvidia_gpu(t)) {
                env_vars.push(EnvVar {
                    name: "GPU_ENABLE_PAL".into(),
                    value: Some("0".into()),
                    ..Default::default()
                });
                if num_peers > 1 {
                    env_vars.push(EnvVar {
                        name: "NCCL_SOCKET_IFNAME".into(),
                        value: Some("eth0".into()),
                        ..Default::default()
                    });
                }
            } else if num_peers > 1 {
                env_vars.push(EnvVar {
                    name: "NCCL_SOCKET_IFNAME".into(),
                    value: Some("eth0".into()),
                    ..Default::default()
                });
            }
        }

        for (k, v) in &spec.environment {
            env_vars.push(EnvVar {
                name: k.clone(),
                value: Some(v.clone()),
                ..Default::default()
            });
        }

        // Issue #117: Inject secret env vars from SpurJob CRD's secretEnv field.
        // These reference K8s Secrets and are injected as secretKeyRef, keeping
        // secret values out of the SpurJob spec and Raft log.
        {
            let api: kube::Api<crate::crd::SpurJob> = kube::Api::all(self.client.clone());
            let lp =
                kube::api::ListParams::default().labels(&format!("spur.amd.com/job-id={}", job_id));
            if let Ok(list) = api.list(&lp).await {
                if let Some(spurjob) = list.items.into_iter().next() {
                    for (env_name, secret_ref) in &spurjob.spec.secret_env {
                        if let Some((secret_name, secret_key)) = secret_ref.split_once('/') {
                            env_vars.push(EnvVar {
                                name: env_name.clone(),
                                value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                    secret_key_ref: Some(
                                        k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: secret_name.to_string(),
                                            key: secret_key.to_string(),
                                            optional: Some(true),
                                        },
                                    ),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        // Build command
        let command = if !spec.argv.is_empty() {
            Some(spec.argv.clone())
        } else if !spec.script.is_empty() {
            Some(vec!["sh".into(), "-c".into(), spec.script.clone()])
        } else {
            // Interactive session: keep pod alive so kube exec can attach a terminal
            Some(vec!["sleep".into(), "infinity".into()])
        };

        // Parse container_mounts → volumes + volume_mounts
        let (mut volumes, mut volume_mounts) = parse_mounts(&spec.container_mounts);

        // Set working_dir from work_dir or container_workdir
        let working_dir = if !spec.container_workdir.is_empty() {
            Some(spec.container_workdir.clone())
        } else if !spec.work_dir.is_empty() {
            Some(spec.work_dir.clone())
        } else {
            None
        };

        // Add extra device plugin resources (RDMA, MIG, etc.) — Issue #88
        for (key, val) in &spec.extra_resources {
            resource_requests.insert(key.clone(), Quantity(val.clone()));
            resource_limits.insert(key.clone(), Quantity(val.clone()));
        }

        // Shared memory volume mount — Issue #87
        if !spec.shm_size.is_empty() {
            volume_mounts.push(k8s_openapi::api::core::v1::VolumeMount {
                name: "dshm".into(),
                mount_path: "/dev/shm".into(),
                ..Default::default()
            });
        }

        // Privileged mode / SecurityContext — Issue #86
        let security_context = if spec.privileged {
            Some(k8s_openapi::api::core::v1::SecurityContext {
                privileged: Some(true),
                ..Default::default()
            })
        } else {
            None
        };

        let container = Container {
            name: "spur-job".into(),
            image: Some(image),
            command,
            env: Some(env_vars),
            working_dir,
            volume_mounts: if volume_mounts.is_empty() {
                None
            } else {
                Some(volume_mounts)
            },
            resources: Some(ResourceRequirements {
                requests: Some(resource_requests),
                limits: Some(resource_limits),
                ..Default::default()
            }),
            security_context,
            ..Default::default()
        };

        // Build labels
        let mut labels = BTreeMap::new();
        labels.insert("spur.amd.com/job-id".to_string(), job_id.to_string());
        labels.insert(
            "spur.amd.com/run-attempt".to_string(),
            run_attempt.to_string(),
        );
        labels.insert(
            "spur.amd.com/managed-by".to_string(),
            "spur-k8s-operator".to_string(),
        );
        if !spec.name.is_empty() {
            labels.insert("spur.amd.com/job-name".to_string(), spec.name.clone());
        }
        if !target_node.is_empty() {
            labels.insert("spur.amd.com/target-node".to_string(), target_node.clone());
        }

        // For multi-node jobs, create headless Service for DNS discovery
        if num_peers > 1 {
            if let Err(e) = self
                .ensure_headless_service(job_id, run_attempt, &labels, &ns)
                .await
            {
                warn!(job_id, error = %e, "failed to create headless service");
            }
        }

        // Pin to target K8s node
        let node_name = if !target_node.is_empty() {
            Some(target_node.clone())
        } else {
            peer_nodes.first().cloned()
        };

        // For headless service DNS: set hostname and subdomain
        let (hostname, subdomain) = if num_peers > 1 && !target_node.is_empty() {
            (Some(sanitize_k8s_name(&target_node)), Some(service_name))
        } else {
            (None, None)
        };

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some(ns.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some({
                // Shared memory emptyDir volume — Issue #87
                if !spec.shm_size.is_empty() {
                    volumes.push(k8s_openapi::api::core::v1::Volume {
                        name: "dshm".into(),
                        empty_dir: Some(k8s_openapi::api::core::v1::EmptyDirVolumeSource {
                            medium: Some("Memory".into()),
                            size_limit: Some(Quantity(spec.shm_size.clone())),
                        }),
                        ..Default::default()
                    });
                }

                PodSpec {
                    containers: vec![container],
                    restart_policy: Some("Never".into()),
                    node_name,
                    hostname,
                    subdomain,
                    volumes: if volumes.is_empty() {
                        None
                    } else {
                        Some(volumes)
                    },
                    // Issue #85: host_network
                    host_network: if spec.host_network { Some(true) } else { None },
                    // Issue #87: host_ipc
                    host_ipc: if spec.host_ipc { Some(true) } else { None },
                    ..Default::default()
                }
            }),
            ..Default::default()
        };

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);
        match pods.create(&PostParams::default(), &pod).await {
            Ok(_) => {
                info!(job_id, pod = %pod_name, namespace = %ns, target = %req.target_node, "K8s Pod created");
                Ok(Response::new(LaunchJobResponse {
                    success: true,
                    error: String::new(),
                    ..Default::default()
                }))
            }
            Err(kube::Error::Api(e)) if e.code == 409 => match pods.get(&pod_name).await {
                Ok(existing)
                    if metadata_matches_attempt(&existing.metadata, job_id, run_attempt) =>
                {
                    info!(job_id, run_attempt, pod = %pod_name, namespace = %ns, "K8s Pod already exists for this attempt");
                    Ok(Response::new(LaunchJobResponse {
                        success: true,
                        error: String::new(),
                        ..Default::default()
                    }))
                }
                Ok(_) => Ok(Response::new(LaunchJobResponse {
                    success: false,
                    error: "existing Pod belongs to a different run attempt".into(),
                    ..Default::default()
                })),
                Err(error) => Ok(Response::new(LaunchJobResponse {
                    success: false,
                    error: format!("failed to validate existing Pod: {error}"),
                    ..Default::default()
                })),
            },
            Err(e) => {
                error!(job_id, error = %e, "failed to create K8s Pod");
                Ok(Response::new(LaunchJobResponse {
                    success: false,
                    error: e.to_string(),
                    ..Default::default()
                }))
            }
        }
    }

    async fn prepare_pmix(
        &self,
        _request: Request<PreparePmixRequest>,
    ) -> Result<Response<PreparePmixResponse>, Status> {
        Err(Status::unimplemented(
            "PMIx prepare is not supported on the K8s virtual agent",
        ))
    }

    async fn release_pmix(
        &self,
        _request: Request<ReleasePmixRequest>,
    ) -> Result<Response<ReleasePmixResponse>, Status> {
        Ok(Response::new(ReleasePmixResponse {}))
    }

    async fn cancel_job(
        &self,
        request: Request<AgentCancelJobRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.term_fence.check(req.term)?;
        let job_id = req.job_id;
        let ns = self.resolve_namespace(job_id).await?;
        self.delete_allocation_resources(&ns, job_id, req.run_attempt)
            .await?;

        Ok(Response::new(()))
    }

    async fn suspend_job(
        &self,
        request: Request<AgentSuspendJobRequest>,
    ) -> Result<Response<()>, Status> {
        // Pod-level SIGSTOP/SIGCONT is not modeled for the k8s backend; the
        // controller-side state change still applies. Accept as a no-op.
        let req = request.into_inner();
        self.term_fence.check(req.term)?;
        debug!(
            job_id = req.job_id,
            resume = req.resume,
            "k8s backend: suspend/resume is a no-op"
        );
        Ok(Response::new(()))
    }

    async fn get_node_resources(
        &self,
        _request: Request<()>,
    ) -> Result<Response<NodeResourcesResponse>, Status> {
        Ok(Response::new(NodeResourcesResponse {
            total: Some(ResourceSet::default()),
            used: Some(spur_proto::proto::ResourceAllocations::default()),
        }))
    }

    async fn exec_in_job(
        &self,
        request: Request<ExecInJobRequest>,
    ) -> Result<Response<ExecInJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let ns = self.resolve_namespace(job_id).await?;
        let pod_name = self.resolve_current_pod(&ns, job_id).await?;
        let command: Vec<String> = if req.command.is_empty() {
            vec!["bash".into(), "-c".into(), "echo ok".into()]
        } else {
            req.command
        };

        debug!(pod = %pod_name, cmd = ?command, "exec in K8s pod");

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);

        let attach = AttachParams {
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
            container: None,
            max_stdin_buf_size: None,
            max_stdout_buf_size: Some(1024 * 1024),
            max_stderr_buf_size: Some(1024 * 1024),
        };

        let mut exec = pods
            .exec(&pod_name, command, &attach)
            .await
            .map_err(|e| Status::internal(format!("exec failed: {e}")))?;

        let mut stdout_data = Vec::new();
        let mut stderr_data = Vec::new();

        if let Some(mut stdout) = exec.stdout() {
            let _ = stdout.read_to_end(&mut stdout_data).await;
        }
        if let Some(mut stderr) = exec.stderr() {
            let _ = stderr.read_to_end(&mut stderr_data).await;
        }

        let status = exec
            .take_status()
            .ok_or_else(|| Status::internal("no exit status"))?
            .await
            .ok_or_else(|| Status::internal("status channel closed"))?;

        let exit_code = status
            .status
            .as_deref()
            .map(|s| if s == "Success" { 0 } else { 1 })
            .unwrap_or(1);

        Ok(Response::new(ExecInJobResponse {
            success: exit_code == 0,
            exit_code,
            stdout: String::from_utf8_lossy(&stdout_data).into_owned(),
            stderr: String::from_utf8_lossy(&stderr_data).into_owned(),
        }))
    }

    async fn run_command(
        &self,
        _request: Request<RunCommandRequest>,
    ) -> Result<Response<RunCommandResponse>, Status> {
        // Srun step dispatch. The K8s virtual agent does not currently support
        // one-shot commands outside the job pod's lifecycle — salloc plus
        // srun-in-allocation is not a common K8s workflow.
        Err(Status::unimplemented(
            "RunCommand is not yet supported by the K8s virtual agent",
        ))
    }

    async fn cancel_step(
        &self,
        _request: Request<CancelStepRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "CancelStep is not yet supported by the K8s virtual agent",
        ))
    }

    async fn register_job_allocation(
        &self,
        _request: Request<RegisterJobAllocationRequest>,
    ) -> Result<Response<RegisterJobAllocationResponse>, Status> {
        Err(Status::unimplemented(
            "RegisterJobAllocation is not yet supported by the K8s virtual agent",
        ))
    }

    async fn stream_job_output(
        &self,
        request: Request<StreamJobOutputRequest>,
    ) -> Result<Response<Self::StreamJobOutputStream>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let ns = self.resolve_namespace(job_id).await?;
        let pod_name = self.resolve_current_pod(&ns, job_id).await?;

        debug!(pod = %pod_name, "streaming logs from K8s pod");

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);
        let log_params = kube::api::LogParams {
            follow: true,
            tail_lines: Some(100),
            ..Default::default()
        };

        let log_stream = pods
            .log_stream(&pod_name, &log_params)
            .await
            .map_err(|e| Status::internal(format!("log stream failed: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            use futures_util::AsyncReadExt;
            let mut reader = log_stream;
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if tx
                            .send(Ok(StreamJobOutputChunk {
                                data: buf[..n].to_vec(),
                                eof: false,
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = tx
                .send(Ok(StreamJobOutputChunk {
                    data: Vec::new(),
                    eof: true,
                }))
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn interactive_session(
        &self,
        _request: Request<tonic::Streaming<InteractiveInput>>,
    ) -> Result<Response<Self::InteractiveSessionStream>, Status> {
        Err(Status::unimplemented(
            "interactive session not supported for K8s agent",
        ))
    }

    // -- Native cluster component control. The virtual K8s agent does not run k0s
    //    systemd units, so these are permanently unsupported here. --
    async fn start_cluster_component(
        &self,
        _request: Request<StartClusterComponentRequest>,
    ) -> Result<Response<StartClusterComponentResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn stop_cluster_component(
        &self,
        _request: Request<StopClusterComponentRequest>,
    ) -> Result<Response<StopClusterComponentResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn get_cluster_component_status(
        &self,
        _request: Request<GetClusterComponentStatusRequest>,
    ) -> Result<Response<GetClusterComponentStatusResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn create_k0s_join_token(
        &self,
        _request: Request<CreateK0sJoinTokenRequest>,
    ) -> Result<Response<CreateK0sJoinTokenResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn get_kubeconfig(
        &self,
        _request: Request<GetKubeconfigRequest>,
    ) -> Result<Response<GetKubeconfigResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn apply_mesh(
        &self,
        _request: Request<MeshMembership>,
    ) -> Result<Response<ApplyMeshResponse>, Status> {
        Err(Status::unimplemented("mesh not supported for K8s agent"))
    }
}

impl VirtualAgent {
    /// Create a headless Service for inter-pod DNS discovery in multi-node jobs.
    async fn ensure_headless_service(
        &self,
        job_id: u32,
        run_attempt: u32,
        labels: &BTreeMap<String, String>,
        namespace: &str,
    ) -> Result<(), kube::Error> {
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let svc_name = job_resource_name(job_id, run_attempt);
        let mut selector =
            BTreeMap::from([("spur.amd.com/job-id".to_string(), job_id.to_string())]);
        if run_attempt != 0 {
            selector.insert(
                "spur.amd.com/run-attempt".to_string(),
                run_attempt.to_string(),
            );
        }

        let svc = Service {
            metadata: ObjectMeta {
                name: Some(svc_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: Some("None".into()), // headless
                selector: Some(selector),
                ports: Some(vec![ServicePort {
                    name: Some("nccl".into()),
                    port: 29500,
                    target_port: Some(IntOrString::Int(29500)),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        match services.create(&PostParams::default(), &svc).await {
            Ok(_) => {
                info!(job_id, svc = %svc_name, "headless Service created");
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                let existing = services.get(&svc_name).await?;
                if metadata_matches_attempt(&existing.metadata, job_id, run_attempt) {
                    debug!(job_id, run_attempt, "headless Service already exists");
                    Ok(())
                } else {
                    Err(kube::Error::Api(e))
                }
            }
            Err(e) => Err(e),
        }
    }
}

/// Parse container_mounts ("/src:/dst:ro" or "pvc:name:/dst") into K8s volumes + mounts.
fn parse_mounts(mounts: &[String]) -> (Vec<Volume>, Vec<VolumeMount>) {
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    for (i, mount_str) in mounts.iter().enumerate() {
        let parts: Vec<&str> = mount_str.split(':').collect();

        if parts.len() >= 2 && parts[0] == "pvc" {
            // PVC mount: "pvc:claim-name:/dst"
            if parts.len() >= 3 {
                let vol_name = format!("pvc-{}", i);
                volumes.push(Volume {
                    name: vol_name.clone(),
                    persistent_volume_claim: Some(
                        k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                            claim_name: parts[1].to_string(),
                            read_only: Some(parts.get(3).is_some_and(|&v| v == "ro")),
                        },
                    ),
                    ..Default::default()
                });
                volume_mounts.push(VolumeMount {
                    name: vol_name,
                    mount_path: parts[2].to_string(),
                    read_only: Some(parts.get(3).is_some_and(|&v| v == "ro")),
                    ..Default::default()
                });
            }
        } else if parts.len() >= 2 {
            // hostPath mount: "/src:/dst[:ro]"
            let vol_name = format!("hostpath-{}", i);
            let read_only = parts.get(2).is_some_and(|&v| v == "ro");
            volumes.push(Volume {
                name: vol_name.clone(),
                host_path: Some(HostPathVolumeSource {
                    path: parts[0].to_string(),
                    type_: Some("DirectoryOrCreate".into()),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: vol_name,
                mount_path: parts[1].to_string(),
                read_only: Some(read_only),
                ..Default::default()
            });
        }
    }

    (volumes, volume_mounts)
}

/// Sanitize a string for use in K8s resource names.
fn job_resource_name(job_id: u32, run_attempt: u32) -> String {
    if run_attempt == 0 {
        format!("spur-job-{job_id}")
    } else {
        format!("spur-job-{job_id}-a{run_attempt}")
    }
}

fn pod_name(job_id: u32, run_attempt: u32, target_node: &str) -> String {
    let base = job_resource_name(job_id, run_attempt);
    if target_node.is_empty() {
        base
    } else {
        format!("{base}-{}", sanitize_k8s_name(target_node))
    }
}

fn allocation_selector(job_id: u32, run_attempt: u32) -> String {
    if run_attempt == 0 {
        format!("spur.amd.com/job-id={job_id}")
    } else {
        format!("spur.amd.com/job-id={job_id},spur.amd.com/run-attempt={run_attempt}")
    }
}

fn metadata_matches_attempt(metadata: &ObjectMeta, job_id: u32, run_attempt: u32) -> bool {
    metadata.labels.as_ref().is_some_and(|labels| {
        labels.get("spur.amd.com/job-id") == Some(&job_id.to_string())
            && (run_attempt == 0
                || labels.get("spur.amd.com/run-attempt") == Some(&run_attempt.to_string()))
    })
}

fn current_attempt_pod_name(job_id: u32, pods: &[Pod]) -> Result<String, Status> {
    let job_label = job_id.to_string();
    let mut candidates: Vec<(u32, String)> = pods
        .iter()
        .filter(|pod| pod.metadata.deletion_timestamp.is_none())
        .filter_map(|pod| {
            let labels = pod.metadata.labels.as_ref()?;
            if labels.get("spur.amd.com/job-id")? != &job_label {
                return None;
            }
            let run_attempt = labels
                .get("spur.amd.com/run-attempt")
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            Some((run_attempt, pod.metadata.name.clone()?))
        })
        .collect();
    let Some(latest_attempt) = candidates.iter().map(|(attempt, _)| *attempt).max() else {
        return Err(Status::not_found(format!(
            "no active Pod found for job {job_id}"
        )));
    };
    candidates.retain(|(attempt, _)| *attempt == latest_attempt);
    if candidates.len() != 1 {
        return Err(Status::failed_precondition(format!(
            "job {job_id} attempt {latest_attempt} has multiple Pods; a target node is required"
        )));
    }
    candidates
        .pop()
        .map(|(_, name)| name)
        .ok_or_else(|| Status::internal("current Pod selection lost its candidate"))
}

fn sanitize_k8s_name(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

/// Determine the K8s device plugin resource key based on GPU type.
///
/// AMD GPUs (mi300x, mi250x, gfx*, etc.) → "amd.com/gpu"
/// NVIDIA GPUs (h100, a100, etc.) → "nvidia.com/gpu"
/// Unknown/generic → "amd.com/gpu" (AMD-first default for ROCm project)
fn gpu_vendor_resource_key(gpu_type: Option<&str>) -> &'static str {
    match gpu_type {
        Some(t) if is_nvidia_gpu(t) => "nvidia.com/gpu",
        _ => "amd.com/gpu",
    }
}

/// Check if a GPU type string refers to an NVIDIA GPU.
fn is_nvidia_gpu(gpu_type: &str) -> bool {
    let lower = gpu_type.to_lowercase();
    // NVIDIA product families
    lower.starts_with("h100")
        || lower.starts_with("h200")
        || lower.starts_with("a100")
        || lower.starts_with("a10g")
        || lower.starts_with("a30")
        || lower.starts_with("v100")
        || lower.starts_with("t4")
        || lower.starts_with("l4")
        || lower.starts_with("l40")
        || lower.starts_with("b100")
        || lower.starts_with("b200")
        || lower.starts_with("gb200")
        || lower.starts_with("rtx")
        || lower == "nvidia"
}

/// Build a gres string from GPU count and type.
pub fn gpu_request_to_gres(count: u32, gpu_type: Option<&str>) -> String {
    let t = gpu_type.unwrap_or("any");
    let t = if t.is_empty() { "any" } else { t };
    format!("gpu:{}:{}", t, count)
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

    fn attempt_pod(job_id: u32, run_attempt: u32, name: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                labels: Some(BTreeMap::from([
                    ("spur.amd.com/job-id".into(), job_id.to_string()),
                    ("spur.amd.com/run-attempt".into(), run_attempt.to_string()),
                ])),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn attempt_scoped_resource_names_and_selectors_are_stable() {
        assert_eq!(job_resource_name(17, 0), "spur-job-17");
        assert_eq!(job_resource_name(17, 3), "spur-job-17-a3");
        assert_eq!(pod_name(17, 3, "GPU_Node.1"), "spur-job-17-a3-gpu-node-1");
        assert_eq!(
            allocation_selector(17, 3),
            "spur.amd.com/job-id=17,spur.amd.com/run-attempt=3"
        );
    }

    #[test]
    fn exec_and_logs_select_the_latest_attempt_pod() {
        let pods = vec![
            attempt_pod(17, 2, "spur-job-17-a2"),
            attempt_pod(17, 3, "spur-job-17-a3"),
        ];

        assert_eq!(
            current_attempt_pod_name(17, &pods).unwrap(),
            "spur-job-17-a3"
        );
    }

    #[test]
    fn ambiguous_multinode_attempt_requires_a_target() {
        let pods = vec![
            attempt_pod(17, 3, "spur-job-17-a3-node1"),
            attempt_pod(17, 3, "spur-job-17-a3-node2"),
        ];

        let error = current_attempt_pod_name(17, &pods).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn current_pod_selection_uses_live_kubernetes_inventory() {
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
                                "name": "spur-job-17-a2",
                                "labels": {
                                    "spur.amd.com/job-id": "17",
                                    "spur.amd.com/run-attempt": "2"
                                }
                            }
                        },
                        {
                            "metadata": {
                                "name": "spur-job-17-a3",
                                "labels": {
                                    "spur.amd.com/job-id": "17",
                                    "spur.amd.com/run-attempt": "3"
                                }
                            }
                        }
                    ]
                }),
            )
        });
        let agent = VirtualAgent::new(client);

        assert_eq!(
            agent.resolve_current_pod("jobs", 17).await.unwrap(),
            "spur-job-17-a3"
        );
        let requests = seen.lock().expect("request recorder poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, Method::GET);
        assert!(requests[0].1.contains("/namespaces/jobs/pods?"));
        assert!(requests[0].1.contains("spur.amd.com%2Fjob-id%3D17"));
    }

    #[tokio::test]
    async fn stale_cancel_deletes_only_its_attempt_resources() {
        let (client, seen) = mock_kube_client(|method, uri| {
            if *method == Method::GET && uri.contains("/pods?") {
                return (
                    StatusCode::OK,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "PodList",
                        "metadata": {},
                        "items": [{"metadata": {"name": "spur-job-17-a2"}}]
                    }),
                );
            }
            (
                StatusCode::OK,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Success"
                }),
            )
        });
        let agent = VirtualAgent::new(client);

        agent
            .delete_allocation_resources("jobs", 17, 2)
            .await
            .unwrap();

        let requests = seen.lock().expect("request recorder poisoned");
        let pod_list = requests
            .iter()
            .find(|(method, uri)| *method == Method::GET && uri.contains("/pods?"))
            .expect("pod inventory was not listed");
        assert!(pod_list.1.contains("spur.amd.com%2Fjob-id%3D17"));
        assert!(pod_list.1.contains("spur.amd.com%2Frun-attempt%3D2"));
        assert!(requests.iter().any(|(method, uri)| {
            *method == Method::DELETE && uri.contains("/pods/spur-job-17-a2")
        }));
        assert!(requests.iter().any(|(method, uri)| {
            *method == Method::DELETE && uri.contains("/services/spur-job-17-a2")
        }));
        assert!(!requests
            .iter()
            .any(|(method, uri)| *method == Method::DELETE && uri.contains("-a3")));
    }

    #[tokio::test]
    async fn cancel_rpc_propagates_kubernetes_delete_failure() {
        let (client, seen) = mock_kube_client(|method, uri| {
            if *method == Method::GET && uri.contains("/spurjobs?") {
                return (
                    StatusCode::OK,
                    serde_json::json!({
                        "apiVersion": "spur.amd.com/v1alpha1",
                        "kind": "SpurJobList",
                        "metadata": {},
                        "items": [{
                            "metadata": {"name": "job-17", "namespace": "jobs"},
                            "spec": {"name": "job-17", "image": "busybox"}
                        }]
                    }),
                );
            }
            if *method == Method::GET && uri.contains("/pods?") {
                return (
                    StatusCode::OK,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "PodList",
                        "metadata": {},
                        "items": [{"metadata": {"name": "spur-job-17-a2"}}]
                    }),
                );
            }
            if *method == Method::DELETE && uri.contains("/pods/spur-job-17-a2") {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Status",
                        "status": "Failure",
                        "reason": "InternalError",
                        "code": 500
                    }),
                );
            }
            (
                StatusCode::OK,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Success"
                }),
            )
        });
        let agent = VirtualAgent::new(client);

        let error = agent
            .cancel_job(tonic::Request::new(AgentCancelJobRequest {
                job_id: 17,
                signal: 0,
                term: 0,
                run_attempt: 2,
            }))
            .await
            .expect_err("a failed Pod delete must fail the cancel RPC");

        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(seen
            .lock()
            .expect("request recorder poisoned")
            .iter()
            .any(|(method, uri)| {
                *method == Method::DELETE && uri.contains("/pods/spur-job-17-a2")
            }));
    }

    // A demoted leader's stale LaunchJob/CancelJob must be rejected here too,
    // not just on the native spurd backend.
    #[test]
    fn term_fence_rejects_stale_and_a_delayed_unfenced_request() {
        let fence = TermFence::new();
        fence
            .check(0)
            .expect("legacy zero passes before any term is seen");
        fence.check(5).expect("first real term is accepted");
        let err = fence.check(3).expect_err("stale term must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            fence.check(0).is_err(),
            "a delayed unfenced request must not bypass an established term"
        );
    }

    // --- gpu_request_to_gres ---

    #[test]
    fn test_gpu_request_to_gres() {
        assert_eq!(gpu_request_to_gres(8, Some("mi300x")), "gpu:mi300x:8");
        assert_eq!(gpu_request_to_gres(4, None), "gpu:any:4");
        assert_eq!(gpu_request_to_gres(2, Some("")), "gpu:any:2");
    }

    #[test]
    fn test_gpu_request_to_gres_single() {
        assert_eq!(gpu_request_to_gres(1, Some("h100")), "gpu:h100:1");
    }

    // --- sanitize_k8s_name ---

    #[test]
    fn test_sanitize_k8s_name() {
        assert_eq!(sanitize_k8s_name("gpu-node-01"), "gpu-node-01");
        assert_eq!(sanitize_k8s_name("NODE_WITH.DOTS"), "node-with-dots");
        assert_eq!(sanitize_k8s_name("--leading--"), "leading");
    }

    #[test]
    fn test_sanitize_k8s_name_uppercase() {
        assert_eq!(sanitize_k8s_name("GPU-NODE-01"), "gpu-node-01");
    }

    #[test]
    fn test_sanitize_k8s_name_spaces_and_special() {
        assert_eq!(sanitize_k8s_name("my node@#$123"), "my-node---123");
    }

    #[test]
    fn test_sanitize_k8s_name_all_special() {
        assert_eq!(sanitize_k8s_name("@@@"), "");
    }

    #[test]
    fn test_sanitize_k8s_name_already_clean() {
        assert_eq!(sanitize_k8s_name("worker-3"), "worker-3");
    }

    // --- parse_mounts: hostPath ---

    #[test]
    fn test_parse_mounts_hostpath() {
        let mounts = vec!["/data:/mnt/data:ro".to_string(), "/tmp:/tmp".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 2);
        assert_eq!(vmounts.len(), 2);
        assert_eq!(vmounts[0].mount_path, "/mnt/data");
        assert_eq!(vmounts[0].read_only, Some(true));
        assert_eq!(vmounts[1].mount_path, "/tmp");
        assert_eq!(vmounts[1].read_only, Some(false));
    }

    #[test]
    fn test_parse_mounts_hostpath_source_path() {
        let mounts = vec!["/host/data:/container/data".to_string()];
        let (vols, _) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 1);
        let hp = vols[0].host_path.as_ref().unwrap();
        assert_eq!(hp.path, "/host/data");
        assert_eq!(hp.type_.as_deref(), Some("DirectoryOrCreate"));
    }

    #[test]
    fn test_parse_mounts_hostpath_volume_naming() {
        let mounts = vec![
            "/a:/b".to_string(),
            "/c:/d".to_string(),
            "/e:/f".to_string(),
        ];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols[0].name, "hostpath-0");
        assert_eq!(vols[1].name, "hostpath-1");
        assert_eq!(vols[2].name, "hostpath-2");
        // Volume mount names must match volume names
        assert_eq!(vmounts[0].name, "hostpath-0");
        assert_eq!(vmounts[1].name, "hostpath-1");
        assert_eq!(vmounts[2].name, "hostpath-2");
    }

    // --- parse_mounts: PVC ---

    #[test]
    fn test_parse_mounts_pvc() {
        let mounts = vec!["pvc:my-claim:/data".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 1);
        assert_eq!(vmounts.len(), 1);
        assert_eq!(vmounts[0].mount_path, "/data");
        assert!(vols[0].persistent_volume_claim.is_some());
    }

    #[test]
    fn test_parse_mounts_pvc_claim_name() {
        let mounts = vec!["pvc:training-data:/mnt/data".to_string()];
        let (vols, _) = parse_mounts(&mounts);
        let pvc = vols[0].persistent_volume_claim.as_ref().unwrap();
        assert_eq!(pvc.claim_name, "training-data");
    }

    #[test]
    fn test_parse_mounts_pvc_readonly() {
        let mounts = vec!["pvc:datasets:/data:ro".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        let pvc = vols[0].persistent_volume_claim.as_ref().unwrap();
        assert_eq!(pvc.read_only, Some(true));
        assert_eq!(vmounts[0].read_only, Some(true));
    }

    #[test]
    fn test_parse_mounts_pvc_readwrite() {
        let mounts = vec!["pvc:output:/results".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        let pvc = vols[0].persistent_volume_claim.as_ref().unwrap();
        assert_eq!(pvc.read_only, Some(false));
        assert_eq!(vmounts[0].read_only, Some(false));
    }

    #[test]
    fn test_parse_mounts_pvc_naming() {
        let mounts = vec!["pvc:a:/x".to_string(), "pvc:b:/y".to_string()];
        let (vols, _) = parse_mounts(&mounts);
        assert_eq!(vols[0].name, "pvc-0");
        assert_eq!(vols[1].name, "pvc-1");
    }

    // --- parse_mounts: mixed and edge cases ---

    #[test]
    fn test_parse_mounts_mixed_hostpath_and_pvc() {
        let mounts = vec![
            "/data:/mnt/data:ro".to_string(),
            "pvc:checkpoints:/checkpoints".to_string(),
            "/logs:/var/log".to_string(),
        ];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 3);
        assert_eq!(vmounts.len(), 3);
        // First is hostPath
        assert!(vols[0].host_path.is_some());
        assert!(vols[0].persistent_volume_claim.is_none());
        // Second is PVC
        assert!(vols[1].persistent_volume_claim.is_some());
        assert!(vols[1].host_path.is_none());
        // Third is hostPath
        assert!(vols[2].host_path.is_some());
    }

    #[test]
    fn test_parse_mounts_empty() {
        let mounts: Vec<String> = vec![];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert!(vols.is_empty());
        assert!(vmounts.is_empty());
    }

    #[test]
    fn test_parse_mounts_single_component_ignored() {
        // A single component (no colon) should be skipped
        let mounts = vec!["just-a-path".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert!(vols.is_empty());
        assert!(vmounts.is_empty());
    }

    #[test]
    fn test_parse_mounts_pvc_missing_mount_path_ignored() {
        // "pvc:name" without mount path should be skipped (parts.len() < 3)
        let mounts = vec!["pvc:my-claim".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert!(vols.is_empty());
        assert!(vmounts.is_empty());
    }

    // --- GPU vendor detection ---

    #[test]
    fn test_is_nvidia_gpu_positive() {
        assert!(is_nvidia_gpu("h100"));
        assert!(is_nvidia_gpu("H100"));
        assert!(is_nvidia_gpu("h200"));
        assert!(is_nvidia_gpu("a100"));
        assert!(is_nvidia_gpu("A100"));
        assert!(is_nvidia_gpu("a10g"));
        assert!(is_nvidia_gpu("a30"));
        assert!(is_nvidia_gpu("v100"));
        assert!(is_nvidia_gpu("t4"));
        assert!(is_nvidia_gpu("T4"));
        assert!(is_nvidia_gpu("l4"));
        assert!(is_nvidia_gpu("l40s"));
        assert!(is_nvidia_gpu("L40"));
        assert!(is_nvidia_gpu("b100"));
        assert!(is_nvidia_gpu("b200"));
        assert!(is_nvidia_gpu("gb200"));
        assert!(is_nvidia_gpu("GB200"));
        assert!(is_nvidia_gpu("rtx4090"));
        assert!(is_nvidia_gpu("RTX3090"));
        assert!(is_nvidia_gpu("nvidia"));
        assert!(is_nvidia_gpu("NVIDIA"));
    }

    #[test]
    fn test_is_nvidia_gpu_negative_amd() {
        assert!(!is_nvidia_gpu("mi300x"));
        assert!(!is_nvidia_gpu("MI300X"));
        assert!(!is_nvidia_gpu("mi250x"));
        assert!(!is_nvidia_gpu("mi210"));
        assert!(!is_nvidia_gpu("mi100"));
        assert!(!is_nvidia_gpu("gfx942"));
        assert!(!is_nvidia_gpu("gfx1201"));
        assert!(!is_nvidia_gpu("gfx90a"));
        assert!(!is_nvidia_gpu("rx7900xtx"));
        assert!(!is_nvidia_gpu("w7900"));
        assert!(!is_nvidia_gpu("amd"));
        assert!(!is_nvidia_gpu("gpu"));
        assert!(!is_nvidia_gpu("any"));
        assert!(!is_nvidia_gpu(""));
    }

    #[test]
    fn test_gpu_vendor_resource_key_amd() {
        assert_eq!(gpu_vendor_resource_key(Some("mi300x")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("mi250x")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("gfx942")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("gfx90a")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("w7900")), "amd.com/gpu");
    }

    #[test]
    fn test_gpu_vendor_resource_key_nvidia() {
        assert_eq!(gpu_vendor_resource_key(Some("h100")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("a100")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("v100")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("t4")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("l40s")), "nvidia.com/gpu");
    }

    #[test]
    fn test_gpu_vendor_resource_key_defaults_amd() {
        // Unknown or generic GPU types default to AMD (ROCm project)
        assert_eq!(gpu_vendor_resource_key(None), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("gpu")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("any")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("")), "amd.com/gpu");
    }

    #[test]
    fn test_gpu_request_to_gres_amd_types() {
        assert_eq!(gpu_request_to_gres(8, Some("mi300x")), "gpu:mi300x:8");
        assert_eq!(gpu_request_to_gres(4, Some("mi250x")), "gpu:mi250x:4");
        assert_eq!(gpu_request_to_gres(1, Some("gfx942")), "gpu:gfx942:1");
    }
}
