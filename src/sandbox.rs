//! Worker sandbox integration with `MuIsolation`.
//!
//! Sub-tasks #20 & #21: 3-layer privilege attenuation, Landlock LSM/namespaces plan building,
//! bounded `RLIMIT_MEMLOCK` ceiling, conditional SpacemiT K3 `/proc/set_ai_thread` registration,
//! and fail-closed `SecurityGrantDenied` exit handling.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use mu_ci_ipc::{
    NetworkEgressMode as IpcNetworkEgressMode, SandboxBackendKind as IpcBackendKind,
    SandboxConstraintSpec, SecurityGrantDenied, StageDispatchTask,
};
use mu_ci_isolation::prelude::*;
use mu_ci_isolation::{
    DesiredProfileConfig, DesiredSecurityFile, EffectiveSandboxPolicy, HostCeilingConfig,
    IsolationBackend, IsolationError, KernelSandbox, NetworkEgressMode as MuNetworkEgressMode,
    SandboxBackendKind as MuBackendKind, SandboxExecutionPlan, TaskSandboxConfig,
    MAX_K3_MEMLOCK_KB,
};
use tracing::error;

/// Construct a safe default operator host ceiling when `/etc/mu-isolation/ceiling.yml` is not provisioned.
pub fn default_host_ceiling(node_id: &str, arch: &str) -> HostCeilingConfig {
    HostCeilingConfig {
        node_id: node_id.to_string(),
        arch: arch.to_string(),
        allowed_backends: vec![MuBackendKind::Kernel, MuBackendKind::Hypervisor],
        allowed_read_prefixes: vec![PathBuf::from("/")],
        allowed_write_prefixes: vec![
            PathBuf::from("/tmp"),
            PathBuf::from("/var/lib/action-runner"),
            std::env::temp_dir(),
        ],
        allowed_exec_prefixes: vec![
            PathBuf::from("/bin"),
            PathBuf::from("/usr/bin"),
            PathBuf::from("/usr/local/bin"),
            PathBuf::from("/opt"),
            PathBuf::from("/var/lib/action-runner"),
            std::env::temp_dir(),
        ],
        allowed_egress_cidrs: vec!["0.0.0.0/0".to_string()],
        max_memlock_kb: MAX_K3_MEMLOCK_KB,
        max_cpu_cores: 16,
        max_memory_mb: 32768,
        max_pids: 32768,
        allow_spacemit_ai_thread: true,
    }
}

/// Construct a default repository desired security profile from host ceiling when `.actionrunner/security.yml` is absent.
pub fn default_security_file_from_ceiling(ceiling: &HostCeilingConfig) -> DesiredSecurityFile {
    let mut profiles = BTreeMap::new();
    profiles.insert(
        "default".to_string(),
        DesiredProfileConfig {
            backend: ceiling
                .allowed_backends
                .first()
                .copied()
                .unwrap_or(MuBackendKind::Kernel),
            read_paths: ceiling.allowed_read_prefixes.clone(),
            write_paths: ceiling.allowed_write_prefixes.clone(),
            exec_paths: ceiling.allowed_exec_prefixes.clone(),
            network_egress_cidrs: ceiling.allowed_egress_cidrs.clone(),
            memlock_limit_kb: ceiling.max_memlock_kb.min(MAX_K3_MEMLOCK_KB),
            max_pids: ceiling.max_pids,
            max_memory_mb: Some(ceiling.max_memory_mb),
            cpu_affinity_mask: None,
            require_set_ai_thread: false,
        },
    );
    DesiredSecurityFile {
        default_profile: "default".to_string(),
        profiles,
    }
}

/// Convert an IPC wire `SandboxConstraintSpec` to a `MuIsolation` `TaskSandboxConfig`.
pub fn constraint_spec_to_task_config(spec: &SandboxConstraintSpec) -> TaskSandboxConfig {
    let backend = match spec.backend {
        IpcBackendKind::Kernel => Some(MuBackendKind::Kernel),
        IpcBackendKind::Hypervisor => Some(MuBackendKind::Hypervisor),
    };

    let network_mode = match &spec.network_mode {
        IpcNetworkEgressMode::Offline => MuNetworkEgressMode::Offline,
        IpcNetworkEgressMode::RestrictedCidrs(cidrs) => {
            MuNetworkEgressMode::RestrictedCidrs(cidrs.clone())
        }
    };

    TaskSandboxConfig {
        backend,
        read_paths: spec.read_paths.iter().map(PathBuf::from).collect(),
        write_paths: spec.write_paths.iter().map(PathBuf::from).collect(),
        exec_paths: spec.exec_paths.iter().map(PathBuf::from).collect(),
        network_mode,
        memlock_limit_kb: if spec.memlock_limit_kb > 0 {
            Some(spec.memlock_limit_kb)
        } else {
            None
        },
        max_pids: if spec.max_pids > 0 {
            Some(spec.max_pids)
        } else {
            None
        },
        max_memory_mb: None,
        cpu_affinity_mask: None,
        require_set_ai_thread: if spec.require_set_ai_thread {
            Some(true)
        } else {
            None
        },
    }
}

/// Worker sandbox runner handling policy loading, attenuation, and plan construction.
#[derive(Debug, Clone)]
pub struct WorkerSandboxRunner {
    pub node_id: String,
    pub arch: String,
    pub ceiling_path: PathBuf,
    pub proc_ai_thread_path: PathBuf,
}

impl WorkerSandboxRunner {
    /// Construct a new sandbox runner with standard system paths.
    pub fn new(node_id: String, arch: String) -> Self {
        let ceiling_path = std::env::var("MU_ISOLATION_CEILING_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/etc/mu-isolation/ceiling.yml"));

        Self {
            node_id,
            arch,
            ceiling_path,
            proc_ai_thread_path: PathBuf::from(mu_ci_isolation::DEFAULT_SET_AI_THREAD_PATH),
        }
    }

    /// Construct a sandbox runner with custom paths for tests or edge deployment.
    pub fn with_custom_paths(
        node_id: String,
        arch: String,
        ceiling_path: PathBuf,
        proc_ai_thread_path: PathBuf,
    ) -> Self {
        Self {
            node_id,
            arch,
            ceiling_path,
            proc_ai_thread_path,
        }
    }

    /// Load the host operator ceiling from disk or use the default safe ceiling.
    pub fn load_host_ceiling(&self) -> Result<HostCeilingConfig, IsolationError> {
        if self.ceiling_path.is_file() {
            HostCeilingConfig::from_path(&self.ceiling_path)
        } else {
            Ok(default_host_ceiling(&self.node_id, &self.arch))
        }
    }

    /// Load the repository desired security profiles from `.muci/security.yml` (falling back to legacy `.actionrunner/security.yml`).
    pub fn load_desired_security(
        &self,
        stage_dir: &Path,
        ceiling: &HostCeilingConfig,
    ) -> Result<DesiredSecurityFile, IsolationError> {
        let muci_sec_path = stage_dir.join(".muci/security.yml");
        let legacy_sec_path = stage_dir.join(".actionrunner/security.yml");
        let sec_path = if muci_sec_path.is_file() {
            Some(muci_sec_path)
        } else if legacy_sec_path.is_file() {
            Some(legacy_sec_path)
        } else {
            None
        };
        if let Some(path) = sec_path {
            DesiredSecurityFile::from_path(&path)
        } else {
            Ok(default_security_file_from_ceiling(ceiling))
        }
    }

    /// Resolve and attenuate the 3-layer security policy:
    /// HostCeiling $\cap$ DesiredSecurityFile $\cap$ TaskConstraints.
    pub fn evaluate_policy(
        &self,
        stage_dir: &Path,
        spec: &SandboxConstraintSpec,
    ) -> Result<EffectiveSandboxPolicy, IsolationError> {
        let ceiling = self.load_host_ceiling()?;
        let desired_file = self.load_desired_security(stage_dir, &ceiling)?;

        let profile_name = if spec.profile_name.trim().is_empty() {
            &desired_file.default_profile
        } else {
            &spec.profile_name
        };

        let desired_profile = desired_file.profiles.get(profile_name).ok_or_else(|| {
            IsolationError::UnknownSecurityProfile {
                profile_name: profile_name.to_string(),
            }
        })?;

        let task_config = constraint_spec_to_task_config(spec);

        resolve_effective_policy(&ceiling, desired_profile, &task_config)
    }

    /// Lower an effective policy into a concrete `SandboxExecutionPlan`.
    pub fn build_execution_plan(
        &self,
        effective: &EffectiveSandboxPolicy,
        argv: &[OsString],
    ) -> Result<SandboxExecutionPlan, IsolationError> {
        match effective.backend {
            MuBackendKind::Kernel => {
                let sandbox = KernelSandbox::with_proc_ai_thread_path(&self.proc_ai_thread_path);
                sandbox.build_execution_plan(effective, argv)
            }
            MuBackendKind::Hypervisor => {
                let sandbox = HypervisorSandbox::new();
                sandbox.build_execution_plan(effective, argv)
            }
        }
    }

    /// Conditionally register the calling thread with `/proc/set_ai_thread` if running
    /// on SpacemiT K3 `spacemit-hmp` kernel and targeting Cluster 1 (A100 cores).
    pub fn apply_thread_registration_if_needed(
        &self,
        policy: &EffectiveSandboxPolicy,
    ) -> Result<bool, IsolationError> {
        #[cfg(target_os = "linux")]
        let tid = unsafe { libc::syscall(libc::SYS_gettid) as u32 };
        #[cfg(not(target_os = "linux"))]
        let tid = std::process::id();

        mu_ci_isolation::register_ai_thread_if_present(
            &self.proc_ai_thread_path,
            tid,
            policy.cpu_affinity_mask,
            policy.require_set_ai_thread,
        )
    }

    /// Fail-closed audit handler translating `IsolationError` into a `SecurityGrantDenied` report.
    pub fn handle_security_violation(
        task: &StageDispatchTask,
        err: &IsolationError,
    ) -> (SecurityGrantDenied, String) {
        let (layer, field, requested, allowed) = match err {
            IsolationError::SecurityGrantDenied {
                violated_layer,
                offending_field,
                requested_value,
                ceiling_value,
            } => (
                violated_layer.clone(),
                offending_field.clone(),
                requested_value.clone(),
                ceiling_value.clone(),
            ),
            IsolationError::HardwareConstraintViolation { field, reason } => (
                "HardwareConstraint".to_string(),
                field.clone(),
                reason.clone(),
                format!("Bounded ceiling (max {MAX_K3_MEMLOCK_KB} KB)"),
            ),
            IsolationError::UnknownSecurityProfile { profile_name } => (
                "DesiredProfile".to_string(),
                "profile_name".to_string(),
                profile_name.clone(),
                "Defined profiles in .muci/security.yml or .actionrunner/security.yml".to_string(),
            ),
            other => (
                "SandboxPolicy".to_string(),
                "general".to_string(),
                other.to_string(),
                "Permitted policy".to_string(),
            ),
        };

        error!(
            event = "security_grant_denied",
            job_id = %task.job_id,
            stage_id = %task.stage_id,
            violated_layer = %layer,
            offending_field = %field,
            requested_value = %requested,
            ceiling_value = %allowed,
            "SecurityGrantDenied: stage aborted fail-closed"
        );

        let denied = SecurityGrantDenied {
            job_id: task.job_id.clone(),
            stage_id: task.stage_id.clone(),
            violated_layer: layer,
            offending_field: field,
            requested_value: requested,
            ceiling_value: allowed,
        };

        let message = format!(
            "SecurityGrantDenied: violated {} on field '{}': requested '{}', allowed '{}'",
            denied.violated_layer,
            denied.offending_field,
            denied.requested_value,
            denied.ceiling_value
        );

        (denied, message)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_test_dispatch_task(spec: SandboxConstraintSpec) -> StageDispatchTask {
        StageDispatchTask {
            job_id: "test-job-sec".to_string(),
            stage_id: "test-stage-sec".to_string(),
            stage_name: "test-stage".to_string(),
            repo_name: "LMA1980/test-repo".to_string(),
            clone_url: "https://github.com/LMA1980/test-repo.git".to_string(),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
            command: "echo test".to_string(),
            working_dir: None,
            env_vars: Vec::new(),
            timeout_seconds: 60,
            threads: 2,
            toolchain: None,
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: Vec::new(),
            sandbox_constraints: spec,
            traceparent: None,
            sccache_mode: Default::default(),
        }
    }

    #[test]
    fn test_attenuation_safe_defaults() {
        let runner = WorkerSandboxRunner::new("worker-node".to_string(), "x86_64".to_string());
        let stage_dir = tempdir().unwrap();

        let spec = SandboxConstraintSpec {
            profile_name: "default".to_string(),
            backend: IpcBackendKind::Kernel,
            read_paths: vec!["/tmp".to_string()],
            write_paths: vec!["/tmp".to_string()],
            exec_paths: vec!["/usr/bin".to_string()],
            network_mode: IpcNetworkEgressMode::Offline,
            memlock_limit_kb: 1024,
            max_pids: 128,
            require_set_ai_thread: false,
        };

        let effective = runner
            .evaluate_policy(stage_dir.path(), &spec)
            .expect("Safe policy attenuation should succeed");

        assert_eq!(effective.backend, MuBackendKind::Kernel);
        assert_eq!(effective.memlock_limit_kb, 1024);
        assert_eq!(effective.max_pids, 128);
    }

    #[test]
    fn test_fail_closed_memlock_exceeding_ceiling() {
        let dir = tempdir().unwrap();
        let ceiling_path = dir.path().join("ceiling.yml");

        // Write strict ceiling with max 16 MiB memlock
        let ceiling_yaml = r#"
node_id: worker-node
arch: x86_64
allowed_backends: [Kernel]
allowed_read_prefixes: [/]
allowed_write_prefixes: [/tmp]
allowed_exec_prefixes: [/usr/bin]
allowed_egress_cidrs: [0.0.0.0/0]
max_memlock_kb: 16384
max_cpu_cores: 4
max_memory_mb: 8192
max_pids: 512
allow_spacemit_ai_thread: false
"#;
        std::fs::write(&ceiling_path, ceiling_yaml).unwrap();

        let runner = WorkerSandboxRunner::with_custom_paths(
            "worker-node".to_string(),
            "x86_64".to_string(),
            ceiling_path,
            dir.path().join("set_ai_thread"),
        );

        let stage_dir = tempdir().unwrap();

        // Request 32 MiB memlock (exceeds 16 MiB ceiling)
        let spec = SandboxConstraintSpec {
            profile_name: "default".to_string(),
            backend: IpcBackendKind::Kernel,
            read_paths: vec!["/tmp".to_string()],
            write_paths: vec!["/tmp".to_string()],
            exec_paths: vec!["/usr/bin".to_string()],
            network_mode: IpcNetworkEgressMode::Offline,
            memlock_limit_kb: 32768,
            max_pids: 128,
            require_set_ai_thread: false,
        };

        let res = runner.evaluate_policy(stage_dir.path(), &spec);
        assert!(res.is_err(), "Exceeding memlock ceiling must fail closed");

        let err = res.unwrap_err();
        let task = make_test_dispatch_task(spec);
        let (denied, msg) = WorkerSandboxRunner::handle_security_violation(&task, &err);

        assert_eq!(denied.offending_field, "memlock_limit_kb");
        assert!(msg.contains("SecurityGrantDenied"));
    }

    #[test]
    fn test_fail_closed_unauthorized_write_path() {
        let dir = tempdir().unwrap();
        let ceiling_path = dir.path().join("ceiling.yml");

        // Ceiling only permits writes in /tmp
        let ceiling_yaml = r#"
node_id: worker-node
arch: x86_64
allowed_backends: [Kernel]
allowed_read_prefixes: [/]
allowed_write_prefixes: [/tmp]
allowed_exec_prefixes: [/usr/bin]
allowed_egress_cidrs: [0.0.0.0/0]
max_memlock_kb: 65536
max_cpu_cores: 4
max_memory_mb: 8192
max_pids: 512
allow_spacemit_ai_thread: false
"#;
        std::fs::write(&ceiling_path, ceiling_yaml).unwrap();

        let runner = WorkerSandboxRunner::with_custom_paths(
            "worker-node".to_string(),
            "x86_64".to_string(),
            ceiling_path,
            dir.path().join("set_ai_thread"),
        );

        let stage_dir = tempdir().unwrap();

        // Request write in /etc
        let spec = SandboxConstraintSpec {
            profile_name: "default".to_string(),
            backend: IpcBackendKind::Kernel,
            read_paths: vec!["/tmp".to_string()],
            write_paths: vec!["/etc".to_string()],
            exec_paths: vec!["/usr/bin".to_string()],
            network_mode: IpcNetworkEgressMode::Offline,
            memlock_limit_kb: 1024,
            max_pids: 128,
            require_set_ai_thread: false,
        };

        let res = runner.evaluate_policy(stage_dir.path(), &spec);
        assert!(res.is_err(), "Write to /etc must fail closed");

        let err = res.unwrap_err();
        let task = make_test_dispatch_task(spec);
        let (denied, _) = WorkerSandboxRunner::handle_security_violation(&task, &err);

        assert_eq!(denied.offending_field, "write_paths");
    }

    #[test]
    fn test_conditional_proc_ai_thread_registration() {
        let dir = tempdir().unwrap();
        let proc_path = dir.path().join("set_ai_thread");

        let runner = WorkerSandboxRunner::with_custom_paths(
            "worker-riscv64".to_string(),
            "riscv64".to_string(),
            dir.path().join("ceiling.yml"),
            proc_path.clone(),
        );

        // When require_set_ai_thread is false, does not attempt procfs write
        let policy = default_host_ceiling("worker-riscv64", "riscv64");
        let effective = EffectiveSandboxPolicy {
            node_id: policy.node_id,
            arch: policy.arch,
            backend: MuBackendKind::Kernel,
            read_paths: vec![PathBuf::from("/")],
            write_paths: vec![PathBuf::from("/tmp")],
            exec_paths: vec![PathBuf::from("/usr/bin")],
            network_mode: MuNetworkEgressMode::Offline,
            memlock_limit_kb: 1024,
            max_pids: 128,
            max_memory_mb: 1024,
            max_cpu_cores: 4,
            cpu_affinity_mask: Some(0xFF00),
            require_set_ai_thread: false,
        };

        let res = runner
            .apply_thread_registration_if_needed(&effective)
            .unwrap();
        assert!(
            !res,
            "Should return false when require_set_ai_thread is false"
        );

        // When require_set_ai_thread is true and procfs node is absent (vanilla kernel)
        let effective_true = EffectiveSandboxPolicy {
            require_set_ai_thread: true,
            ..effective.clone()
        };
        let res2 = runner
            .apply_thread_registration_if_needed(&effective_true)
            .unwrap();
        assert!(
            !res2,
            "Should return false cleanly when procfs node does not exist (vanilla kernel)"
        );

        // When procfs node exists (spacemit-hmp kernel)
        std::fs::write(&proc_path, "").unwrap();
        let res3 = runner
            .apply_thread_registration_if_needed(&effective_true)
            .unwrap();
        assert!(
            res3,
            "Should return true and write TID when procfs node exists"
        );
        let contents = std::fs::read_to_string(&proc_path).unwrap();
        assert!(
            !contents.trim().is_empty(),
            "Thread ID must be written to procfs node"
        );
    }
}
