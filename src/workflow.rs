use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowConfig {
    pub version: Option<String>,
    pub name: String,
    pub env: Option<HashMap<String, String>>,
    pub steps: Vec<StepConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepConfig {
    pub name: String,
    pub run: String,
    pub working_dir: Option<String>,
    pub env: Option<HashMap<String, String>>,
    pub timeout_seconds: Option<u64>,
}

impl WorkflowConfig {
    pub fn load_from_str(
        content: &str,
        repo_root: &Path,
        max_timeout: u64,
    ) -> Result<Self, String> {
        let workflow: WorkflowConfig = serde_yaml::from_str(content)
            .map_err(|e| format!("Failed to parse YAML workflow: {}", e))?;

        if workflow.steps.is_empty() {
            return Err("Workflow must contain at least one step in 'steps'".to_string());
        }

        for (idx, step) in workflow.steps.iter().enumerate() {
            if step.run.trim().is_empty() {
                return Err(format!(
                    "Step #{} ('{}') has an empty 'run' command",
                    idx + 1,
                    step.name
                ));
            }

            if let Some(ref dir) = step.working_dir {
                let path = PathBuf::from(dir);
                if path.is_absolute() || dir.contains("..") {
                    return Err(format!(
                        "Step '{}' has an invalid working_dir '{}': path traversal is strictly forbidden",
                        step.name, dir
                    ));
                }
                let resolved = repo_root.join(dir);
                if !resolved.starts_with(repo_root) {
                    return Err(format!(
                        "Step '{}' working_dir escapes repository boundary",
                        step.name
                    ));
                }
            }

            if let Some(timeout) = step.timeout_seconds {
                if timeout == 0 || timeout > max_timeout {
                    return Err(format!(
                        "Step '{}' timeout_seconds ({}s) must be between 1 and {}s",
                        step.name, timeout, max_timeout
                    ));
                }
            }
        }

        Ok(workflow)
    }
}
