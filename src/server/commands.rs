use bollard::container::Config;
use bollard::secret::ContainerSummary;

use crate::server::ServerError;

/// Commands represent intentions - what the Runner wants to happen
#[derive(Debug, Clone, PartialEq)]
pub enum DockerCommand {
    ListContainers,
    PullImage { image: String },
    CreateContainer { name: String, config: Config<String> },
    StartContainer { name: String },
    StopContainer { name: String, timeout: u64 },
    RemoveContainer { name: String },
}

/// Events represent outcomes - what actually happened
#[derive(Debug)]
pub enum DockerEvent {
    ContainersListed { containers: Vec<ContainerSummary> },
    ImagePulled { image: String },
    ContainerCreated { name: String },
    ContainerStarted { name: String },
    ContainerStopped { name: String },
    ContainerRemoved { name: String },
    Error { context: String, error: ServerError },
}

/// Actions to take during reconciliation
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReconcileActions {
    pub to_create: Vec<String>,
    pub to_stop: Vec<String>,
    pub to_remove: Vec<String>,
}
