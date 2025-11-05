use bollard::auth::DockerCredentials;
use bollard::container::Config;
use bollard::container::CreateContainerOptions;
use bollard::container::ListContainersOptions;
use bollard::container::RemoveContainerOptions;
use bollard::container::StartContainerOptions;
use bollard::container::StopContainerOptions;
use bollard::image::CreateImageOptions;
use bollard::secret::ContainerSummary;
use bollard::secret::HostConfig;
use bollard::secret::Mount;
use bollard::secret::MountTypeEnum;
use bollard::secret::PortBinding;
use bollard::secret::RestartPolicy;
use bollard::secret::RestartPolicyNameEnum;

use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use thiserror::Error;
use tokio::{fs, io};
use tracing::debug;
use tracing::trace;

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::time::Instant;

pub mod commands;
pub mod desired_state;

use self::commands::{DockerCommand, DockerEvent, ReconcileActions};
use self::desired_state::DesiredState;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("couldn't read env file: {0}")]
    EnvFile(#[from] io::Error),
    #[error("something went wrong interacting with docker: {0}")]
    DockerError(#[from] bollard::errors::Error),
    #[error("there are no more ips available")]
    IpsExhausted,
    #[error("server {0} not found")]
    ServerNotFound(String),
    #[error("the server didn't get an ip before trying to run")]
    ServerMissingIP,
    #[error("couldn't parse the service: {0}")]
    ServiceParseError(#[from] serde_json::Error),
}

type ServerResult<T> = Result<T, ServerError>;

#[derive(Debug, Clone)]
pub struct Server {
    pub name: String,
    pub addr: Option<SocketAddr>,
    service: Service,
}

#[derive(Debug, Clone, Hash, Deserialize)]
pub struct VolumeMapping {
    pub source: String,
    pub destination: String,
}

impl Display for VolumeMapping {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}:{}", self.source, self.destination)
    }
}

#[derive(Debug, Clone, Default, Hash, Deserialize)]
pub struct Service {
    pub name: String,
    pub port: u16,
    pub image: String,
    pub env: Option<Vec<String>>,
    pub volume_mapping: Option<VolumeMapping>,
}

impl Service {
    pub async fn with_env_file(self, path: impl AsRef<Path>) -> ServerResult<Self> {
        let env = read_env_file(path).await?;
        let mut ret = self.clone();
        ret.env = Some(env);
        Ok(ret)
    }

    pub async fn parse_from_file(path: impl AsRef<Path>) -> ServerResult<Self> {
        #[derive(Debug, Clone, Default, Deserialize)]
        struct ServiceFromFile {
            #[serde(flatten)]
            pub service: Service,
            pub env_file: Option<String>,
        }

        let file = fs::read(path).await?;
        let service_file: ServiceFromFile = serde_json::from_slice(&file)?;

        let mut svc = service_file.service;
        if let Some(path) = service_file.env_file {
            svc = svc.with_env_file(path).await?;
        }

        Ok(svc)
    }

    pub fn container_config(&self, ip: IpAddr) -> ServerResult<Config<String>> {
        let empty_object = HashMap::new();

        let port_name = format!("{0}/tcp", self.port);
        let mut ports = HashMap::new();
        ports.insert(port_name.clone(), empty_object);

        let ip_string = ip.to_string();
        let port_string = self.port.to_string();
        let port_binding = PortBinding {
            host_ip: Some(ip_string),
            host_port: Some(port_string),
        };
        let mut port_bindings = HashMap::new();
        let binding_name = port_name.clone();
        port_bindings.insert(binding_name, Some(vec![port_binding]));

        let restart_always = RestartPolicy {
            name: Some(RestartPolicyNameEnum::ALWAYS),
            maximum_retry_count: None,
        };

        let mounts = self.volume_mapping.as_ref().map(|volume| {
            let mount = Mount {
                typ: Some(MountTypeEnum::BIND),
                source: Some(volume.source.clone()),
                target: Some(volume.destination.clone()),
                ..Default::default()
            };
            vec![mount]
        });
        let host_config = Some(HostConfig {
            port_bindings: Some(port_bindings),
            mounts,
            restart_policy: Some(restart_always),
            ..Default::default()
        });

        let labels = Some(HashMap::from([
            ("managed-by".to_string(), "deployd".to_string()),
            ("deployd/service-name".to_string(), self.name.clone()),
        ]));

        let config = Config {
            image: Some(self.image.clone()),
            env: self.env.clone(),
            labels,
            exposed_ports: Some(ports),
            host_config,
            ..Default::default()
        };
        Ok(config)
    }
}

pub async fn read_env_file(path: impl AsRef<Path>) -> ServerResult<Vec<String>> {
    let read = fs::read_to_string(path).await?;
    let lines = read
        .split('\n')
        .filter_map(|l| {
            if !l.is_empty() {
                Some(l.to_owned())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(lines)
}

#[derive(Debug, Clone)]
struct IpProvisioner(BTreeSet<u8>);

impl IpProvisioner {
    fn new() -> Self {
        let mut available = BTreeSet::new();
        for i in 2..=254 {
            available.insert(i);
        }
        IpProvisioner(available)
    }
    fn reserve_ip(&mut self) -> ServerResult<Ipv4Addr> {
        let ip = self.0.pop_first().ok_or(ServerError::IpsExhausted)?;

        Ok(Ipv4Addr::new(127, 0, 0, ip))
    }

    fn release_ip(&mut self, ip: Ipv4Addr) {
        let [_, _, _, last] = ip.octets();
        self.0.insert(last);
    }
}

impl Default for IpProvisioner {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Default)]
struct Identifier;

impl Identifier {
    fn get(svc: &Service) -> String {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut h = DefaultHasher::default();
        svc.hash(&mut h);
        let ret = h.finish();
        bs58::encode(ret.to_le_bytes()).into_string()
    }
}

#[derive(Debug, Clone)]
struct ContainerMetadata {
    created_at: Option<Instant>,
    started_at: Option<Instant>,
    replaced_at: Option<Instant>,
}

impl Default for ContainerMetadata {
    fn default() -> Self {
        Self {
            created_at: None,
            started_at: None,
            replaced_at: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Runner {
    ips: IpProvisioner,
    desired: DesiredState,
    pending_commands: VecDeque<DockerCommand>,
    container_metadata: HashMap<String, ContainerMetadata>,
    last_container_list: HashSet<String>,
    reconcile_needed: bool,
}

impl Runner {
    pub fn new() -> ServerResult<Self> {
        let desired = DesiredState::default();
        let ips = IpProvisioner::default();

        Ok(Self {
            ips,
            desired,
            pending_commands: VecDeque::new(),
            container_metadata: HashMap::new(),
            last_container_list: HashSet::new(),
            reconcile_needed: false,
        })
    }

    /// Poll for the next command to execute
    pub fn poll_command(&mut self) -> Option<DockerCommand> {
        self.pending_commands.pop_front()
    }

    /// Handle an event from the Docker daemon
    pub fn handle_event(&mut self, event: DockerEvent, now: Instant) {
        match event {
            DockerEvent::ContainersListed { containers } => {
                self.last_container_list = containers
                    .into_iter()
                    .filter_map(|cs| {
                        cs.names?
                            .first()
                            .map(|n| n.trim().trim_start_matches('/').to_string())
                    })
                    .collect();
                trace!(containers = ?self.last_container_list.iter().cloned().collect::<Vec<String>>(), "found containers");
                self.reconcile_needed = true;
            }
            DockerEvent::ImagePulled { image: _ } => {
                // Continue with next command
            }
            DockerEvent::ContainerCreated { name } => {
                if let Some(metadata) = self.container_metadata.get_mut(&name) {
                    metadata.created_at = Some(now);
                } else {
                    self.container_metadata.insert(
                        name,
                        ContainerMetadata {
                            created_at: Some(now),
                            ..Default::default()
                        },
                    );
                }
            }
            DockerEvent::ContainerStarted { name } => {
                if let Some(metadata) = self.container_metadata.get_mut(&name) {
                    metadata.started_at = Some(now);
                }
            }
            DockerEvent::ContainerStopped { name: _ } => {
                // Continue with removal
            }
            DockerEvent::ContainerRemoved { name } => {
                self.container_metadata.remove(&name);
                if let Some(s) = self.desired.remove_server(&name) {
                    if let Some(addr) = s.addr {
                        if let IpAddr::V4(ip) = addr.ip() {
                            self.release_ip(ip);
                        }
                    }
                }
            }
            DockerEvent::Error { context, error } => {
                tracing::error!(context = context, error = ?error, "docker operation failed");
            }
        }
    }

    /// Handle timeout - check for time-based actions
    pub fn handle_timeout(&mut self, now: Instant) {
        // Check for old containers to remove (30s grace period after replacement)
        let to_stop: Vec<String> = self
            .container_metadata
            .iter()
            .filter_map(|(name, metadata)| {
                metadata.replaced_at.and_then(|t| {
                    if now.duration_since(t).as_secs() >= 30 {
                        Some(name.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        for name in to_stop {
            debug!(container = name, "stopping old container after grace period");
            self.pending_commands
                .push_back(DockerCommand::StopContainer {
                    name: name.clone(),
                    timeout: 30,
                });
            self.pending_commands
                .push_back(DockerCommand::RemoveContainer { name });
        }
    }

    /// Poll for next timeout
    pub fn poll_timeout(&self) -> Option<Instant> {
        self.container_metadata
            .values()
            .filter_map(|m| m.replaced_at)
            .map(|t| t + std::time::Duration::from_secs(30))
            .min()
    }

    /// Request reconciliation on next poll_command
    pub fn request_reconcile(&mut self) {
        self.reconcile_needed = true;
        self.pending_commands
            .push_back(DockerCommand::ListContainers);
    }

    /// Plan what actions need to be taken during reconciliation (pure logic)
    pub fn plan_reconcile(&mut self, now: Instant) -> ReconcileActions {
        if !self.reconcile_needed {
            return ReconcileActions::default();
        }

        let container_names = self.last_container_list.clone();
        let mut actions = ReconcileActions::default();

        // find services with multiple servers
        // if new server is running delete the old one(s)
        let to_remove = self.old_servers(&container_names);
        if !to_remove.is_empty() {
            debug!(servers = ?to_remove, "found old versions of servers to remove");
        }

        // Mark old servers as replaced
        for name in &to_remove {
            if let Some(metadata) = self.container_metadata.get_mut(name) {
                metadata.replaced_at = Some(now);
            }
        }

        actions.to_remove = to_remove;

        // compare with desired containers
        let mut not_found: Vec<String> = Vec::new();
        for name in self.desired.server_names() {
            if !container_names.contains(name) {
                not_found.push(name.to_string())
            }
        }

        // Find extra containers to stop
        for name in &container_names {
            if !self.desired.server_names().any(|n| n == name) {
                actions.to_stop.push(name.clone());
            }
        }

        actions.to_create = not_found;
        self.reconcile_needed = false;

        actions
    }

    /// Execute reconciliation actions by emitting commands
    pub fn execute_reconcile(&mut self, actions: ReconcileActions) {
        // Remove old servers from state
        for name in actions.to_remove {
            self.desired.remove_server(name);
        }

        // Stop extra containers
        for name in &actions.to_stop {
            debug!(container = name, "stopping container");
            self.pending_commands
                .push_back(DockerCommand::StopContainer {
                    name: name.clone(),
                    timeout: 30,
                });
            self.pending_commands
                .push_back(DockerCommand::RemoveContainer {
                    name: name.clone(),
                });
        }

        // Create missing containers
        for name in &actions.to_create {
            if let Some(server) = self.desired.get_server(name) {
                debug!(container = name, "starting container");
                let ip = server.addr.expect("server should have addr").ip();
                let config = server
                    .service
                    .container_config(ip)
                    .expect("failed to create config");

                self.pending_commands
                    .push_back(DockerCommand::PullImage {
                        image: server.service.image.clone(),
                    });
                self.pending_commands
                    .push_back(DockerCommand::CreateContainer {
                        name: name.clone(),
                        config,
                    });
                self.pending_commands
                    .push_back(DockerCommand::StartContainer {
                        name: name.clone(),
                    });
            }
        }
    }

    // old servers are attached to a service and have a newer version running
    fn old_servers(&mut self, container_names: &HashSet<String>) -> Vec<String> {
        self.desired
            .servers()
            .flat_map(|server_names| {
                if server_names.len() == 1 {
                    return vec![];
                }
                if let Some(new) = server_names.last() {
                    if container_names.contains(new) {
                        return server_names.iter().filter(|n| n != &new).cloned().collect();
                    }
                }

                vec![]
            })
            .collect()
    }

    pub fn latest_server_for_service(&self, service_name: impl Into<String>) -> Option<Server> {
        self.desired
            .servers_for_service(service_name)?
            .last()
            .cloned()
    }

    /// return an ip from 127.0.0.2-254
    fn reserve_ip(&mut self) -> ServerResult<Ipv4Addr> {
        self.ips.reserve_ip()
    }

    fn release_ip(&mut self, ip: Ipv4Addr) {
        self.ips.release_ip(ip)
    }

    fn container_name(name: String, identifier: String) -> String {
        format!("deployd-{}-{}", name, identifier)
    }


    /// Add the service to be run
    pub fn add(&mut self, service: &Service) -> ServerResult<String> {
        let id = Identifier::get(service);
        let name = Runner::container_name(service.name.clone(), id);
        // if we already have a server don't reserve an ip
        if self.desired.get_server(&name).is_some() {
            debug!(server = name, "server already exists for service");
            return Ok(name);
        }
        let ip = self.reserve_ip()?;
        let _ = service.container_config(IpAddr::V4(ip))?;

        let server = Server {
            name: name.clone(),
            addr: Some(SocketAddr::new(ip.into(), service.port)),
            service: service.clone(),
        };
        self.desired.insert(server);
        debug!(server = name, "added server");
        Ok(name)
    }

    pub fn remove(&mut self, name: impl Into<String>) -> ServerResult<bool> {
        Ok(self.desired.remove_service(name))
    }

    /// Remove the running server from being managed
    #[cfg(test)]
    pub fn remove_server(&mut self, name: impl Into<String>) -> ServerResult<bool> {
        let name = name.into();
        Ok(self.desired.remove_server(&name).is_some())
    }
}

/// Execute a Docker command and return the result as an event
pub async fn execute_docker_command(
    docker: &bollard::Docker,
    command: DockerCommand,
) -> DockerEvent {
    use bollard::image::CreateImageOptions;
    use bollard::container::{CreateContainerOptions, StartContainerOptions, StopContainerOptions, RemoveContainerOptions, ListContainersOptions};

    match command {
        DockerCommand::ListContainers => {
            let filters = HashMap::from([("label".to_string(), vec!["managed-by=deployd".to_string()])]);
            let options = Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            });

            match docker.list_containers(options).await {
                Ok(containers) => DockerEvent::ContainersListed { containers },
                Err(e) => DockerEvent::Error {
                    context: "list_containers".to_string(),
                    error: ServerError::DockerError(e),
                },
            }
        }
        DockerCommand::PullImage { image } => {
            let (repo, tag) = image.split_once(':').unwrap_or_else(|| (&image, "latest"));
            let options = CreateImageOptions {
                from_image: image.clone(),
                tag: tag.to_owned(),
                ..Default::default()
            };
            let creds = read_docker_credentials(repo).await;

            match docker
                .create_image(Some(options), None, creds)
                .try_collect::<Vec<_>>()
                .await
            {
                Ok(_) => DockerEvent::ImagePulled { image },
                Err(e) => DockerEvent::Error {
                    context: format!("pull_image: {}", image),
                    error: ServerError::DockerError(e),
                },
            }
        }
        DockerCommand::CreateContainer { name, config } => {
            let options = Some(CreateContainerOptions {
                name: name.as_str(),
                platform: None,
            });

            match docker.create_container(options, config).await {
                Ok(_) => DockerEvent::ContainerCreated { name },
                Err(e) => DockerEvent::Error {
                    context: format!("create_container: {}", name),
                    error: ServerError::DockerError(e),
                },
            }
        }
        DockerCommand::StartContainer { name } => {
            match docker
                .start_container(&name, None::<StartContainerOptions<String>>)
                .await
            {
                Ok(_) => DockerEvent::ContainerStarted { name },
                Err(e) => DockerEvent::Error {
                    context: format!("start_container: {}", name),
                    error: ServerError::DockerError(e),
                },
            }
        }
        DockerCommand::StopContainer { name, timeout } => {
            let options = Some(StopContainerOptions { t: timeout as i64 });

            match docker.stop_container(&name, options).await {
                Ok(_) => DockerEvent::ContainerStopped { name },
                Err(e) => DockerEvent::Error {
                    context: format!("stop_container: {}", name),
                    error: ServerError::DockerError(e),
                },
            }
        }
        DockerCommand::RemoveContainer { name } => {
            let options = Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            });

            match docker.remove_container(&name, options).await {
                Ok(_) => DockerEvent::ContainerRemoved { name },
                Err(e) => DockerEvent::Error {
                    context: format!("remove_container: {}", name),
                    error: ServerError::DockerError(e),
                },
            }
        }
    }
}

async fn read_docker_credentials(repo: impl Into<String>) -> Option<DockerCredentials> {
    let mut home = dirs::home_dir()?;
    home.extend(&[".docker", "config.json"]);
    let contents = fs::read(home).await.ok()?;
    let config: Value = serde_json::from_slice(&contents).ok()?;
    let auths = config.get("auths")?;
    let repo_creds = auths.get(repo.into())?;
    let auth: HashMap<String, String> = serde_json::from_value(repo_creds.clone()).ok()?;
    let auth = auth.get("auth").cloned();
    Some(DockerCredentials {
        auth,
        ..Default::default()
    })
}

#[cfg(test)]
mod test {
    use test_log::test;

    use async_tempfile::TempFile;
    use bollard::secret::Port;
    use tokio::time::sleep;
    use tracing::error;

    use super::*;
    use std::str::FromStr;
    use std::time::Duration;

    #[test(tokio::test)]
    async fn test_env_file_read() {
        let file = TempFile::new().await.expect("couldn't create tempfile");
        let path = file.file_path();
        fs::write(path, "HELLO=WORLD\nYES=no")
            .await
            .expect("couldn't write env file");
        let envs = read_env_file(path).await.expect("couldn't read env file");
        assert_eq!(envs.len(), 2);
        assert_eq!(envs[0], "HELLO=WORLD");
        assert_eq!(envs[1], "YES=no");
    }

    #[test(tokio::test)]
    async fn test_service_with_env_file() {
        let file = TempFile::new().await.expect("couldn't create tempfile");
        let path = file.file_path();
        fs::write(path, "HELLO=WORLD\nYES=no")
            .await
            .expect("couldn't write env file");

        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            ..Default::default()
        };

        let svc = svc
            .with_env_file(path)
            .await
            .expect("couldn't create svc with env file");
        let env = svc.env.expect("service env was empty");
        assert_eq!(env.len(), 2);
        assert_eq!(env[0], "HELLO=WORLD");
        assert_eq!(env[1], "YES=no");
    }

    #[test(tokio::test)]
    async fn test_service_from_file() {
        let env_file = TempFile::new().await.expect("couldn't create tempfile");
        let env_path = env_file.file_path();
        fs::write(env_path, "HELLO=WORLD\nYES=no")
            .await
            .expect("couldn't write env file");

        let file = TempFile::new().await.expect("couldn't create tempfile");
        let path = file.file_path();
        let file_json = serde_json::json!({
            "name": "test",
            "port": 8080,
            "image": "nginx",
            "env_file": env_path,
        });

        fs::write(path, file_json.to_string())
            .await
            .expect("couldn't write service file");

        let read_svc = Service::parse_from_file(path)
            .await
            .expect("couldn't read service");
        assert_eq!(read_svc.name, "test");
        assert_eq!(read_svc.port, 8080);
        assert_eq!(read_svc.image, "nginx");
        let env = read_svc.env.expect("service env was empty");
        assert_eq!(env[0], "HELLO=WORLD");
        assert_eq!(env[1], "YES=no");
    }

    #[test]
    fn test_service_to_config() {
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: Some(vec!["SERVICE=var".to_string(), "ONE=TWO".to_string()]),
            volume_mapping: Some(VolumeMapping {
                source: "/here".to_string(),
                destination: "/there".to_string(),
            }),
        };

        let config = svc
            .container_config(IpAddr::from_str("127.0.0.2").unwrap())
            .expect("couldn't create container config");
        assert_eq!(svc.image, config.image.unwrap());
        assert_eq!(svc.env, config.env);

        let ports = config.exposed_ports.unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.keys().any(|k| k.contains(&svc.port.to_string())));

        let host_config = config.host_config.expect("didn't get host_config");
        assert!(host_config.port_bindings.is_some(), "no port bindings");

        let volumes = host_config.mounts.expect("didn't get mounts");
        assert_eq!(volumes.len(), 1);
        let mapping = svc.volume_mapping.unwrap();
        let source = mapping.source;
        let destination = mapping.destination;
        assert!(volumes
            .iter()
            .any(|v| v.source.clone().unwrap().contains(&source)));
        assert!(volumes
            .iter()
            .any(|v| v.target.clone().unwrap().contains(&destination)));
    }

    #[test]
    fn test_reserve_ip() {
        let mut ips = IpProvisioner::default();
        for i in 2..=254 {
            let actual = ips.reserve_ip().expect("couldn't get ip");
            let expected = Ipv4Addr::new(127, 0, 0, i);
            assert_eq!(actual, expected);
        }

        let ip = ips.reserve_ip();
        if !matches!(ip, Err(ServerError::IpsExhausted)) {
            panic!("wanted IpsExhausted error, got {:#?}", ip);
        }

        ips.release_ip(Ipv4Addr::new(127, 0, 0, 5));
        assert!(ips.reserve_ip().is_ok());
    }

    #[test]
    fn test_identifier() {
        let svc_a = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: None,
            volume_mapping: None,
        };

        let svc_b = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx:latest".to_string(),
            env: None,
            volume_mapping: None,
        };

        let a = Identifier::get(&svc_a);
        let b = Identifier::get(&svc_b);

        assert_ne!(a, b);
        let aa = Identifier::get(&svc_a);
        assert_eq!(a, aa);
    }

    #[test]
    fn test_old_servers() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: None,
            volume_mapping: None,
        };

        let name = runner.add(&svc).expect("couldn't add service");
        let mut running = HashSet::new();
        running.insert(name);
        let found_old = runner.old_servers(&running);
        assert_eq!(found_old.len(), 0);

        let svc_new = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx:1.27.0-alpine".to_string(),
            env: None,
            volume_mapping: None,
        };
        let name_new = runner.add(&svc_new).expect("couldn't add service");
        running.insert(name_new);

        debug!(server = ?runner.desired.servers(), "service associated servers");

        let found_old = runner.old_servers(&running);
        assert_eq!(found_old.len(), 1);
    }

    // Test sans-IO reconciliation logic
    #[test]
    fn test_plan_reconcile_creates_missing_containers() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: None,
            volume_mapping: None,
        };

        runner.add(&svc).expect("couldn't add service");
        runner.request_reconcile();

        // Simulate container list result
        runner.handle_event(
            DockerEvent::ContainersListed { containers: vec![] },
            Instant::now(),
        );

        let actions = runner.plan_reconcile(Instant::now());
        assert_eq!(actions.to_create.len(), 1);
        assert_eq!(actions.to_stop.len(), 0);
    }

    #[test]
    fn test_plan_reconcile_stops_extra_containers() {
        let mut runner = Runner::new().expect("couldn't create runner");
        runner.request_reconcile();

        // Simulate some container running that we don't want
        let mut container = ContainerSummary::default();
        container.names = Some(vec!["/deployd-unknown-abc".to_string()]);
        runner.handle_event(
            DockerEvent::ContainersListed {
                containers: vec![container],
            },
            Instant::now(),
        );

        let actions = runner.plan_reconcile(Instant::now());
        assert_eq!(actions.to_create.len(), 0);
        assert_eq!(actions.to_stop.len(), 1);
    }

    #[test]
    fn test_handle_timeout_stops_old_containers() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let now = Instant::now();

        // Mark a container as replaced
        runner.container_metadata.insert(
            "old-container".to_string(),
            ContainerMetadata {
                created_at: Some(now),
                started_at: Some(now),
                replaced_at: Some(now),
            },
        );

        // Advance time by 31 seconds
        let later = now + Duration::from_secs(31);
        runner.handle_timeout(later);

        // Should have emitted stop command
        let cmd = runner.poll_command();
        assert!(matches!(
            cmd,
            Some(DockerCommand::StopContainer { name, .. }) if name == "old-container"
        ));
    }

    #[test]
    fn test_execute_reconcile_emits_commands() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: None,
            volume_mapping: None,
        };

        let name = runner.add(&svc).expect("couldn't add service");

        let actions = ReconcileActions {
            to_create: vec![name.clone()],
            to_stop: vec![],
            to_remove: vec![],
        };

        runner.execute_reconcile(actions);

        // Should emit: PullImage, CreateContainer, StartContainer
        let cmd1 = runner.poll_command();
        assert!(matches!(cmd1, Some(DockerCommand::PullImage { .. })));

        let cmd2 = runner.poll_command();
        assert!(matches!(cmd2, Some(DockerCommand::CreateContainer { .. })));

        let cmd3 = runner.poll_command();
        assert!(matches!(
            cmd3,
            Some(DockerCommand::StartContainer { name: n }) if n == name
        ));
    }

    #[ignore]
    #[test(tokio::test)]
    async fn test_pull_image_integration() {
        let docker = bollard::Docker::connect_with_defaults().expect("couldn't connect to docker");
        let cmd = DockerCommand::PullImage {
            image: "tianon/toybox".to_string(),
        };
        let event = execute_docker_command(&docker, cmd).await;
        assert!(matches!(event, DockerEvent::ImagePulled { .. }));
    }

    #[ignore]
    #[test(tokio::test)]
    async fn test_run_server_integration() {
        let docker = bollard::Docker::connect_with_defaults().expect("couldn't connect to docker");
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "tianon/toybox".to_string(),
            env: None,
            volume_mapping: None,
        };

        let name = runner.add(&svc).expect("couldn't add service");

        // Execute commands
        let actions = ReconcileActions {
            to_create: vec![name.clone()],
            to_stop: vec![],
            to_remove: vec![],
        };
        runner.execute_reconcile(actions);

        while let Some(cmd) = runner.poll_command() {
            let event = execute_docker_command(&docker, cmd).await;
            runner.handle_event(event, Instant::now());
        }

        // Verify container exists
        runner.request_reconcile();
        let cmd = runner.poll_command().expect("should have list command");
        let event = execute_docker_command(&docker, cmd).await;

        if let DockerEvent::ContainersListed { containers } = event {
            let info = containers
                .into_iter()
                .find(|c| {
                    c.names
                        .as_ref()
                        .and_then(|names| names.first())
                        .map(|n| n.contains(&name))
                        .unwrap_or(false)
                })
                .expect("container not found");

            let image = info.image.expect("no image found");
            assert!(image.contains(&svc.image));

            let container_names = info.names.expect("no names found");
            let container_name = container_names.first().expect("didn't find first name");
            assert!(container_name.contains("test"));
            assert!(container_name.contains("deployd"));

            let ports = info.ports.expect("no ports found");
            let ports = ports
                .into_iter()
                .filter(|p| {
                    if let Some(ip) = &p.ip {
                        return IpAddr::from_str(ip).is_ok_and(|ip| ip.is_loopback());
                    }
                    false
                })
                .collect::<Vec<Port>>();
            assert!(!ports.is_empty());
            let port = ports.first().unwrap();
            assert_eq!(8080, port.private_port, "private port");
            assert_eq!(8080, port.public_port.unwrap(), "public port");
        }

        // Clean up
        let stop_cmd = DockerCommand::StopContainer {
            name: name.clone(),
            timeout: 10,
        };
        execute_docker_command(&docker, stop_cmd).await;

        let remove_cmd = DockerCommand::RemoveContainer { name };
        execute_docker_command(&docker, remove_cmd).await;
    }
}
