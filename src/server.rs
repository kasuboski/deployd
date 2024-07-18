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

use fasthash::MetroHasher;
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

pub mod desired_state;

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
    let lines = read.split('\n').map(|l| l.to_owned()).collect::<Vec<_>>();

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
        use std::hash::{Hash, Hasher};
        let mut h = MetroHasher::default();
        svc.hash(&mut h);
        let ret = h.finish();
        bs58::encode(ret.to_le_bytes()).into_string()
    }
}

#[derive(Debug, Clone)]
pub struct Runner {
    ips: IpProvisioner,
    desired: DesiredState,
    docker: bollard::Docker,
}

impl Runner {
    pub fn new() -> ServerResult<Self> {
        let docker = bollard::Docker::connect_with_defaults()?;
        let desired = DesiredState::default();
        let ips = IpProvisioner::default();

        Ok(Self {
            ips,
            docker,
            desired,
        })
    }

    pub async fn reconcile(&mut self) -> ServerResult<()> {
        // List containers we are managing
        let mut container_names: HashSet<String> = self
            .list_containers()
            .await?
            .into_iter()
            .filter_map(|cs| {
                cs.names?
                    .first()
                    .map(|n| n.trim().trim_start_matches('/').to_string())
            })
            .collect();
        trace!(containers = ?container_names.iter().cloned().collect::<Vec<String>>(), "found containers");

        // find services with multiple servers
        // if new server is running delete the old one(s)
        let to_remove = self.old_servers(&container_names);
        if !to_remove.is_empty() {
            debug!(servers = ?to_remove, "found old versions of servers to remove");
        }
        for s in to_remove {
            self.desired.remove_server(s);
        }

        // compare with desired containers
        let mut not_found: Vec<String> = Vec::with_capacity(container_names.len());
        for name in self.desired.server_names() {
            let found = container_names.remove(name);
            if !found {
                not_found.push(name.to_string())
            }
        }
        // stop extras
        for n in &container_names {
            debug!(container = n, "stopping container");
            self.stop(n).await?;
        }
        // create nonexistent
        for nf in &not_found {
            if self.desired.get_server(nf).is_none() {
                // This shouldn't happen as we specifically iterated through containers
                // to get the not_found
                continue;
            };
            debug!(container = nf, "starting container");
            self.run_server(nf).await?;
        }

        Ok(())
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

    async fn run_server(&mut self, name: impl Into<String>) -> ServerResult<()> {
        let name = name.into();
        let server = self
            .desired
            .get_server(&name)
            .ok_or(ServerError::ServerNotFound(name.clone()))?;

        let options = Some(CreateContainerOptions {
            name: &name,
            platform: None,
        });
        let ip = server.addr.ok_or(ServerError::ServerMissingIP)?.ip();
        let config = server.service.container_config(ip)?;

        let _ = self.pull_image(server.service.image.clone()).await;
        self.docker.create_container(options, config).await?;
        self.docker
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await?;
        Ok(())
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

    /// Stop with the given name. Return whether or not it was actually stopped
    async fn stop(&mut self, name: impl Into<String>) -> ServerResult<bool> {
        let name = name.into();
        if self.find_container(&name).await.is_none() {
            self.remove_container(&name);
            return Ok(false);
        }

        // TODO: there's probably an issue if the container fails to stop or remove...
        let options = Some(StopContainerOptions { t: 30 });
        self.docker.stop_container(&name, options).await?;
        let options = Some(RemoveContainerOptions {
            force: true,
            ..Default::default()
        });
        self.docker.remove_container(&name, options).await?;

        Ok(self.remove_container(&name))
    }

    fn remove_container(&mut self, name: impl Into<String>) -> bool {
        let name = name.into();
        if let Some(s) = self.desired.remove_server(&name) {
            if let Some(addr) = s.addr {
                if let IpAddr::V4(ip) = addr.ip() {
                    self.release_ip(ip);
                }
            }
            return true;
        }
        false
    }

    async fn find_container(&self, name: &String) -> Option<ContainerSummary> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("name".to_string(), vec![name.to_string()]);

        let options = Some(ListContainersOptions {
            // only list running
            all: false,
            filters,
            ..Default::default()
        });
        let containers = self.docker.list_containers(options).await.ok()?;
        if containers.len() != 1 {
            // warn!("more than one container found for name");
            return None;
        }

        containers.first().cloned()
    }

    async fn list_containers(&self) -> ServerResult<Vec<ContainerSummary>> {
        let filters =
            HashMap::from([("label".to_string(), vec!["managed-by=deployd".to_string()])]);
        let options = Some(ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        });
        let containers = self.docker.list_containers(options).await?;

        Ok(containers)
    }

    async fn pull_image(&self, image: impl Into<String>) -> ServerResult<()> {
        let image = image.into();
        // This probably won't correctly for shas
        let (repo, tag) = image.split_once(':').unwrap_or_else(|| (&image, "latest"));
        let options = CreateImageOptions {
            from_image: image.clone(),
            tag: tag.to_owned(),
            ..Default::default()
        };
        let creds = read_docker_credentials(repo).await;
        let stream = self.docker.create_image(Some(options), None, creds);
        stream.try_collect::<Vec<_>>().await?;
        Ok(())
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

    #[ignore]
    #[test(tokio::test)]
    async fn test_pull_image() {
        let runner = Runner::new().expect("couldn't create runner");
        runner
            .pull_image("tianon/toybox")
            .await
            .expect("couldn't pull image");
    }

    #[ignore]
    #[test(tokio::test)]
    async fn test_run_server() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "tianon/toybox".to_string(),
            env: None,
            volume_mapping: None,
        };

        let name = runner.add(&svc).expect("couldn't add service");
        runner.run_server(&name).await.unwrap();
        let info = runner
            .find_container(&name)
            .await
            .expect("error finding container");
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

        runner.stop(name).await.unwrap();
    }

    #[ignore]
    #[test(tokio::test)]
    async fn test_list_containers() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: None,
            volume_mapping: None,
        };
        let name = runner.add(&svc).expect("couldn't add server");
        runner.run_server(&name).await.unwrap();
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 1);

        runner.stop(name).await.unwrap();
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 0);
    }

    #[ignore]
    #[test(tokio::test)]
    async fn test_reconcile() {
        let mut runner = Runner::new().expect("couldn't create runner");
        runner.reconcile().await.expect("couldn't reconcile");
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 0);

        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "busybox".to_string(),
            env: None,
            volume_mapping: None,
        };
        let name = runner.add(&svc).expect("couldn't add service");
        runner.reconcile().await.expect("couldn't reconcile");
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 1);

        runner.remove_server(&name).expect("couldn't remove server");
        for _ in 0..40 {
            let _ = runner
                .reconcile()
                .await
                .inspect_err(|e| error!(error = %e, "couldn't reconcile"));
            sleep(Duration::from_secs(1)).await;
            if let Ok(containers) = runner.list_containers().await {
                if containers.is_empty() {
                    break;
                }
            }
        }

        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 0);

        let name = runner.add(&svc).expect("couldn't add service");
        runner.reconcile().await.expect("couldn't reconcile");
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 1);

        let name_2 = runner.add(&svc).expect("couldn't add service");
        assert_eq!(name, name_2);

        runner.reconcile().await.expect("couldn't reconcile");
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 1);

        let svc_2 = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx:1.27.0-alpine".to_string(),
            env: None,
            volume_mapping: None,
        };

        let name_2 = runner.add(&svc_2).expect("couldn't add service");
        assert_ne!(name, name_2);

        runner.reconcile().await.expect("couldn't reconcile");
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 2);

        runner.reconcile().await.expect("couldn't reconcile");
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 1);
        assert!(containers
            .into_iter()
            .filter_map(|cs| {
                cs.names?
                    .first()
                    .map(|n| n.trim().trim_start_matches('/').to_string())
            })
            .any(|c| c.contains(&name_2)));
        let active_server = runner
            .latest_server_for_service("test")
            .expect("didn't find active server");
        assert_eq!(active_server.name, name_2);

        runner.remove_server(&name).expect("couldn't remove server");
        for _ in 0..40 {
            let _ = runner
                .reconcile()
                .await
                .inspect_err(|e| error!(error = %e, "couldn't reconcile"));
            sleep(Duration::from_secs(1)).await;
            if let Ok(containers) = runner.list_containers().await {
                if containers.len() == 1 {
                    break;
                }
            }
        }
        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 1);

        runner
            .remove_server(&name_2)
            .expect("couldn't remove server");
        for _ in 0..40 {
            let _ = runner
                .reconcile()
                .await
                .inspect_err(|e| error!(error = %e, "couldn't reconcile"));
            sleep(Duration::from_secs(1)).await;
            if let Ok(containers) = runner.list_containers().await {
                if containers.is_empty() {
                    break;
                }
            }
        }

        let containers = runner.list_containers().await.unwrap();
        assert_eq!(containers.len(), 0);
    }
}
