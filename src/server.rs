use bollard::container::Config;
use bollard::container::CreateContainerOptions;
use bollard::container::ListContainersOptions;
use bollard::container::NetworkingConfig;
use bollard::container::RemoveContainerOptions;
use bollard::container::StartContainerOptions;
use bollard::container::StopContainerOptions;
use bollard::secret::ContainerSummary;
use bollard::secret::EndpointSettings;
use bollard::secret::HostConfig;
use bollard::secret::Mount;
use bollard::secret::MountTypeEnum;
use bollard::secret::PortBinding;
use bollard::secret::RestartPolicy;
use bollard::secret::RestartPolicyNameEnum;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use thiserror::Error;
use tokio::{fs, io};

use std::collections::BTreeSet;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Server {
    pub name: String,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Default)]
pub struct Service {
    pub name: String,
    pub port: u16,
    pub image: String,
    pub env: Option<Vec<String>>,
    pub volume_mapping: Option<VolumeMapping>,
}

#[derive(Debug, Clone)]
pub struct VolumeMapping {
    pub source: String,
    pub destination: String,
}

impl Display for VolumeMapping {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}:{}", self.source, self.destination)
    }
}

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
}

type ServerResult<T> = Result<T, ServerError>;

impl Service {
    pub async fn with_env_file(self, path: impl AsRef<Path>) -> ServerResult<Self> {
        let env = read_env_file(path).await?;
        let mut ret = self.clone();
        ret.env = Some(env);
        Ok(ret)
    }

    pub fn container_config(&self, ip: Ipv4Addr) -> ServerResult<Config<String>> {
        let empty_object = HashMap::new();
        // "ExposedPorts": {
        //     "80/tcp": {}
        // },
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

        // "PortBindings": {
        //     "80/tcp": [
        //         {
        //             "HostIp": "127.0.0.2",
        //             "HostPort": "80"
        //         }
        //     ]
        // },
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
    let lines = read.split("\n").map(|l| l.to_owned()).collect::<Vec<_>>();

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

#[derive(Debug, Clone)]
pub struct Runner {
    ips: IpProvisioner,
    containers: HashMap<String, Server>,
    docker: bollard::Docker,
}

impl Runner {
    pub fn new() -> ServerResult<Self> {
        let docker = bollard::Docker::connect_with_defaults()?;
        let containers = HashMap::default();
        let ips = IpProvisioner::default();

        Ok(Self {
            ips,
            docker,
            containers,
        })
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

    pub async fn run(&mut self, service: &Service) -> ServerResult<String> {
        let id = "1";
        let name = Runner::container_name(service.name.clone(), id.into());
        let options = Some(CreateContainerOptions {
            name: &name,
            platform: None,
        });
        let ip = self.reserve_ip()?;
        let config = service.container_config(ip)?;
        self.docker.create_container(options, config).await?;
        self.docker
            .start_container(&name, None::<StartContainerOptions<String>>)
            .await?;
        let server = Server {
            name: name.clone(),
            addr: SocketAddr::new(ip.into(), service.port),
        };
        self.containers.insert(name.clone(), server);
        Ok(name)
    }

    /// Stop with the given name. Return whether or not it was actually stopped
    pub async fn stop(&mut self, name: String) -> ServerResult<bool> {
        if self.find(&name).await.is_none() {
            self.remove_container(&name);
            return Ok(false)
        }

        // TODO: there's probably an issue if the container fails to stop or remove...
        let options = Some(StopContainerOptions {
            t: 30,
        });
        self.docker.stop_container(&name, options).await?;
        let options = Some(RemoveContainerOptions {
            force: true,
            ..Default::default()
        });
        self.docker.remove_container(&name, options).await?;

        Ok(self.remove_container(&name))
    }

    fn remove_container(&mut self, name: impl ToString) -> bool {
        let name = name.to_string();
        if let Some(s) = self.containers.remove(&name) {
            if let IpAddr::V4(ip) = s.addr.ip() {
                self.release_ip(ip);
            }
            return true
        }
        false
    }

    pub async fn find(&self, name: &String) -> Option<ContainerSummary> {
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
}

#[cfg(test)]
mod test {
    use async_tempfile::TempFile;
    use bollard::secret::Port;

    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn env_file_read() {
        let file = TempFile::new().await.expect("couldn't create tempfile");
        let path = file.file_path();
        fs::write(path, "HELLO=WORLD\nYES=no")
            .await
            .expect("couldn't write env file");
        let envs = read_env_file(path).await.expect("couldn't read env file");
        assert_eq!(envs[0], "HELLO=WORLD");
        assert_eq!(envs[1], "YES=no");
    }

    #[tokio::test]
    async fn service_with_env_file() {
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

    #[test]
    fn service_to_config() {
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
            .container_config(Ipv4Addr::from_str("127.0.0.2").unwrap())
            .expect("couldn't create container config");
        assert_eq!(svc.image, config.image.unwrap());
        assert_eq!(svc.env, config.env);
        let ports = config.exposed_ports.unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.keys().any(|k| k.contains(&svc.port.to_string())));

        let volumes = config.volumes.unwrap();
        assert_eq!(volumes.len(), 1);
        let mapping = svc.volume_mapping.unwrap();
        let source = mapping.source;
        let destination = mapping.destination;

        assert!(volumes.keys().any(|k| k.contains(&source)));
        assert!(volumes.keys().any(|k| k.contains(&destination)));
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

    #[ignore]
    #[tokio::test]
    async fn run_service() {
        let mut runner = Runner::new().expect("couldn't create runner");
        let svc = Service {
            name: "test".to_string(),
            port: 8080,
            image: "nginx".to_string(),
            env: None,
            volume_mapping: None,
        };
        let name = runner.run(&svc).await.unwrap();
        // let docker = bollard::Docker::connect_with_defaults().expect("couldn't create test docker");
        let info = runner.find(&name).await.expect("error finding container");
        let image = info.image.expect("no image found");
        assert!(image.contains("nginx"));

        let container_names = info.names.expect("no names found");
        let container_name = container_names.first().expect("didn't find first name");
        assert!(container_name.contains("test"));
        assert!(container_name.contains("deployd"));

        let ports = info.ports.expect("no ports found");
        let ports = ports.into_iter().filter(|p| {
            if let Some(ip) = &p.ip {
                return IpAddr::from_str(ip).is_ok_and(|ip| ip.is_loopback());
            }
            false
        })
        .collect::<Vec<Port>>();
        assert!(ports.len() > 0);
        let port = ports.first().unwrap();
        assert_eq!(8080, port.private_port, "private port");
        assert_eq!(8080, port.public_port.unwrap(), "public port");

        runner.stop(name).await.unwrap();
    }
}
