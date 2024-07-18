use std::collections::HashMap;

use std::collections::hash_map::{Keys, Values};

#[derive(Debug, Clone, Default)]
pub(crate) struct DesiredState {
    servers: HashMap<String, Server>,
    // map service name to container/server names
    services: HashMap<String, Vec<String>>,
}

use super::Server;
impl DesiredState {
    pub fn server_names(&self) -> Keys<String, Server> {
        self.servers.keys()
    }

    pub fn get_server(&self, name: impl Into<String>) -> Option<&Server> {
        self.servers.get(&name.into())
    }

    #[allow(dead_code)]
    pub fn get_mut_server(&mut self, name: impl Into<String>) -> Option<&mut Server> {
        self.servers.get_mut(&name.into())
    }

    pub fn servers(&self) -> Values<String, Vec<String>> {
        self.services.values()
    }

    pub fn insert(&mut self, server: Server) {
        let server_name = server.name.clone();
        let service_name = server.service.name.clone();

        let server_mapping = server_name.clone();
        let server_mapping_default = server_name.clone();
        self.servers.insert(server_name, server);
        self.services
            .entry(service_name)
            .and_modify(|server_names| {
                if !server_names.iter().any(|n| *n == server_mapping) {
                    server_names.push(server_mapping);
                }
            })
            .or_insert_with(|| {
                let servers = vec![server_mapping_default];
                servers
            });
    }

    pub fn servers_for_service(&self, service_name: impl Into<String>) -> Option<Vec<Server>> {
        let service_name = service_name.into();
        let server_names = self.services.get(&service_name)?;

        let mut servers: Vec<Server> = Vec::with_capacity(server_names.len());
        for server_name in server_names {
            let server = self.get_server(server_name)?;
            servers.push(server.clone());
        }
        Some(servers)
    }

    pub fn remove_server(&mut self, server_name: impl Into<String>) -> Option<Server> {
        let server_name = server_name.into();

        let server = self.servers.remove(&server_name)?;
        let service_name = &server.service.name;
        self.services
            .entry(service_name.into())
            .and_modify(|server_names| {
                server_names.retain(|n| n != &server_name);
            });
        Some(server)
    }

    pub fn remove_service(&mut self, service_name: impl Into<String>) -> bool {
        let service_name = service_name.into();

        let server_names = if let Some(ns) = self.services.get(&service_name) {
            ns
        } else {
            return false;
        };
        for name in server_names {
            self.servers.remove(name);
        }

        self.services.remove(&service_name).is_some()
    }
}

#[cfg(test)]
mod test {
    use crate::server::Service;

    use super::*;
    #[test]
    fn test_insert() {
        let mut state = DesiredState::default();
        let svc = Service {
            name: "test".into(),
            port: 8080,
            image: "nginx".into(),
            env: None,
            volume_mapping: None,
        };
        let server = Server {
            name: "test-1".into(),
            addr: None,
            service: svc,
        };

        state.insert(server);
        assert!(state.get_server("test-1").is_some());
        assert!(state.servers().any(|ns| ns.contains(&"test-1".to_owned())));
    }

    #[test]
    fn test_remove_server() {
        let mut state = DesiredState::default();
        let svc = Service {
            name: "test".into(),
            port: 8080,
            image: "nginx".into(),
            env: None,
            volume_mapping: None,
        };
        let server = Server {
            name: "test-1".into(),
            addr: None,
            service: svc.clone(),
        };

        state.insert(server);
        assert!(state.get_server("test-1").is_some());
        assert!(state.servers().any(|ns| ns.contains(&"test-1".to_owned())));

        let server_b = Server {
            name: "test-2".into(),
            addr: None,
            service: svc.clone(),
        };
        state.insert(server_b);
        assert!(state.get_server("test-2").is_some());
        assert!(state.servers().any(|ns| ns.contains(&"test-2".to_owned())));

        assert!(state.remove_server("test-1").is_some());
        assert!(!state.servers().any(|ns| ns.contains(&"test-1".to_owned())));

        assert!(state.remove_server("test-2").is_some());
        assert!(!state.servers().any(|ns| ns.contains(&"test-2".to_owned())));

        assert_eq!(state.server_names().count(), 0);
    }

    #[test]
    fn test_service_server_ordering() {
        let mut state = DesiredState::default();
        let svc = Service {
            name: "b".into(),
            port: 8080,
            image: "nginx".into(),
            env: None,
            volume_mapping: None,
        };
        let server = Server {
            name: "b-1".into(),
            addr: None,
            service: svc.clone(),
        };

        state.insert(server);

        let server_a = Server {
            name: "a-1".into(),
            addr: None,
            service: svc.clone(),
        };
        state.insert(server_a);

        let servers = state
            .servers_for_service("b")
            .expect("didn't get servers back");

        let latest_server = servers.last().expect("couldn't get last server");
        assert_eq!(latest_server.name, "a-1");

        let older_server = servers.first().expect("couldn't get first server");
        assert_eq!(older_server.name, "b-1");
    }
}
