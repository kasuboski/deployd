use std::collections::BTreeSet;
use std::collections::HashMap;

use std::collections::hash_map::{Keys, Values};

#[derive(Debug, Clone, Default)]
pub(crate) struct DesiredState {
    servers: HashMap<String, Server>,
    // map service name to container/server names
    services: HashMap<String, BTreeSet<String>>,
}

use super::Server;
impl DesiredState {
    pub fn server_names(&self) -> Keys<String, Server> {
        self.servers.keys()
    }

    pub fn get_server(&self, name: impl Into<String>) -> Option<&Server> {
        self.servers.get(&name.into())
    }

    pub fn get_mut_server(&mut self, name: impl Into<String>) -> Option<&mut Server> {
        self.servers.get_mut(&name.into())
    }

    pub fn servers(&self) -> Values<String, BTreeSet<String>> {
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
                server_names.insert(server_mapping);
            })
            .or_insert_with(|| {
                let mut set = BTreeSet::default();
                set.insert(server_mapping_default);
                set
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
                server_names.remove(&server_name);
            });
        Some(server)
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
        assert!(state.servers().any(|ns| ns.contains("test-1")));
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
        assert!(state.servers().any(|ns| ns.contains("test-1")));

        let server_b = Server {
            name: "test-2".into(),
            addr: None,
            service: svc.clone(),
        };
        state.insert(server_b);
        assert!(state.get_server("test-2").is_some());
        assert!(state.servers().any(|ns| ns.contains("test-2")));

        assert!(state.remove_server("test-1").is_some());
        assert!(!state.servers().any(|ns| ns.contains("test-1")));

        assert!(state.remove_server("test-2").is_some());
        assert!(!state.servers().any(|ns| ns.contains("test-2")));

        assert_eq!(state.server_names().count(), 0);
    }
}
