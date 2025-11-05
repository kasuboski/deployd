use anyhow::Result;
use server::{execute_docker_command, Runner, Service};
use std::time::Instant;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use std::env;
use tokio::time::{self, Duration};

mod server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "deployd=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut runner = Runner::new().expect("couldn't create runner");
    let docker = bollard::Docker::connect_with_defaults()?;

    // Load initial config
    let svc = Service::parse_from_file("./server.json")
        .await
        .expect("couldn't parse service");
    runner.add(&svc).expect("couldn't add service");
    let current_service = svc.name.clone();

    // Start reconciliation
    runner.request_reconcile();

    // Set up timers
    let mut config_check_interval = time::interval(Duration::from_secs(5));
    let mut reconcile_interval = time::interval(Duration::from_millis(500));

    // Set up TCP listener for proxying
    let listener = TcpListener::bind(&listen_addr).await?;
    debug!("Listening on {}", listen_addr);

    loop {
        // Process all pending commands from the state machine
        while let Some(cmd) = runner.poll_command() {
            let event = execute_docker_command(&docker, cmd).await;
            runner.handle_event(event, Instant::now());
        }

        // Check if reconciliation produced actions
        let actions = runner.plan_reconcile(Instant::now());
        if actions.to_create.len() > 0 || actions.to_stop.len() > 0 || actions.to_remove.len() > 0
        {
            debug!(
                "Reconcile actions: create={}, stop={}, remove={}",
                actions.to_create.len(),
                actions.to_stop.len(),
                actions.to_remove.len()
            );
            runner.execute_reconcile(actions);
            continue; // Process commands immediately
        }

        // Calculate next timeout
        let next_timeout = runner.poll_timeout().unwrap_or_else(|| {
            Instant::now() + Duration::from_secs(3600) // 1 hour default
        });

        // Wait for next event
        tokio::select! {
            // Config file check interval
            _ = config_check_interval.tick() => {
                match Service::parse_from_file("./server.json").await {
                    Ok(svc) => {
                        let prev_service = current_service.clone();
                        if svc.name != prev_service {
                            runner.remove(prev_service).ok();
                        }
                        runner.add(&svc).ok();
                    }
                    Err(e) => {
                        error!(error = ?e, "failed to parse config");
                    }
                }
            }

            // Reconcile interval
            _ = reconcile_interval.tick() => {
                runner.request_reconcile();
            }

            // Timeout for time-based actions
            _ = time::sleep_until(tokio::time::Instant::from_std(next_timeout)) => {
                runner.handle_timeout(Instant::now());
            }

            // New TCP connection
            Ok((inbound, _addr)) = listener.accept() => {
                let service_name = current_service.clone();
                let addr = runner.latest_server_for_service(&service_name)
                    .and_then(|s| s.addr);

                if let Some(backend_addr) = addr {
                    tokio::spawn(async move {
                        match TcpStream::connect(backend_addr).await {
                            Ok(mut outbound) => {
                                let mut inbound = inbound;
                                if let Err(e) = copy_bidirectional(&mut inbound, &mut outbound).await {
                                    error!(error = %e, "Failed to transfer");
                                }
                            }
                            Err(e) => {
                                error!(error = %e, backend = %backend_addr, "Failed to connect to backend");
                            }
                        }
                    });
                } else {
                    error!("No backend available for service {}", service_name);
                }
            }
        }
    }
}
