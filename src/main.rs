use anyhow::Result;
use server::{Runner, Service};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tracing::error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use futures::{future, FutureExt, StreamExt};
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tokio_stream::wrappers::{IntervalStream, TcpListenerStream};

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

    let runner = Arc::new(Mutex::new(Runner::new().expect("couldn't create runner")));

    // make sure we read config before doing anything
    let svc = Service::parse_from_file("./server.json")
        .await
        .expect("couldn't parse service");
    {
        let mut guard = runner.lock().await;
        guard.add(&svc).expect("couldn't add service");
    }
    let initial_service = svc.name;
    let (service_tx, service_rx) = tokio::sync::watch::channel(initial_service);

    let config_stream =
        IntervalStream::new(time::interval(Duration::from_secs(5))).for_each(|_| async {
            let svc = Service::parse_from_file("./server.json")
                .await
                .expect("couldn't parse service");
            {
                let mut guard = runner.lock().await;
                guard.add(&svc).expect("couldn't add service");
            }
            let name = svc.name.clone();
            let _ = service_tx.send(name);

            let remove_svc = {
                let prev_name = service_rx.borrow().clone();

                if prev_name != svc.name {
                    Some(prev_name)
                } else {
                    None
                }
            };
            if let Some(prev_svc_name) = remove_svc {
                let mut guard = runner.lock().await;
                guard
                    .remove(prev_svc_name)
                    .expect("couldn't remove service");
            }
        });

    let runner_reconcile = runner.clone();
    let reconcile_interval = Duration::from_millis(500);
    let reconcile_stream = IntervalStream::new(time::interval(reconcile_interval)).for_each(|_| {
        let value = &runner_reconcile;
        async move {
            let runner = value.clone();
            let mut guard = runner.lock().await;
            match guard.reconcile().await {
                Ok(_) => (),
                Err(e) => error!(error = ?e, "error during reconcile"),
            };
        }
    });

    let listener = TcpListener::bind(listen_addr).await?;
    let listener_stream = TcpListenerStream::new(listener);
    let handler_stream = listener_stream.for_each_concurrent(None, |res| {
        let mut tcp_service_rx = service_rx.clone();
        let runner = runner.clone();
        async move {
            let addr = loop {
                let service = tcp_service_rx.borrow_and_update().clone();
                {
                    let guard = runner.lock().await;
                    if let Some(server) = guard.latest_server_for_service(service) {
                        break server.addr;
                    }
                }

                if tcp_service_rx.changed().await.is_err() {
                    return;
                }
            };

            let addr = if let Some(a) = addr {
                a
            } else {
                return;
            };

            let mut outbound = TcpStream::connect(addr)
                .await
                .expect("couldn't connect to server");
            let mut inbound = res.expect("invalid accept?");
            copy_bidirectional(&mut inbound, &mut outbound)
                .map(|r| {
                    if let Err(e) = r {
                        error!(error = %e, "Failed to transfer");
                    }
                })
                .await
        }
    });
    future::join3(
        Box::pin(config_stream),
        Box::pin(handler_stream),
        Box::pin(reconcile_stream),
    )
    .await;

    Ok(())
}
