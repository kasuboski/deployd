use anyhow::Result;
use server::Runner;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::fs::{self};
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
    let runner_reconcile = runner.clone();
    let reconcile_interval = Duration::from_millis(500);
    let reconcile_stream = IntervalStream::new(time::interval(reconcile_interval))
        .for_each(|_| {
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

    let server = config_server().await.expect("couldn't read server file");
    let active_server: Arc<Mutex<SocketAddr>> = Arc::new(Mutex::new(server));
    let config_stream =
        IntervalStream::new(time::interval(Duration::from_secs(5))).for_each(|_| async {
            let server = config_server().await.expect("couldn't read server file");
            let mut guard = active_server.lock().await;
            *guard = server;
        });

    let listener = TcpListener::bind(listen_addr).await?;
    let listener_stream = TcpListenerStream::new(listener);
    let handler_stream = listener_stream.for_each_concurrent(None, |res| async {
        let addr = {
            let active = active_server.clone();
            let x = active.lock().await.clone();
            x
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
    });
    future::join3(
        Box::pin(config_stream),
        Box::pin(handler_stream),
        Box::pin(reconcile_stream),
    )
    .await;

    Ok(())
}

async fn config_server() -> Result<SocketAddr> {
    let read = fs::read_to_string("./server").await?;
    let server_addr = read.trim();
    let server_addr = SocketAddr::from_str(server_addr)?;
    Ok(server_addr)
}
