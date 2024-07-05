use anyhow::Result;
use tokio::fs::{self};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};

use futures::{future, FutureExt, StreamExt};
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tokio_stream::wrappers::{IntervalStream, TcpListenerStream};

#[derive(Debug, Clone)]
pub struct Server {
    addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let server = config_server().await.expect("couldn't read server file");
    let active_server: Arc<Mutex<Server>> = Arc::new(Mutex::new(server));
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
            let x = active.lock().await.addr.clone();
            x
        };

        let mut outbound = TcpStream::connect(addr)
            .await
            .expect("couldn't connect to server");
        let mut inbound = res.expect("invalid accept?");
        copy_bidirectional(&mut inbound, &mut outbound)
            .map(|r| {
                if let Err(e) = r {
                    println!("Failed to transfer; error={}", e);
                }
            })
            .await
    });
    future::select(Box::pin(config_stream), Box::pin(handler_stream)).await;

    Ok(())
}

async fn config_server() -> Result<Server> {
    let read = fs::read_to_string("./server").await?;
    let server_addr = read.trim();
    Ok(Server {
        addr: server_addr.into(),
    })
}
