#![deny(clippy::all, clippy::pedantic, clippy::nursery)]
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use warp::Filter;

const TCP_PORT: u16 = 12000;
const WEBSOCKET_PORT: u16 = 8080;

#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel::<Vec<u8>>(16);
    let tx = Arc::new(tx);

    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{TCP_PORT}"))
        .await
        .expect("Failed to bind TCP listener");

    println!("TCP listener started on 0.0.0.0:{TCP_PORT}");

    // Main TCP listener loop
    let tcp_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((stream, addr)) => {
                    println!("New TCP connection from {addr}");
                    // For every TCP connection swamp a task
                    let connection_tx = tcp_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tcp_connection(stream, addr, connection_tx).await {
                            eprintln!("Error handling TCP connection from {addr}: {e}");
                        }
                        println!("TCP connection from {addr} closed");
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting TCP connection: {e}");
                    // Wait a bit before retrying
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    });

    // WebSocket route
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(with_broadcast(tx.clone()))
        .map(|ws: warp::ws::Ws, tx| ws.on_upgrade(move |socket| client_connection(socket, tx)));

    // Add CORS support
    let routes = ws_route.with(warp::cors().allow_any_origin());
    println!("Starting WebSocket server on 0.0.0.0:{WEBSOCKET_PORT}");
    // WebSocket - run server
    warp::serve(routes)
        .run(([0, 0, 0, 0], WEBSOCKET_PORT))
        .await;
}

/// TCP connection handler
async fn handle_tcp_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    tx: Arc<broadcast::Sender<Vec<u8>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = vec![0u8; 1024];

    loop {
        // Read data from TCP stream
        let bytes_read = stream.read(&mut buf).await?;

        if bytes_read == 0 {
            // Client disconnected
            break;
        }

        // Received data
        let received_data = buf[..bytes_read].to_vec();
        println!("Received {bytes_read} bytes from {addr}");

        // Send data to broadcast channel
        let _ = tx.send(received_data);
    }

    Ok(())
}

/// Helper function to create a warp filter for the broadcast channel
fn with_broadcast(
    tx: Arc<broadcast::Sender<Vec<u8>>>,
) -> impl Filter<Extract = (Arc<broadcast::Sender<Vec<u8>>>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || tx.clone())
}

/// WebSocket client connection handler
#[allow(clippy::unused_async)]
async fn client_connection(
    ws: warp::ws::WebSocket,
    tx: Arc<broadcast::Sender<Vec<u8>>>, // Тип изменен на Vec<u8>
) {
    // Split the WebSocket into sender and receiver
    let (mut ws_tx, mut _ws_rx) = ws.split();
    // Subscribe to the broadcast channel
    let mut rx = tx.subscribe();

    println!("New WebSocket client connected");

    // Spawn a task to handle incoming messages from the broadcast channel
    tokio::spawn(async move {
        // Loop to receive messages from the broadcast channel
        while let Ok(msg_bytes) = rx.recv().await {
            // Send the received message to the WebSocket client
            if ws_tx
                .send(warp::ws::Message::binary(msg_bytes))
                .await
                .is_err()
            {
                // If sending fails, the client is likely disconnected≠
                break;
            }
        }
        println!("WebSocket client disconnected");
    });
}
