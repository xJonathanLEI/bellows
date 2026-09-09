//! Application glue for callback-free INSERTs, not a Bellows publishing backend.
//! Own and await the driver: returning from a request must not abandon socket I/O.

use tokio::sync::oneshot;
use tokio_postgres::{
    Client, Config,
    config::{Host, SslMode},
};
use worker::{SecureTransport, Socket, postgres_tls::PassthroughTls};

pub struct Connection {
    client: Option<Client>,
    driver: oneshot::Receiver<Result<(), tokio_postgres::Error>>,
    closed: Option<Result<(), &'static str>>,
}

impl Connection {
    pub async fn connect(hyperdrive_url: &str) -> Result<Self, &'static str> {
        // Only a real env.hyperdrive("HYPERDRIVE") connection string reaches this helper.
        let config: Config = hyperdrive_url
            .parse()
            .map_err(|_| "invalid Hyperdrive URL")?;
        let host = match config.get_hosts() {
            [Host::Tcp(host)] if !host.is_empty() => host,
            _ => return Err("Hyperdrive requires one TCP host"),
        };
        let port = match config.get_ports() {
            [] => 5432,
            [port] if *port != 0 => *port,
            _ => return Err("Hyperdrive requires one TCP port"),
        };
        let transport = match config.get_ssl_mode() {
            SslMode::Disable => SecureTransport::Off,
            _ => SecureTransport::StartTls,
        };
        let socket = Socket::builder()
            .secure_transport(transport)
            .connect(host.clone(), port)
            .map_err(|_| "could not open Hyperdrive socket")?;
        let (client, connection) = config
            .connect_raw(socket, PassthroughTls)
            .await
            .map_err(|_| "could not connect to Hyperdrive")?;
        let (send, driver) = oneshot::channel();
        worker::wasm_bindgen_futures::spawn_local(async move {
            let result = connection.await;
            if result.is_err() {
                worker::console_error!("application PostgreSQL driver failed");
            }
            let _ = send.send(result);
        });
        Ok(Self {
            client: Some(client),
            driver,
            closed: None,
        })
    }

    pub fn client(&self) -> &Client {
        self.client.as_ref().expect("connection must still be open")
    }

    // Borrow the driver so an aborted processing future cannot discard a pending close.
    pub async fn close(&mut self) -> Result<(), &'static str> {
        if let Some(result) = self.closed {
            return result;
        }
        self.client.take();
        let result = (&mut self.driver)
            .await
            .map_err(|_| "application PostgreSQL driver exited without a result")
            .and_then(|result| result.map_err(|_| "application PostgreSQL shutdown failed"));
        self.closed = Some(result);
        result
    }
}
