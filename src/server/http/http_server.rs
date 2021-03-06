use super::*;
use warp::reply::Reply;

/// Uses Warp to spin up an HTTP server. At time of writing this is only used to initialize WebRTC,
/// but it accepts an arbitrary Warp filter and so could easily be used for whatever else we
/// needed.
pub struct HttpServer {
    name: String,
    socket_addr: SocketAddr,
    shutdown_tx: Option<futures::channel::oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

fn redirect_request_to_https(
    authority: Option<warp::host::Authority>,
    path: warp::path::FullPath,
    query: String,
) -> Box<dyn warp::Reply> {
    warn!(
        "redirecting to hard-coded path, request path: {}",
        path.as_str()
    );
    let authority = match authority {
        Some(a) => a,
        None => {
            warn!("could not redirect to HTTPS: no authority");
            return Box::new(warp::http::status::StatusCode::NOT_FOUND.into_response())
                as Box<dyn warp::Reply>;
        }
    };
    let path_and_query_str = if query.is_empty() {
        path.as_str().to_string()
    } else {
        format!("{}?{}", path.as_str(), query)
    };
    let path_and_query = match warp::http::uri::PathAndQuery::from_maybe_shared(path_and_query_str)
    {
        Ok(p) => p,
        Err(e) => {
            warn!(
                "could not redirect to HTTPS: failed to build path and query: {}",
                e
            );
            return Box::new(warp::http::status::StatusCode::NOT_FOUND.into_response())
                as Box<dyn warp::Reply>;
        }
    };
    match warp::hyper::Uri::builder()
        .scheme("https")
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
    {
        Ok(uri) => Box::new(warp::redirect(uri)) as Box<dyn warp::Reply>,
        Err(e) => {
            error!("could not redirect to HTTPS: failed to build URI: {}", e);
            Box::new(warp::http::status::StatusCode::INTERNAL_SERVER_ERROR.into_response())
                as Box<dyn warp::Reply>
        }
    }
}

async fn https_redirect_fallback_response(
    rejection: warp::Rejection,
) -> Result<warp::http::Response<String>, warp::Rejection> {
    warn!("Redirecting to HTTPS failed: {:?}", rejection);
    Ok(warp::http::Response::builder()
        .status(warp::http::status::StatusCode::INTERNAL_SERVER_ERROR)
        .body(format!(
            "Please use HTTPS instead of HTTP, automatic redirection failed: {:?}",
            rejection
        ))
        .expect("failed to create response"))
}

impl HttpServer {
    #[allow(dead_code)]
    pub fn new_unencrypted(
        filter: GenericFilter,
        socket_addr: SocketAddr,
    ) -> Result<Self, Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel();
        trace!("starting HTTP server on {:?}", socket_addr);
        let (_addr, server) = warp::serve(filter)
            .try_bind_with_graceful_shutdown(socket_addr, async {
                let _ = shutdown_rx.await;
            })
            .map_err(|e| format!("failed to bind HTTP server to {}: {}", socket_addr, e))?;
        let join_handle = tokio::spawn(async move {
            server.await;
        });
        Ok(HttpServer {
            name: "Unencrypted HTTP".to_string(),
            socket_addr,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }

    /// Create a new server that redirects all requests to HTTPS
    pub fn new_https_redirect(socket_addr: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel();
        trace!("starting redirect-to-HTTPS server on {:?}", socket_addr);
        let (_addr, server) = warp::serve(
            warp::host::optional()
                .and(warp::path::full())
                .and(warp::query::raw())
                .map(redirect_request_to_https)
                // If there's no query string, `warp::query::raw()` fails so we need to try again
                .or(warp::host::optional()
                    .and(warp::path::full())
                    .map(|authority, path| redirect_request_to_https(authority, path, "".into())))
                .recover(https_redirect_fallback_response),
        )
        .try_bind_with_graceful_shutdown(socket_addr, async {
            let _ = shutdown_rx.await;
        })
        .map_err(|e| {
            format!(
                "failed to bind HTTP redirect server to {}: {}",
                socket_addr, e
            )
        })?;

        let join_handle = tokio::spawn(async move {
            server.await;
        });
        Ok(HttpServer {
            name: "HTTP-to-HTTPS redirect".to_string(),
            socket_addr,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }

    pub fn new_encrypted(
        filter: GenericFilter,
        socket_addr: SocketAddr,
        cert_path: &str,
        key_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel();
        trace!("starting HTTPS server on {:?}", socket_addr);

        let (_addr, server) = warp::serve(filter)
            .tls()
            .cert_path(cert_path)
            .key_path(key_path)
            .bind_with_graceful_shutdown(socket_addr, async {
                let _ = shutdown_rx.await;
            });
        // TODO: we want to use .try_bind_with_graceful_shutdown() (like we do in new_unencrypted())
        // so it doesn't panic if there's an error, but that's not implemented for TlsServer (see
        // https://github.com/seanmonstar/warp/pull/717). Once that PR lands and we upgrade to a
        // warp version that supports it we should use it.

        let join_handle = tokio::spawn(async move {
            server.await;
        });
        Ok(HttpServer {
            name: "Encrypted HTTPS".to_string(),
            socket_addr,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        })
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if let Err(()) = self.shutdown_tx.take().unwrap().send(()) {
            error!("failed to send {} server shutdown request", self.name);
        };
        match futures::executor::block_on(tokio::time::timeout(
            Duration::from_millis(200),
            self.join_handle.take().unwrap(),
        )) {
            Err(_) => warn!("shutting down {} server timed out", self.name),
            Ok(Err(e)) => error!("failed to join {} server task: {}", self.name, e),
            _ => trace!("{} server shut down", self.name),
        }
    }
}

impl Debug for HttpServer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} server on {}", self.name, self.socket_addr)
    }
}

impl ServerComponent for HttpServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;
    const CERT_PATH: &str = "src/server/tls_test_files/mock-cert.pem";
    const KEY_PATH: &str = "src/server/tls_test_files/mock-privkey.pem";

    fn mock_filter() -> GenericFilter {
        warp::any()
            .map(|| {
                Box::new(warp::http::status::StatusCode::OK.into_response()) as Box<dyn warp::Reply>
            })
            .boxed()
    }

    #[test]
    fn tcp_stream_connects_to_unencrypted() {
        run_with_tokio(move || {
            let socket = provision_socket();
            let _server = HttpServer::new_unencrypted(mock_filter(), *socket).unwrap();
            let _stream = TcpStream::connect(*socket).unwrap();
        });
    }

    #[test]
    fn tcp_stream_connects_to_encrypted() {
        run_with_tokio(move || {
            let socket = provision_socket();
            let _server =
                HttpServer::new_encrypted(mock_filter(), *socket, CERT_PATH, KEY_PATH).unwrap();
            let _stream = TcpStream::connect(*socket).unwrap();
        });
    }

    #[test]
    fn tcp_stream_connects_to_https_redirect() {
        run_with_tokio(move || {
            let socket = provision_socket();
            let _server = HttpServer::new_https_redirect(*socket).unwrap();
            let _stream = TcpStream::connect(*socket).unwrap();
        });
    }

    #[test]
    fn can_stop_unencrypted_while_tcp_stream_open() {
        run_with_tokio(move || {
            let socket = provision_socket();
            let mut _server = Some(HttpServer::new_unencrypted(mock_filter(), *socket).unwrap());
            let _stream = TcpStream::connect(*socket).unwrap();
            _server = None;
        });
    }
}
