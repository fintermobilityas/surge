use std::path::Path;
use std::sync::Arc;

use crate::logline;
use surge_core::config::manifest::SurgeManifest;
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::lock::mutex::DistributedMutex;

/// Acquire a distributed lock.
pub async fn acquire(manifest_path: &Path, name: &str, timeout: u32) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let lock_config = manifest
        .lock
        .as_ref()
        .ok_or_else(|| SurgeError::Config("No lock server configured in manifest".to_string()))?;

    if lock_config.url.is_empty() {
        return Err(SurgeError::Config("Lock server URL is empty in manifest".to_string()));
    }

    let ctx = Arc::new(Context::new());
    ctx.set_lock_server(&lock_config.url);

    let mut mutex = DistributedMutex::new(ctx, name);
    let acquired = mutex.try_acquire(timeout as i32).await?;
    if !acquired {
        return Err(SurgeError::Lock(format!("Lock '{name}' is held by another process")));
    }

    let challenge = mutex.challenge().unwrap_or("");
    logline::emit_raw(challenge);
    logline::success(&format!("Lock '{name}' acquired"));

    Ok(())
}

/// Release a distributed lock.
pub async fn release(manifest_path: &Path, name: &str, challenge: &str) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let lock_config = manifest
        .lock
        .as_ref()
        .ok_or_else(|| SurgeError::Config("No lock server configured in manifest".to_string()))?;

    if lock_config.url.is_empty() {
        return Err(SurgeError::Config("Lock server URL is empty in manifest".to_string()));
    }

    let ctx = Arc::new(Context::new());
    ctx.set_lock_server(&lock_config.url);

    let mut mutex = DistributedMutex::new(ctx, name);
    mutex.set_challenge(challenge.to_string());
    mutex.try_release().await?;

    logline::success(&format!("Lock '{name}' released"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use std::time::Duration;

    use surge_core::platform::detect::current_rid;

    fn write_manifest(path: &Path, store_dir: &Path, lock_url: &str) {
        let yaml = format!(
            "schema: 1\nstorage:\n  provider: filesystem\n  bucket: {}\nlock:\n  url: {lock_url}\napps:\n  - id: demo\n    target:\n      rid: {}\n",
            store_dir.display(),
            current_rid()
        );
        std::fs::write(path, yaml).expect("manifest should be written");
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        let mut headers_end = None;
        let mut content_length = 0_usize;

        while headers_end.is_none() {
            let read = stream.read(&mut chunk).expect("request bytes");
            assert!(read > 0, "request should contain headers");
            buffer.extend_from_slice(&chunk[..read]);

            if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                headers_end = Some(index + 4);
                let headers = String::from_utf8_lossy(&buffer[..index + 4]);
                for line in headers.lines() {
                    if let Some(value) = line.strip_prefix("Content-Length:") {
                        content_length = value.trim().parse().expect("valid content length");
                    }
                }
            }
        }

        let headers_end = headers_end.expect("headers end");
        while buffer.len() < headers_end + content_length {
            let read = stream.read(&mut chunk).expect("request body");
            assert!(read > 0, "request should contain declared body bytes");
            buffer.extend_from_slice(&chunk[..read]);
        }

        String::from_utf8(buffer).expect("utf-8 request")
    }

    fn spawn_lock_server() -> (String, Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        listener.set_nonblocking(false).expect("listener should be blocking");
        let address = format!("http://{}", listener.local_addr().expect("listener addr"));
        let (request_tx, request_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            for response_body in ["challenge-123", ""] {
                let (mut stream, _) = listener.accept().expect("request");
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("read timeout");
                let request = read_http_request(&mut stream);
                request_tx.send(request).expect("request capture");

                let status_line = "HTTP/1.1 200 OK";
                let response = format!(
                    "{status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).expect("response bytes");
                stream.flush().expect("response flush");
            }
        });

        (address, request_rx, handle)
    }

    #[tokio::test]
    async fn acquire_and_release_use_manifest_lock_server() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let manifest_path = temp_dir.path().join("surge.yml");
        let store_dir = temp_dir.path().join("store");
        std::fs::create_dir_all(&store_dir).expect("store dir");

        let (lock_url, request_rx, server_handle) = spawn_lock_server();
        write_manifest(&manifest_path, &store_dir, &lock_url);

        acquire(&manifest_path, "demo-lock", 30)
            .await
            .expect("lock acquire should succeed");
        release(&manifest_path, "demo-lock", "challenge-123")
            .await
            .expect("lock release should succeed");

        let acquire_request = request_rx.recv().expect("acquire request");
        let release_request = request_rx.recv().expect("release request");
        server_handle.join().expect("server thread");

        assert!(acquire_request.starts_with("POST /lock "));
        assert!(acquire_request.contains("\"name\":\"demo-lock\""));
        assert!(acquire_request.contains("\"duration\":\"00:00:30\""));

        assert!(release_request.starts_with("DELETE /unlock "));
        assert!(release_request.contains("\"name\":\"demo-lock\""));
        assert!(release_request.contains("\"challenge\":\"challenge-123\""));
    }

    #[tokio::test]
    async fn acquire_requires_lock_configuration() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let manifest_path = temp_dir.path().join("surge.yml");
        let store_dir = temp_dir.path().join("store");
        std::fs::create_dir_all(&store_dir).expect("store dir");

        let yaml = format!(
            "schema: 1\nstorage:\n  provider: filesystem\n  bucket: {}\napps:\n  - id: demo\n    target:\n      rid: {}\n",
            store_dir.display(),
            current_rid()
        );
        std::fs::write(&manifest_path, yaml).expect("manifest");

        let err = acquire(&manifest_path, "demo-lock", 30)
            .await
            .expect_err("lock acquire should fail without configuration");
        assert!(err.to_string().contains("No lock server configured"));
    }
}
