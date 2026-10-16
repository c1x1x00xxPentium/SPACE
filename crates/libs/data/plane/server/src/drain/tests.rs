use super::*;
use crate::tests::test_data_plane_state;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceExt;

/// A running [`serve_and_drain`] plus the handles a test drives it with.
struct Drain {
    addr: SocketAddr,
    shutdown_token: CancellationToken,
    stream_shutdown_token: CancellationToken,
    tasks: TaskTracker,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn start_drain(
    app: impl FnOnce(CancellationToken) -> Router,
    shutdown_grace: Duration,
) -> Drain {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind drain listener");
    let addr = listener.local_addr().expect("drain listener addr");
    let shutdown_token = CancellationToken::new();
    let stream_shutdown_token = CancellationToken::new();
    let tasks = TaskTracker::new();
    let server = tokio::spawn(serve_and_drain(
        pplx_space_common_svc::http_listener::nodelay(listener),
        app(stream_shutdown_token.clone()),
        DrainLifecycle {
            shutdown_token: shutdown_token.clone(),
            stream_shutdown_token: stream_shutdown_token.clone(),
            tasks: tasks.clone(),
            attached_streams: Arc::new(AttachedStreamLimiter::new(4, 4, 4)),
            shutdown_grace,
        },
    ));
    Drain {
        addr,
        shutdown_token,
        stream_shutdown_token,
        tasks,
        server,
    }
}

/// A route whose response body yields whatever the test sends, until the
/// test drops the sender.
fn chunk_stream_app() -> (Router, mpsc::Sender<Bytes>) {
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    let rx = Arc::new(std::sync::Mutex::new(Some(rx)));
    let app = Router::new().route(
        "/stream",
        get(move || {
            let rx = rx.lock().expect("stream receiver").take();
            async move {
                let rx = rx.expect("the stream route is opened once");
                Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, Infallible>))
            }
        }),
    );
    (app, tx)
}

/// A route whose response body never yields and ends only when the stream
/// shutdown token is cancelled, the way the data plane's own streams do.
fn token_bound_stream_app(stream_shutdown_token: CancellationToken) -> Router {
    Router::new().route(
        "/stream",
        get(move || {
            let shutdown = stream_shutdown_token.child_token().cancelled_owned();
            async move {
                Body::from_stream(
                    futures_util::stream::pending::<Result<Bytes, Infallible>>()
                        .take_until(shutdown),
                )
            }
        }),
    )
}

async fn open_stream(addr: SocketAddr) -> Body {
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to drain server");
    let (mut sender, connection) = http1::handshake(TokioIo::new(tcp))
        .await
        .expect("HTTP/1 handshake");
    tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::get("/stream")
                .header(header::HOST, "drain.test")
                .body(Body::empty())
                .expect("stream request"),
        )
        .await
        .expect("stream response");
    assert_eq!(response.status(), StatusCode::OK);
    Body::new(response.into_body())
}

async fn next_chunk(body: &mut Body) -> Option<Bytes> {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
            .await
            .expect("stream frame within timeout")?
            .ok()?;
        if let Ok(data) = frame.into_data() {
            return Some(data);
        }
    }
}

#[tokio::test]
async fn drain_keeps_live_streams_and_exits_when_they_end() {
    let (app, tx) = chunk_stream_app();
    let drain = start_drain(|_| app, Duration::from_secs(60)).await;
    let mut body = open_stream(drain.addr).await;
    tx.send(Bytes::from_static(b"before")).await.unwrap();
    assert_eq!(next_chunk(&mut body).await.as_deref(), Some(&b"before"[..]));

    drain.shutdown_token.cancel();

    // The listener stops accepting new connections...
    let addr = drain.addr;
    tokio::time::timeout(Duration::from_secs(5), async {
        while tokio::net::TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("listener closes after SIGTERM");
    // ...while the attached stream keeps flowing, and its token stays live.
    tx.send(Bytes::from_static(b"during")).await.unwrap();
    assert_eq!(next_chunk(&mut body).await.as_deref(), Some(&b"during"[..]));
    assert!(!drain.stream_shutdown_token.is_cancelled());
    assert!(!drain.server.is_finished());

    // The last stream ending is what lets the drain finish.
    drop(tx);
    assert_eq!(next_chunk(&mut body).await, None);
    tokio::time::timeout(Duration::from_secs(5), drain.server)
        .await
        .expect("drain finishes once the stream ends")
        .unwrap()
        .unwrap();
    assert!(drain.stream_shutdown_token.is_cancelled());
}

#[tokio::test]
async fn drain_deadline_cancels_remaining_streams() {
    let shutdown_grace = Duration::from_millis(200);
    let drain = start_drain(token_bound_stream_app, shutdown_grace).await;
    let mut body = open_stream(drain.addr).await;

    let started = tokio::time::Instant::now();
    drain.shutdown_token.cancel();
    assert_eq!(next_chunk(&mut body).await, None);
    assert!(
        started.elapsed() >= shutdown_grace,
        "the stream must outlive SIGTERM until the drain deadline"
    );
    tokio::time::timeout(Duration::from_secs(5), drain.server)
        .await
        .expect("drain finishes after cancelling streams")
        .unwrap()
        .unwrap();
    assert!(drain.stream_shutdown_token.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn drain_waits_for_tracked_upgraded_connections() {
    let drain = start_drain(|_| Router::new(), Duration::from_secs(60)).await;
    let upgraded = drain.tasks.token();

    drain.shutdown_token.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !drain.server.is_finished(),
        "an upgraded connection is a live stream the drain must wait for"
    );

    drop(upgraded);
    tokio::time::timeout(Duration::from_secs(5), drain.server)
        .await
        .expect("drain finishes once the upgraded connection ends")
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn drain_exits_when_a_stream_ignores_cancellation() {
    let drain = start_drain(|_| Router::new(), Duration::from_secs(60)).await;
    let _stuck = drain.tasks.token();

    let started = tokio::time::Instant::now();
    drain.shutdown_token.cancel();
    drain.server.await.unwrap().unwrap();

    assert!(drain.stream_shutdown_token.is_cancelled());
    assert_eq!(
        started.elapsed(),
        Duration::from_secs(60) + STREAM_CANCEL_GRACE,
        "exit at the deadline plus the cancel grace, not when the stream ends"
    );
}

fn insecure_dev_test_process() -> bool {
    pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )])
}

#[tokio::test]
async fn readyz_fails_once_draining_starts() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    let drain_started = state.drain_started.clone();
    let app = router(state);
    let readyz = || Request::get("/readyz").body(Body::empty()).unwrap();

    let response = app.clone().oneshot(readyz()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    drain_started.cancel();
    let response = app.clone().oneshot(readyz()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "draining"
    );
    // Liveness is unaffected: the process is healthy, just leaving.
    let response = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

fn assert_draining_headers(response: &Response) {
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get(header::CONNECTION).unwrap(), "close");
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
}

#[tokio::test]
async fn native_event_stream_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));

    let response = router(state)
        .oneshot(
            Request::get(format!("/v1/sessions/{session_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Refused before route resolution: the stub cluster API would otherwise
    // fail the request with a different status.
    assert_draining_headers(&response);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "error": {
                "code": "service_unavailable",
                "message": "data plane is draining; retry",
                "retryable": true,
            }
        })
    );
}

#[tokio::test]
async fn ecw_stream_permit_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();

    let Err(error) = acquire_ecw_stream_permit(&state, ECW_PROCESS_STREAM_KIND) else {
        panic!("no new ECW stream while draining");
    };

    assert_eq!(state.attached_streams.active_total(), 0);
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    assert!(response.extensions().get::<DrainingRefusal>().is_some());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "code": 503, "message": "Data plane is draining; retry" })
    );
}

#[tokio::test]
async fn streams_attached_before_draining_are_unaffected_by_the_flag() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    let permit = acquire_ecw_stream_permit(&state, ECW_PROCESS_STREAM_KIND)
        .expect("stream attaches before the drain");

    state.drain_started.cancel();

    assert_eq!(state.attached_streams.active_total(), 1);
    drop(permit);
    assert_eq!(state.attached_streams.active_total(), 0);
}

#[tokio::test]
async fn drain_refusals_close_http1_connections_only() {
    let app = Router::new()
        .route(
            "/stream",
            get(|| async {
                let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
                mark_draining_response(&mut response);
                response
            }),
        )
        .route("/unary", get(|| async { "ok" }))
        .layer(middleware::from_fn(close_draining_http1_connections));
    let request = |path: &str, version| {
        Request::get(path)
            .version(version)
            .body(Body::empty())
            .unwrap()
    };

    let response = app
        .clone()
        .oneshot(request("/stream", axum::http::Version::HTTP_11))
        .await
        .unwrap();
    assert_draining_headers(&response);

    // HTTP/2 forbids connection-specific headers; GOAWAY does that job.
    let response = app
        .clone()
        .oneshot(request("/stream", axum::http::Version::HTTP_2))
        .await
        .unwrap();
    assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    assert!(response.headers().get(header::CONNECTION).is_none());

    // Responses that are not drain refusals are left alone.
    let response = app
        .oneshot(request("/unary", axum::http::Version::HTTP_11))
        .await
        .unwrap();
    assert!(response.headers().get(header::CONNECTION).is_none());
}

/// Read one server WebSocket frame header from `tcp` and return its opcode and
/// payload. Server frames are never masked, so the header is two bytes plus an
/// optional extended length.
async fn read_server_frame(tcp: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0_u8; 2];
    tcp.read_exact(&mut header).await.expect("frame header");
    let opcode = header[0] & 0x0f;
    let len = match header[1] & 0x7f {
        126 => {
            let mut extended = [0_u8; 2];
            tcp.read_exact(&mut extended)
                .await
                .expect("extended length");
            usize::from(u16::from_be_bytes(extended))
        }
        len => usize::from(len),
    };
    let mut payload = vec![0_u8; len];
    tcp.read_exact(&mut payload).await.expect("frame payload");
    (opcode, payload)
}

#[tokio::test]
async fn unattached_websocket_is_told_to_reconnect_when_the_drain_starts() {
    if !insecure_dev_test_process() {
        return;
    }
    let shutdown_token = CancellationToken::new();
    let mut state = test_data_plane_state().await;
    state.drain_started = shutdown_token.child_token();
    let stream_shutdown_token = state.stream_shutdown_token.clone();
    let tasks = state.upgrade_tasks.clone();
    let tracked = state.upgrade_tasks.clone();
    let attached_streams = Arc::clone(&state.attached_streams);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_and_drain(
        pplx_space_common_svc::http_listener::nodelay(listener),
        router(state),
        DrainLifecycle {
            shutdown_token: shutdown_token.clone(),
            stream_shutdown_token,
            tasks,
            attached_streams,
            shutdown_grace: Duration::from_secs(60),
        },
    ));

    // Upgrade without a header token, so the server waits for the first
    // frame: the socket is accepted but not attached.
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    // Any 16 bytes, base64-encoded, make a valid handshake key.
    let key = BASE64_STANDARD.encode(b"drain-test-nonce");
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        format!(
            "GET /v1/sessions/{session_id}/attach HTTP/1.1\r\n\
             Host: drain.test\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: {key}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        let mut byte = [0_u8; 1];
        tcp.read_exact(&mut byte).await.expect("upgrade response");
        response.push(byte[0]);
    }
    assert!(
        response.starts_with(b"HTTP/1.1 101"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    // The upgraded socket counts as a task once the HTTP connection is gone,
    // so the drain waits for it.
    assert_eq!(tracked.len(), 1, "the attach holds a tracker token");

    shutdown_token.cancel();

    let (opcode, payload) =
        tokio::time::timeout(Duration::from_secs(5), read_server_frame(&mut tcp))
            .await
            .expect("close frame without waiting for the attach deadline");
    assert_eq!(opcode, 0x8, "close frame");
    assert_eq!(
        payload.get(..2),
        Some(&axum::extract::ws::close_code::RESTART.to_be_bytes()[..]),
        "1012 service restart"
    );
    // Nothing is attached, so the drain finishes long before its 60s budget.
    drop(tcp);
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("drain finishes once the socket is refused")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn native_proxy_upgrade_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Port);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));

    let response = router(state)
        .oneshot(
            Request::get(format!("/v1/sessions/{session_id}/proxy"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONNECTION, "upgrade")
                .header(header::UPGRADE, "websocket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Refused before route resolution, which the stub cluster API would fail.
    assert_draining_headers(&response);
}

#[tokio::test]
async fn ecw_service_upgrade_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let mut state = test_data_plane_state().await;
    state.ecw_hosts = EcwHostConfig::from_env_value(Some(r#"[{"host":"example.test"}]"#));
    state.drain_started.cancel();
    let sandbox = SandboxId::new();

    let response = router(state)
        .oneshot(
            Request::get("/socket")
                .header(header::HOST, format!("8080-{sandbox}.example.test"))
                .header(header::CONNECTION, "upgrade")
                .header(header::UPGRADE, "websocket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_draining_headers(&response);
}

#[tokio::test]
async fn streamed_process_input_is_refused_while_draining() {
    if !insecure_dev_test_process() {
        return;
    }
    let state = test_data_plane_state().await;
    state.drain_started.cancel();

    let response = router(state)
        .oneshot(
            Request::post("/process.Process/StreamInput")
                .header(header::CONTENT_TYPE, "application/connect+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Refused before route resolution, which these headers would fail.
    assert_draining_headers(&response);
}

#[tokio::test]
async fn before_stream_deadline_runs_the_open_unless_the_deadline_wins() {
    let token = CancellationToken::new();
    assert_eq!(before_stream_deadline(&token, async { 7 }).await, Some(7));

    token.cancel();
    assert_eq!(
        before_stream_deadline(&token, std::future::pending::<()>()).await,
        None
    );
}
