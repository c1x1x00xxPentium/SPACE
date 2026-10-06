use super::*;
use crate::ECW_ENVD_PORT;
use axum::Router;
use axum::extract::Extension;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use http_body::Body as _;
use http_body_util::BodyExt;
use pplx_space_metrics::test_capture::{
    CapturedCounter, CapturedValue, capture_counters_guard, capture_values_guard,
};
use tower::ServiceExt;

const ECW_HOST: &str = "ecw.test";

fn ecw_hosts() -> EcwHostConfig {
    EcwHostConfig::from_env_value(Some(&format!(r#"[{{"host":"{ECW_HOST}"}}]"#)))
}

fn sandbox_host(port: u32) -> String {
    format!("{port}-{}.{ECW_HOST}", SandboxId::new())
}

fn headers_with_host(host: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(host) = host {
        headers.insert(axum::http::header::HOST, host.parse().unwrap());
    }
    headers
}

pub(crate) fn byte_counts(counters: &[CapturedCounter]) -> Vec<(Vec<String>, u64)> {
    let mut counts: Vec<(Vec<String>, u64)> = Vec::new();
    for counter in counters
        .iter()
        .filter(|counter| counter.name == "data_plane.http.bytes")
    {
        match counts.iter_mut().find(|(tags, _)| *tags == counter.tags) {
            Some((_, total)) => *total += counter.value,
            None => counts.push((counter.tags.clone(), counter.value)),
        }
    }
    counts
}

pub(crate) fn bytes_tags(direction: &str, route_class: &str) -> Vec<String> {
    vec![
        format!("direction:{direction}"),
        format!("route_class:{route_class}"),
    ]
}

fn too_many_requests(counters: &[CapturedCounter]) -> Vec<Vec<String>> {
    counters
        .iter()
        .filter(|counter| counter.name == "data_plane.http.too_many_requests")
        .map(|counter| counter.tags.clone())
        .collect()
}

pub(crate) fn upload_gauges(values: &[CapturedValue]) -> Vec<(String, f64)> {
    values
        .iter()
        .filter(|value| value.name.starts_with("data_plane.upload."))
        .map(|value| (value.name.clone(), value.value))
        .collect()
}

fn observed(router: Router) -> Router {
    router.layer(middleware::from_fn_with_state(
        Arc::new(ecw_hosts()),
        observe_http_traffic,
    ))
}

#[test]
fn routes_classify_by_surface_with_service_ports_decided_by_host() {
    let hosts = ecw_hosts();
    let envd = sandbox_host(ECW_ENVD_PORT);
    let service = sandbox_host(8080);
    for (host, path, expected) in [
        (Some(envd.as_str()), "/files", HttpRouteClass::Files),
        (None, "/files", HttpRouteClass::Files),
        (
            Some(envd.as_str()),
            "/filesystem.Filesystem/ListDir",
            HttpRouteClass::Filesystem,
        ),
        (
            Some(envd.as_str()),
            "/process.Process/Start",
            HttpRouteClass::Process,
        ),
        (
            None,
            "/v1/sessions/abc/events",
            HttpRouteClass::NativeSession,
        ),
        (None, "/healthz", HttpRouteClass::Health),
        (None, "/readyz", HttpRouteClass::Health),
        (Some(envd.as_str()), "/health", HttpRouteClass::Health),
        (Some(envd.as_str()), "/files/compose", HttpRouteClass::Other),
        (Some(envd.as_str()), "/metrics", HttpRouteClass::Other),
        // A service port forwards every path, including ones envd also serves.
        (
            Some(service.as_str()),
            "/files",
            HttpRouteClass::ServicePort,
        ),
        (Some(service.as_str()), "/", HttpRouteClass::ServicePort),
    ] {
        assert_eq!(
            classify_route(&hosts, &headers_with_host(host), path),
            expected,
            "{host:?} {path}",
        );
    }
}

#[tokio::test]
async fn request_and_response_bytes_are_counted_by_route_class() {
    let router = observed(Router::new().route(
        "/files",
        post(
            |Extension(request_bytes): Extension<RequestBodyBytes>, body: Body| async move {
                let received = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                assert_eq!(request_bytes.get(), received.len() as u64);
                "x".repeat(7)
            },
        ),
    ));
    let capture = capture_counters_guard();
    let response = router
        .oneshot(
            Request::post("/files")
                .body(Body::from(vec![0_u8; 1234]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .len(),
        7
    );

    let counts = byte_counts(&capture.finish());
    assert!(
        counts.contains(&(bytes_tags("in", "files"), 1234)),
        "{counts:?}"
    );
    assert!(
        counts.contains(&(bytes_tags("out", "files"), 7)),
        "{counts:?}"
    );
}

#[tokio::test]
async fn request_body_handle_is_only_offered_to_files_requests() {
    let router = observed(Router::new().route(
        "/process.Process/List",
        post(
            |request_bytes: Option<Extension<RequestBodyBytes>>| async move {
                assert!(request_bytes.is_none());
            },
        ),
    ));
    let response = router
        .oneshot(
            Request::post("/process.Process/List")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn wrapping_keeps_an_exact_response_size_hint() {
    let router = observed(Router::new().route("/healthz", get(|| async { "ok" })));
    let response = router
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    // Losing the exact hint would make hyper send chunked encoding instead of
    // Content-Length.
    assert_eq!(response.body().size_hint().exact(), Some(2));
}

#[test]
fn large_bodies_flush_while_still_moving() {
    let capture = capture_counters_guard();
    let mut tally = ByteTally::new(HttpBodyDirection::In, HttpRouteClass::Files, None);
    tally.add(BYTE_FLUSH_THRESHOLD as usize);
    let flushed = byte_counts(&capture.finish());
    assert_eq!(
        flushed,
        vec![(bytes_tags("in", "files"), BYTE_FLUSH_THRESHOLD)],
        "a full threshold must report before the body ends",
    );

    let capture = capture_counters_guard();
    tally.add(5);
    assert!(byte_counts(&capture.finish()).is_empty());
    let capture = capture_counters_guard();
    drop(tally);
    assert_eq!(
        byte_counts(&capture.finish()),
        vec![(bytes_tags("in", "files"), 5)],
        "dropping the body must flush the remainder",
    );
}

#[tokio::test]
async fn every_429_is_counted_and_unnamed_ones_are_unclassified() {
    let router = observed(
        Router::new()
            .route(
                "/process.Process/Start",
                post(|| async {
                    let mut response = StatusCode::TOO_MANY_REQUESTS.into_response();
                    response
                        .extensions_mut()
                        .insert(TooManyRequestsReason::SandboxBusy);
                    response
                }),
            )
            .route("/files", post(|| async { StatusCode::TOO_MANY_REQUESTS }))
            .route("/healthz", get(|| async { "ok" })),
    );
    let capture = capture_counters_guard();
    for (method, path) in [
        ("POST", "/process.Process/Start"),
        ("POST", "/files"),
        ("GET", "/healthz"),
    ] {
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        too_many_requests(&capture.finish()),
        vec![
            vec![
                "route_class:process".to_string(),
                "reason:sandbox_busy".to_string()
            ],
            vec![
                "route_class:files".to_string(),
                "reason:unclassified".to_string()
            ],
        ],
    );
}

#[test]
fn in_flight_uploads_report_count_and_reserved_bytes() {
    let tracker = Arc::new(UploadTracker::default());
    let capture = capture_values_guard();
    let first = tracker.begin(100, SandboxId::new(), None, None);
    let second = tracker.begin(
        40,
        SandboxId::new(),
        Some(OrgId::new()),
        Some(RequestBodyBytes::default()),
    );
    assert_eq!(tracker.snapshot(), (2, 140));

    second.finish(StatusCode::OK);
    assert_eq!(tracker.snapshot(), (1, 100));
    // Dropping without `finish` is a cancelled upload; it still releases.
    drop(first);
    assert_eq!(tracker.snapshot(), (0, 0));

    assert_eq!(
        upload_gauges(&capture.finish()),
        [(1.0, 100.0), (2.0, 140.0), (1.0, 100.0), (0.0, 0.0),]
            .into_iter()
            .flat_map(|(count, bytes)| [
                ("data_plane.upload.in_flight".to_string(), count),
                ("data_plane.upload.in_flight_bytes".to_string(), bytes),
            ])
            .collect::<Vec<_>>(),
    );
}
