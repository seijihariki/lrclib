use crate::{
    errors::ApiError,
    metrics::MonitoredRoute,
    mirror::{DUMPS_DIR, REPLICATION_DIR},
    routes::{
        flag_lyrics, get_lyrics_by_metadata, get_lyrics_by_track_id, publish_lyrics,
        request_challenge, search_lyrics,
    },
    AppState,
};
use axum::{
    body::Body,
    extract::{Request as AxumRequest, State},
    http::{header, Request},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tower_http::{
    classify::ServerErrorsFailureClass,
    cors::{Any, CorsLayer},
    services::ServeDir,
    trace::TraceLayer,
};
use tracing::Span;

/// Builds the application router with all routes, middleware, and CORS configuration.
pub(crate) fn build_router(state: Arc<AppState>, mirror_dir: Option<PathBuf>) -> Router {
    let state_for_request_metrics = Arc::clone(&state);
    let state_for_response_metrics = Arc::clone(&state);

    let read_routes = Router::new()
        .route(
            "/get",
            get(get_lyrics_by_metadata::route).layer(middleware::from_fn_with_state(
                Arc::clone(&state),
                record_get_metadata_metrics,
            )),
        )
        .route("/get/:track_id", get(get_lyrics_by_track_id::route))
        .route(
            "/search",
            get(search_lyrics::route).layer(middleware::from_fn_with_state(
                Arc::clone(&state),
                record_search_metrics,
            )),
        );
    let api_routes = if state.read_only {
        read_routes
            .route("/request-challenge", post(read_only_mirror))
            .route("/publish", post(read_only_mirror))
            .route("/flag", post(read_only_mirror))
    } else {
        read_routes
            .route("/request-challenge", post(request_challenge::route))
            .route("/publish", post(publish_lyrics::route))
            .route("/flag", post(flag_lyrics::route))
    };

    let mut router = Router::new().nest("/api", api_routes);
    if let Some(dir) = mirror_dir {
        router = router.nest("/mirror", mirror_routes(&dir));
    }

    router
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    let headers = request.headers();
                    let user_agent = headers
                        .get("Lrclib-Client")
                        .and_then(|value| value.to_str().ok())
                        .or_else(|| {
                            headers
                                .get("X-User-Agent")
                                .and_then(|value| value.to_str().ok())
                        })
                        .or_else(|| {
                            headers
                                .get(header::USER_AGENT)
                                .and_then(|value| value.to_str().ok())
                        })
                        .unwrap_or("");
                    let method = request.method().to_string();
                    let uri = request.uri().to_string();

                    tracing::debug_span!("request", method, uri, user_agent)
                })
                .on_failure(
                    |_failure: ServerErrorsFailureClass, _latency: Duration, _span: &Span| {},
                )
                .on_request(move |_request: &Request<Body>, _span: &Span| {
                    state_for_request_metrics.http_metrics.record_request();
                })
                .on_response(
                    move |response: &Response, latency: Duration, _span: &Span| {
                        state_for_response_metrics
                            .http_metrics
                            .record_response(response.status().is_server_error(), latency);
                    },
                ),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers([
                    header::CONTENT_TYPE,
                    "X-User-Agent".parse().unwrap(),
                    "Lrclib-Client".parse().unwrap(),
                ])
                .expose_headers([header::RETRY_AFTER]),
        )
}

/// Only the published subtrees, so other files in the publish directory (locks,
/// or a database placed alongside) are never exposed.
fn mirror_routes<S: Clone + Send + Sync + 'static>(dir: &Path) -> Router<S> {
    Router::new()
        .nest_service(&format!("/{DUMPS_DIR}"), ServeDir::new(dir.join(DUMPS_DIR)))
        .nest_service(
            &format!("/{REPLICATION_DIR}"),
            ServeDir::new(dir.join(REPLICATION_DIR)),
        )
        .layer(middleware::from_fn(mirror_file_policy))
}

/// State files change in place and must be revalidated; everything else is
/// written once.
async fn mirror_file_policy(request: AxumRequest, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let hidden = path.split('/').any(|segment| {
        segment.starts_with('.')
            || [".tmp", ".part", ".partial", ".lock"]
                .iter()
                .any(|suffix| segment.ends_with(suffix))
    });
    if hidden {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }

    let mut response = next.run(request).await;
    if response.status().is_success() {
        let cache_control = if path.ends_with("/state.json") || path.ends_with("/latest.json") {
            "no-cache"
        } else {
            "public, max-age=86400"
        };
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static(cache_control),
        );
    }
    response
}

async fn read_only_mirror() -> ApiError {
    ApiError::ReadOnlyMirrorError
}

async fn record_search_metrics(
    State(state): State<Arc<AppState>>,
    request: AxumRequest,
    next: Next,
) -> Response {
    record_endpoint_metrics(state, MonitoredRoute::Search, request, next).await
}

async fn record_get_metadata_metrics(
    State(state): State<Arc<AppState>>,
    request: AxumRequest,
    next: Next,
) -> Response {
    record_endpoint_metrics(state, MonitoredRoute::GetMetadata, request, next).await
}

async fn record_endpoint_metrics(
    state: Arc<AppState>,
    route: MonitoredRoute,
    request: AxumRequest,
    next: Next,
) -> Response {
    state.http_metrics.record_endpoint_request(route);
    let started_at = Instant::now();
    let response = next.run(request).await;
    state.http_metrics.record_endpoint_response(
        route,
        response.status().is_server_error(),
        started_at.elapsed(),
    );
    response
}
