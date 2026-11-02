use crate::{
    background::{spawn_lyrics_counter, spawn_metrics_reporter, spawn_optimize_pragma},
    db::init_db,
    mirror::{self, MirrorConfig},
    router::build_router,
    search_index,
    state::build_app_state,
};
use std::{path::PathBuf, sync::Arc};
use tokio::signal;
use tracing_subscriber::EnvFilter;

/// Start the LRCLIB server on the specified port with the given database.
pub async fn serve(port: u16, database: &PathBuf) {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::from_env("LRCLIB_LOG"))
        .init();

    let mirror_config = MirrorConfig::from_env()
        .unwrap_or_else(|error| exit_with("Invalid mirror configuration", error));
    if let Some(follower) = &mirror_config.follower {
        match mirror::bootstrap_database_if_missing(follower, database).await {
            Ok(true) => {
                // An existing index was built from the replaced database.
                let index_dir = search_index::index_dir_from_env(database);
                if let Err(error) = search_index::invalidate(&index_dir) {
                    exit_with("Cannot reset the search index", error);
                }
            }
            Ok(false) => {}
            Err(error) => exit_with("Cannot bootstrap database from the upstream mirror", error),
        }
    }

    let pool = init_db(database).expect("Cannot initialize connection to SQLite database!");
    if mirror_config.follower.is_some() {
        let ready = pool
            .get()
            .map_err(anyhow::Error::from)
            .and_then(|conn| mirror::ensure_follower_database(&conn));
        if let Err(error) = ready {
            exit_with("Cannot follow the upstream mirror", error);
        }
    }
    let publisher = mirror::prepare_publisher(&pool, mirror_config.publisher.as_ref(), database)
        .unwrap_or_else(|error| exit_with("Cannot prepare mirror publishing", error));

    let (search_index, search_indexer) = search_index::bootstrap_from_env(database, &pool);
    let state = build_app_state(pool, search_index, mirror_config.follower.is_some());

    spawn_metrics_reporter(Arc::clone(&state));
    spawn_lyrics_counter(Arc::clone(&state));
    spawn_optimize_pragma(Arc::clone(&state));
    if let Some(follower) = mirror_config.follower {
        if let Err(error) = mirror::spawn_follower(state.pool.clone(), follower, database) {
            exit_with("Cannot start mirror follower", error);
        }
    }
    let mirror_dir = publisher
        .as_ref()
        .filter(|publisher| publisher.config().serve)
        .map(|publisher| publisher.config().dir.clone());
    if let Some(publisher) = publisher {
        mirror::spawn_publisher(state.pool.clone(), publisher);
    }

    let app = build_router(state, mirror_dir);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();

    println!(
        "LRCLIB server v{} is listening on {}!",
        env!("CARGO_PKG_VERSION"),
        listener.local_addr().unwrap()
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    if let Some(indexer) = search_indexer {
        indexer.shutdown();
    }
}

/// Reports the full error chain instead of a panic backtrace.
fn exit_with(context: &str, error: anyhow::Error) -> ! {
    tracing::error!(error = format!("{error:#}"), "{context}");
    eprintln!("{context}: {error:#}");
    std::process::exit(1);
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
