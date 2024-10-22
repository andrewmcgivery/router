mod router_service;
mod supergraph_service;

use crate::layers::async_checkpoint::OneShotAsyncCheckpointLayer;
use crate::plugin::Plugin;
use crate::plugin::PluginInit;
use crate::register_plugin;
use crate::services;
use crate::services::router;
use crate::services::supergraph;
use crate::Context;
use arc_swap::ArcSwap;
use extism::*;
use http::StatusCode;
use notify::event::DataChange;
use notify::event::MetadataKind;
use notify::event::ModifyKind;
use notify::Config;
use notify::EventKind;
use notify::PollWatcher;
use notify::RecursiveMode;
use notify::Watcher;
use router_service::process_router_request_stage;
use router_service::process_router_response_stage;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};
use supergraph_service::process_supergraph_request_stage;
use supergraph_service::process_supergraph_response_stage;
use tokio::sync::Mutex;
use tower::util::MapFutureLayer;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;

pub(crate) const WASM_VERSION: u8 = 1;
const WASM_ERROR_EXTENSION: &str = "ERROR";
const WASM_DESERIALIZATION_ERROR_EXTENSION: &str = "WASM_DESERIALIZATION_ERROR";

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Control {
    #[default]
    Continue,
    Break(u16),
}

impl Control {
    pub(crate) fn get_http_status(&self) -> Result<StatusCode, BoxError> {
        match self {
            Control::Continue => Ok(StatusCode::OK),
            Control::Break(code) => StatusCode::from_u16(*code).map_err(|e| e.into()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RouterExtensionPayload<T> {
    version: u8,
    stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    control: Option<Control>,
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<Context>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sdl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    has_next: Option<bool>,
}

pub(crate) struct WasmEngine {
    plugin: extism::Plugin,
}

impl WasmEngine {
    fn new(base_path: PathBuf, main: String) -> Result<Self, extism::Error> {
        let mut full_main_path = PathBuf::from(base_path);
        full_main_path.push(main);

        tracing::info!("Creating WASM engine at {:?}", full_main_path);

        let wasm_file = extism::Wasm::File {
            path: full_main_path,
            meta: WasmMetadata::default(),
        };
        let manifest = Manifest::new([wasm_file]);
        let plugin = extism::Plugin::new(&manifest, [], true)?;

        tracing::info!("WASM Engine created!",);

        Ok(Self { plugin })
    }
}

/// Plugin which implements Wasm functionality
/// Note: We use ArcSwap here in preference to a shared RwLock. Updates to
/// the engine block will be infrequent in relation to the accesses of it.
/// We'd love to use AtomicArc if such a thing existed, but since it doesn't
/// we'll use ArcSwap to accomplish our goal.
struct Wasm {
    wasm_engine: Arc<ArcSwap<Mutex<WasmEngine>>>,
    park_flag: Arc<AtomicBool>,
    watcher_handle: Option<std::thread::JoinHandle<()>>,
    sdl: Arc<String>,
}

/// Configuration for the Wasm Plugin
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Conf {
    /// The directory where Wasm extensions can be found
    base_path: Option<PathBuf>,
    /// The main entry point for Wasm extensions evaluation
    main: Option<String>,
}

#[async_trait::async_trait]
impl Plugin for Wasm {
    type Config = Conf;

    async fn new(init: PluginInit<Self::Config>) -> Result<Self, BoxError> {
        let base_path = match init.config.base_path {
            Some(path) => path,
            None => "wasm".into(),
        };

        let main = match init.config.main {
            Some(main) => main,
            None => "main.wasm".to_string(),
        };

        let watched_path = base_path.clone();
        let watched_main = main.clone();

        let wasm_engine = Arc::new(ArcSwap::from_pointee(Mutex::new(WasmEngine::new(
            base_path, main,
        )?)));
        let watched_engine = wasm_engine.clone();

        let park_flag = Arc::new(AtomicBool::new(false));
        let watching_flag = park_flag.clone();

        let watcher_handle = std::thread::spawn(move || {
            let watching_path = watched_path.clone();
            let config = Config::default()
                .with_poll_interval(Duration::from_secs(3))
                .with_compare_contents(true);
            let mut watcher = PollWatcher::new(
                move |res: Result<notify::Event, notify::Error>| {
                    match res {
                        Ok(event) => {
                            // Let's limit the events we are interested in to:
                            //  - Modified files
                            //  - Created/Remove files
                            //  - with suffix "wasm"
                            if matches!(
                                event.kind,
                                EventKind::Modify(ModifyKind::Metadata(MetadataKind::WriteTime))
                                    | EventKind::Modify(ModifyKind::Data(DataChange::Any))
                                    | EventKind::Create(_)
                                    | EventKind::Remove(_)
                            ) {
                                let mut proceed = false;
                                for path in event.paths {
                                    if path.extension().map_or(false, |ext| ext == "wasm") {
                                        proceed = true;
                                        break;
                                    }
                                }

                                if proceed {
                                    tracing::info!("Update detected in WASM files. Updating WASM execution engine...");
                                    match WasmEngine::new(
                                        watching_path.clone(),
                                        watched_main.clone(),
                                    ) {
                                        Ok(eb) => {
                                            tracing::info!("WASM execution engine updated.");
                                            watched_engine.store(Arc::new(Mutex::new(eb)))
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "could not create new WASM execution engine: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => tracing::error!("WASM watching event error: {:?}", e),
                    }
                },
                config,
            )
            .unwrap_or_else(|_| panic!("could not create watch on: {watched_path:?}"));
            watcher
                .watch(&watched_path, RecursiveMode::Recursive)
                .unwrap_or_else(|_| panic!("could not watch: {watched_path:?}"));
            // Park the thread until this WASM instance is dropped (see Drop impl)
            // We may actually unpark() before this code executes or exit from park() spuriously.
            // Use the watching_flag to control a loop which waits from the flag to be updated
            // from Drop.
            while !watching_flag.load(Ordering::Acquire) {
                std::thread::park();
            }
        });

        Ok(Self {
            park_flag,
            watcher_handle: Some(watcher_handle),
            wasm_engine,
            sdl: init.supergraph_sdl,
        })
    }

    fn router_service(&self, service: router::BoxService) -> router::BoxService {
        let request_layer = {
            let wasm_engine_clone = self.wasm_engine.clone();
            let sdl = self.sdl.clone();
            Some(OneShotAsyncCheckpointLayer::new(
                move |request: router::Request| {
                    let wasm_engine_clone = wasm_engine_clone.clone();
                    let sdl = sdl.clone();
                    async move {
                        {
                            let wasm_instance = wasm_engine_clone.load();
                            let mut locked_instance = wasm_instance.lock().await;
                            let plugin = &mut locked_instance.plugin;

                            if !plugin.function_exists("RouterRequest") {
                                return Ok(ControlFlow::Continue(request));
                            }
                        }

                        let result = process_router_request_stage(request, wasm_engine_clone, sdl)
                            .await
                            .map_err(|error| {
                                tracing::error!(
                                    "WASM Extension: router request stage error: {error}"
                                );
                                error
                            });
                        result
                    }
                },
            ))
        };

        let response_layer = {
            let wasm_engine_clone = self.wasm_engine.clone();
            let sdl = self.sdl.clone();
            Some(MapFutureLayer::new(move |fut| {
                let wasm_engine_clone = wasm_engine_clone.clone();
                let sdl = sdl.clone();
                async move {
                    let response: router::Response = fut.await?;

                    {
                        let wasm_instance = wasm_engine_clone.load();
                        let mut locked_instance = wasm_instance.lock().await;
                        let plugin = &mut locked_instance.plugin;

                        if !plugin.function_exists("RouterResponse") {
                            return Ok(response);
                        }
                    }

                    let result = process_router_response_stage(response, wasm_engine_clone, sdl)
                        .await
                        .map_err(|error| {
                            tracing::error!(
                                "external extensibility: router response stage error: {error}"
                            );
                            error
                        });

                    result
                }
            }))
        };

        ServiceBuilder::new()
            .option_layer(request_layer)
            .option_layer(response_layer)
            .service(service)
            .boxed()
    }

    fn supergraph_service(
        &self,
        service: services::supergraph::BoxService,
    ) -> services::supergraph::BoxService {
        let request_layer = {
            let wasm_engine_clone = self.wasm_engine.clone();
            let sdl = self.sdl.clone();
            Some(OneShotAsyncCheckpointLayer::new(
                move |request: supergraph::Request| {
                    let wasm_engine_clone = wasm_engine_clone.clone();
                    let sdl = sdl.clone();
                    async move {
                        {
                            let wasm_instance = wasm_engine_clone.load();
                            let mut locked_instance = wasm_instance.lock().await;
                            let plugin = &mut locked_instance.plugin;

                            if !plugin.function_exists("SupergraphRequest") {
                                return Ok(ControlFlow::Continue(request));
                            }
                        }

                        let result =
                            process_supergraph_request_stage(request, wasm_engine_clone, sdl)
                                .await
                                .map_err(|error| {
                                    tracing::error!(
                                        "WASM Extension: Supergraph request stage error: {error}"
                                    );
                                    error
                                });
                        result
                    }
                },
            ))
        };

        let response_layer = {
            let wasm_engine_clone = self.wasm_engine.clone();
            let sdl = self.sdl.clone();
            Some(MapFutureLayer::new(move |fut| {
                let wasm_engine_clone = wasm_engine_clone.clone();
                let sdl = sdl.clone();
                async move {
                    let response: supergraph::Response = fut.await?;

                    {
                        let wasm_instance = wasm_engine_clone.load();
                        let mut locked_instance = wasm_instance.lock().await;
                        let plugin = &mut locked_instance.plugin;

                        if !plugin.function_exists("SupergraphResponse") {
                            return Ok(response);
                        }
                    }

                    let result =
                        process_supergraph_response_stage(response, wasm_engine_clone, sdl)
                            .await
                            .map_err(|error| {
                                tracing::error!(
                                "external extensibility: Supergraph response stage error: {error}"
                            );
                                error
                            });

                    result
                }
            }))
        };

        ServiceBuilder::new()
            .option_layer(request_layer)
            .option_layer(response_layer)
            .service(service)
            .boxed()
    }
}

impl Drop for Wasm {
    fn drop(&mut self) {
        if let Some(wh) = self.watcher_handle.take() {
            self.park_flag.store(true, Ordering::Release);
            wh.thread().unpark();
            wh.join().expect("wasm file watcher thread terminating");
        }
    }
}

register_plugin!("apollo", "wasm", Wasm);
