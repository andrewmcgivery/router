use crate::layers::async_checkpoint::OneShotAsyncCheckpointLayer;
use crate::plugin::Plugin;
use crate::plugin::PluginInit;
use crate::register_plugin;
use crate::services::router;
use arc_swap::ArcSwap;
use extism::*;
use notify::event::DataChange;
use notify::event::MetadataKind;
use notify::event::ModifyKind;
use notify::Config;
use notify::EventKind;
use notify::PollWatcher;
use notify::RecursiveMode;
use notify::Watcher;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use serde_json::to_string;
use std::error::Error;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};
use tower::util::MapFutureLayer;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;

struct WasmEngine {
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
                                    match WasmEngine::new(
                                        watching_path.clone(),
                                        watched_main.clone(),
                                    ) {
                                        Ok(eb) => {
                                            tracing::info!("updating WASM execution engine");
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
        })
    }

    fn router_service(&self, service: router::BoxService) -> router::BoxService {
        let request_layer = {
            let wasm_engine_clone = self.wasm_engine.clone();
            Some(OneShotAsyncCheckpointLayer::new(
                move |request: router::Request| {
                    let wasm_engine_clone = wasm_engine_clone.clone(); // Clone inside the async block
                    async move {
                        tracing::info!("WASM execution: Hit router_request");

                        let wasm_instance = wasm_engine_clone.load();
                        let mut locked_instance = wasm_instance.lock().unwrap();
                        let plugin = &mut locked_instance.plugin;

                        let payload = json!({
                            "version": "1",
                            "stage": "RouterRequest",
                            "control": "Continue"
                        });
                        let payload_string = to_string(&payload).unwrap();

                        let function_result =
                            plugin.call::<&str, &str>("RouterRequest", &payload_string)?;

                        tracing::info!(
                            "WASM execution: Called RouterRequest and got back: {}",
                            function_result
                        );

                        let result = Ok(ControlFlow::Continue(request));
                        result
                    }
                },
            ))
        };

        let response_layer = Some(MapFutureLayer::new(
            move |fut: std::pin::Pin<
                Box<
                    dyn Future<Output = Result<router::Response, Box<dyn Error + Send + Sync>>>
                        + Send,
                >,
            >| async {
                let response: router::Response = fut.await?;
                tracing::info!("WASM execution: Hit router_response");
                let result: Result<router::Response, BoxError> = Ok(response);
                result
            },
        ));

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
