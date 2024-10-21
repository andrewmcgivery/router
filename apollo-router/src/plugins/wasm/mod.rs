use crate::error::Error;
use crate::layers::async_checkpoint::OneShotAsyncCheckpointLayer;
use crate::plugin::Plugin;
use crate::plugin::PluginInit;
use crate::plugins::coprocessor::internalize_header_map;
use crate::register_plugin;
use crate::services::external::externalize_header_map;
use crate::services::external::PipelineStep;
use crate::services::router;
use crate::services::router::body::get_body_bytes;
use crate::services::router::body::RouterBody;
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
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::to_string;
use std::collections::HashMap;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::Mutex;
use tower::util::MapFutureLayer;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;

const EXTERNALIZABLE_VERSION: u8 = 1;
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
struct RouterExtensionPayload {
    version: u8,
    stage: String,
    control: Control,
    id: String,
    body: String,
    headers: HashMap<String, Vec<String>>,
    context: Context,
    sdl: String,
    path: String,
    method: String,
}

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
                        let wasm_instance = wasm_engine_clone.load();
                        let mut locked_instance = wasm_instance.lock().await;
                        let plugin = &mut locked_instance.plugin;

                        if !plugin.function_exists("RouterRequest") {
                            return Ok(ControlFlow::Continue(request));
                        }

                        let result = process_router_request_stage(request, plugin, sdl)
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

        let response_layer = Some(MapFutureLayer::new(
            move |fut: std::pin::Pin<
                Box<
                    dyn Future<
                            Output = Result<
                                router::Response,
                                Box<dyn std::error::Error + Send + Sync>,
                            >,
                        > + Send,
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

async fn process_router_request_stage(
    mut request: router::Request,
    plugin: &mut extism::Plugin,
    sdl: Arc<String>,
) -> Result<ControlFlow<router::Response, router::Request>, BoxError> {
    let (parts, body) = request.router_request.into_parts();
    let bytes = get_body_bytes(body).await?;
    let body_to_send = String::from_utf8(bytes.to_vec())?;
    let headers_to_send = externalize_header_map(&parts.headers)?;
    let context_to_send = request.context.clone();
    let sdl_to_send = sdl.clone().to_string();
    let path_to_send = parts.uri.to_string();

    let payload = json!({
        "version": EXTERNALIZABLE_VERSION,
        "stage": PipelineStep::RouterRequest,
        "control": Control::default(),
        "id": request.context.id.clone(),
        "headers": headers_to_send,
        "body": body_to_send,
        "context": context_to_send,
        "sdl": sdl_to_send,
        "path": path_to_send,
        "method": parts.method.to_string()
    });
    let payload_string = to_string(&payload).unwrap();

    let function_result = plugin.call::<&str, &str>("RouterRequest", &payload_string)?;

    tracing::info!(
        "WASM execution: Called RouterRequest and got back: {}",
        function_result
    );

    let returned_payload: RouterExtensionPayload = serde_json::from_str(function_result)?;
    let control = returned_payload.control;

    if matches!(control, Control::Break(_)) {
        let code = control.get_http_status()?;

        // At this point our body is a String. Try to get a valid JSON value from it
        let body_as_value = serde_json::from_str(&returned_payload.body)
            .ok()
            .unwrap_or(serde_json::Value::Null);

        // Now we have some JSON, let's see if it's the right "shape" to create a graphql_response.
        // If it isn't, we create a graphql error response
        let graphql_response: crate::graphql::Response = match body_as_value {
            serde_json::Value::Null => crate::graphql::Response::builder()
                .errors(vec![Error::builder()
                    .message(returned_payload.body)
                    .extension_code(WASM_ERROR_EXTENSION)
                    .build()])
                .build(),
            _ => serde_json::from_value(body_as_value).unwrap_or_else(|error| {
                crate::graphql::Response::builder()
                    .errors(vec![Error::builder()
                        .message(format!("couldn't deserialize WASM output body: {error}"))
                        .extension_code(WASM_DESERIALIZATION_ERROR_EXTENSION)
                        .build()])
                    .build()
            }),
        };

        let res = router::Response::builder()
            .errors(graphql_response.errors)
            .extensions(graphql_response.extensions)
            .status_code(code)
            .context(request.context);

        let mut res = match (graphql_response.label, graphql_response.data) {
            (Some(label), Some(data)) => res.label(label).data(data).build()?,
            (Some(label), None) => res.label(label).build()?,
            (None, Some(data)) => res.data(data).build()?,
            (None, None) => res.build()?,
        };

        *res.response.headers_mut() = internalize_header_map(returned_payload.headers)?;

        for (key, value) in returned_payload.context.try_into_iter()? {
            res.context.upsert_json_value(key, move |_current| value);
        }

        return Ok(ControlFlow::Break(res));
    }

    let new_body = RouterBody::from(returned_payload.body);
    request.router_request = http::Request::from_parts(parts, new_body.into_inner());
    *request.router_request.headers_mut() = internalize_header_map(returned_payload.headers)?;

    for (key, value) in returned_payload.context.try_into_iter()? {
        request
            .context
            .upsert_json_value(key, move |_current| value);
    }

    return Ok(ControlFlow::Continue(request));
}

register_plugin!("apollo", "wasm", Wasm);
