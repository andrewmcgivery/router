use crate::error::Error;
use crate::graphql;
use crate::layers::async_checkpoint::OneShotAsyncCheckpointLayer;
use crate::plugin::Plugin;
use crate::plugin::PluginInit;
use crate::plugins::coprocessor::internalize_header_map;
use crate::register_plugin;
use crate::services;
use crate::services::external::externalize_header_map;
use crate::services::external::PipelineStep;
use crate::services::router;
use crate::services::router::body::get_body_bytes;
use crate::services::router::body::RouterBody;
use crate::services::supergraph;
use crate::Context;
use arc_swap::ArcSwap;
use bytes::Bytes;
use extism::*;
use futures::future;
use futures::future::ready;
use futures::stream;
use futures::stream::once;
use futures::StreamExt;
use futures::TryStreamExt;
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
use serde_json::Value;
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

use super::coprocessor::handle_graphql_response;

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
struct RouterExtensionPayload<T> {
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
            Some(MapFutureLayer::new(
                move |fut: std::pin::Pin<
                    Box<
                        dyn Future<
                                Output = Result<
                                    router::Response,
                                    Box<dyn std::error::Error + Send + Sync>,
                                >,
                            > + Send,
                    >,
                >| {
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

                        let result =
                            process_router_response_stage(response, wasm_engine_clone, sdl)
                                .await
                                .map_err(|error| {
                                    tracing::error!(
                                "external extensibility: router response stage error: {error}"
                            );
                                    error
                                });

                        result
                    }
                },
            ))
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
            Some(MapFutureLayer::new(
                move |fut: std::pin::Pin<
                    Box<
                        dyn Future<
                                Output = Result<
                                    supergraph::Response,
                                    Box<dyn std::error::Error + Send + Sync>,
                                >,
                            > + Send,
                    >,
                >| {
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
                },
            ))
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

async fn process_router_request_stage(
    mut request: router::Request,
    wasm_engine: Arc<ArcSwap<Mutex<WasmEngine>>>,
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

    let wasm_instance = wasm_engine.load();
    let mut locked_instance = wasm_instance.lock().await;
    let plugin = &mut locked_instance.plugin;
    let function_result = plugin.call::<&str, &str>("RouterRequest", &payload_string)?;

    let returned_payload: RouterExtensionPayload<String> = serde_json::from_str(function_result)?;
    let control = returned_payload
        .control
        .expect("We should have validation similar to coprocessor");

    if matches!(control, Control::Break(_)) {
        let code = control.get_http_status()?;

        // At this point our body is a String. Try to get a valid JSON value from it
        let body_as_value = returned_payload
            .body
            .as_ref()
            .and_then(|b| serde_json::from_str(b).ok())
            .unwrap_or(serde_json::Value::Null);

        // Now we have some JSON, let's see if it's the right "shape" to create a graphql_response.
        // If it isn't, we create a graphql error response
        let graphql_response: crate::graphql::Response = match body_as_value {
            serde_json::Value::Null => crate::graphql::Response::builder()
                .errors(vec![Error::builder()
                    .message(
                        returned_payload
                            .body
                            .expect("We should have validation similar to coprocessor"),
                    )
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

        if let Some(headers) = returned_payload.headers {
            *res.response.headers_mut() = internalize_header_map(headers)?;
        }

        if let Some(context) = returned_payload.context {
            for (key, value) in context.try_into_iter()? {
                res.context.upsert_json_value(key, move |_current| value);
            }
        }

        return Ok(ControlFlow::Break(res));
    }

    let new_body = match returned_payload.body {
        Some(bytes) => RouterBody::from(bytes),
        None => RouterBody::from(bytes),
    };

    request.router_request = http::Request::from_parts(parts, new_body.into_inner());

    if let Some(context) = returned_payload.context {
        for (key, value) in context.try_into_iter()? {
            request
                .context
                .upsert_json_value(key, move |_current| value);
        }
    }

    if let Some(headers) = returned_payload.headers {
        *request.router_request.headers_mut() = internalize_header_map(headers)?;
    }

    return Ok(ControlFlow::Continue(request));
}

async fn process_router_response_stage(
    mut response: router::Response,
    wasm_engine: Arc<ArcSwap<Mutex<WasmEngine>>>,
    sdl: Arc<String>,
) -> Result<router::Response, BoxError> {
    let (parts, body) = response.response.into_parts();
    let (first, rest): (
        Option<Result<Bytes, hyper::Error>>,
        crate::services::router::Body,
    ) = body.into_future().await;

    let opt_first: Option<Bytes> = first.and_then(|f| f.ok());
    let bytes = match opt_first {
        Some(b) => b,
        None => {
            tracing::error!("Wasm cannot convert body into future due to problem with first part");
            return Err(BoxError::from(
                "Wasm cannot convert body into future due to problem with first part",
            ));
        }
    };
    let body_to_send = String::from_utf8(bytes.to_vec())?;
    let headers_to_send = externalize_header_map(&parts.headers)?;
    let context_to_send = response.context.clone();
    let sdl_to_send = sdl.clone().to_string();
    let status_to_send = parts.status.as_u16();

    let payload = json!({
        "version": EXTERNALIZABLE_VERSION,
        "stage": PipelineStep::RouterResponse,
        "control": Control::default(),
        "id": response.context.id.clone(),
        "headers": headers_to_send,
        "body": body_to_send,
        "context": context_to_send,
        "sdl": sdl_to_send,
        "status": status_to_send,
    });
    let payload_string = to_string(&payload).unwrap();

    let wasm_instance = wasm_engine.load();
    let mut locked_instance = wasm_instance.lock().await;
    let plugin = &mut locked_instance.plugin;

    let function_result = plugin.call::<&str, &str>("RouterResponse", &payload_string)?;
    let returned_payload: RouterExtensionPayload<String> = serde_json::from_str(function_result)?;

    let new_body = match returned_payload.body {
        Some(bytes) => RouterBody::from(bytes),
        None => RouterBody::from(bytes),
    };

    response.response = http::Response::from_parts(parts, new_body.into_inner());

    if let Some(control) = returned_payload.control {
        *response.response.status_mut() = control.get_http_status()?
    }

    if let Some(context) = returned_payload.context {
        for (key, value) in context.try_into_iter()? {
            response
                .context
                .upsert_json_value(key, move |_current| value);
        }
    }

    if let Some(headers) = returned_payload.headers {
        *response.response.headers_mut() = internalize_header_map(headers)?;
    }

    // Now break our WASM modified response back into parts
    let (parts, body) = response.response.into_parts();

    // Clone all the bits we need
    let context = response.context.clone();
    let map_context = response.context.clone();

    // Map the rest of our body to process subsequent chunks of response
    let mapped_stream = rest
        .map_err(BoxError::from)
        .and_then(move |deferred_response| {
            //let generator_client = http_client.clone();
            let generator_wasm_engine = wasm_engine.clone();
            let generator_map_context = map_context.clone();
            let generator_sdl_to_send = sdl_to_send.clone();
            let generator_id = map_context.id.clone();

            async move {
                let bytes = deferred_response.to_vec();
                let body_to_send = String::from_utf8(bytes.to_vec())?;
                let context_to_send = generator_map_context.clone();

                // Note: We deliberately DO NOT send headers or status_code even if the user has
                // requested them. That's because they are meaningless on a deferred response and
                // providing them will be a source of confusion.
                let payload = json!({
                    "version": EXTERNALIZABLE_VERSION,
                    "stage": PipelineStep::RouterResponse,
                    "control": Control::default(),
                    "id": generator_id,
                    "body": body_to_send,
                    "context": context_to_send,
                    "sdl": generator_sdl_to_send,
                });
                let payload_string = to_string(&payload).unwrap();

                let wasm_instance = generator_wasm_engine.load();
                let mut locked_instance = wasm_instance.lock().await;
                let plugin = &mut locked_instance.plugin;
                let function_result =
                    plugin.call::<&str, &str>("RouterResponse", &payload_string)?;

                let returned_payload: RouterExtensionPayload<String> =
                    serde_json::from_str(function_result)?;

                // Third, process our reply and act on the contents. Our processing logic is
                // that we replace "bits" of our incoming response with the updated bits if they
                // are present in our returned_payload. If they aren't present, just use the
                // bits that we sent to the WASM plugin.
                let final_bytes: Bytes = match returned_payload.body {
                    Some(bytes) => bytes.into(),
                    None => bytes.into(),
                };

                if let Some(context) = returned_payload.context {
                    for (key, value) in context.try_into_iter()? {
                        generator_map_context.upsert_json_value(key, move |_current| value);
                    }
                }

                // We return the final_bytes into our stream of response chunks
                Ok(final_bytes)
            }
        });

    // Create our response stream which consists of the bytes from our first body chained with the
    // rest of the responses in our mapped stream.
    let bytes = get_body_bytes(body).await.map_err(BoxError::from);
    let final_stream = once(ready(bytes)).chain(mapped_stream).boxed();

    // Finally, return a response which has a Body that wraps our stream of response chunks.
    Ok(router::Response {
        context,
        response: http::Response::from_parts(
            parts,
            RouterBody::wrap_stream(final_stream).into_inner(),
        ),
    })
}

async fn process_supergraph_request_stage(
    mut request: supergraph::Request,
    wasm_engine: Arc<ArcSwap<Mutex<WasmEngine>>>,
    sdl: Arc<String>,
) -> Result<ControlFlow<supergraph::Response, supergraph::Request>, BoxError> {
    let (parts, body) = request.supergraph_request.into_parts();
    let bytes = Bytes::from(serde_json::to_vec(&body)?);
    let body_to_send = serde_json::from_slice::<serde_json::Value>(&bytes)?;
    let headers_to_send = externalize_header_map(&parts.headers)?;
    let context_to_send = request.context.clone();
    let sdl_to_send = sdl.clone().to_string();

    let payload = json!({
        "version": EXTERNALIZABLE_VERSION,
        "stage": PipelineStep::RouterRequest,
        "control": Control::default(),
        "id": request.context.id.clone(),
        "headers": headers_to_send,
        "body": body_to_send,
        "context": context_to_send,
        "sdl": sdl_to_send,
        "method": parts.method.to_string()
    });
    let payload_string = to_string(&payload).unwrap();

    let wasm_instance = wasm_engine.load();
    let mut locked_instance = wasm_instance.lock().await;
    let plugin = &mut locked_instance.plugin;
    let function_result = plugin.call::<&str, &str>("SupergraphRequest", &payload_string)?;

    let returned_payload: RouterExtensionPayload<Value> = serde_json::from_str(function_result)?;
    let control = returned_payload
        .control
        .expect("We should have validation similar to coprocessor");

    if matches!(control, Control::Break(_)) {
        let code = control.get_http_status()?;

        let res = {
            let graphql_response: crate::graphql::Response =
                serde_json::from_value(returned_payload.body.unwrap_or(serde_json::Value::Null))
                    .unwrap_or_else(|error| {
                        crate::graphql::Response::builder()
                            .errors(vec![Error::builder()
                                .message(format!(
                                    "couldn't deserialize coprocessor output body: {error}"
                                ))
                                .extension_code("EXTERNAL_DESERIALIZATION_ERROR")
                                .build()])
                            .build()
                    });

            let mut http_response = http::Response::builder()
                .status(code)
                .body(stream::once(future::ready(graphql_response)).boxed())?;
            if let Some(headers) = returned_payload.headers {
                *http_response.headers_mut() = internalize_header_map(headers)?;
            }

            let supergraph_response = supergraph::Response {
                response: http_response,
                context: request.context,
            };

            if let Some(context) = returned_payload.context {
                for (key, value) in context.try_into_iter()? {
                    supergraph_response
                        .context
                        .upsert_json_value(key, move |_current| value);
                }
            }

            supergraph_response
        };
        return Ok(ControlFlow::Break(res));
    }

    let new_body: crate::graphql::Request = match returned_payload.body {
        Some(value) => serde_json::from_value(value)?,
        None => body,
    };

    request.supergraph_request = http::Request::from_parts(parts, new_body);

    if let Some(context) = returned_payload.context {
        for (key, value) in context.try_into_iter()? {
            request
                .context
                .upsert_json_value(key, move |_current| value);
        }
    }

    if let Some(headers) = returned_payload.headers {
        *request.supergraph_request.headers_mut() = internalize_header_map(headers)?;
    }

    if let Some(uri) = returned_payload.uri {
        *request.supergraph_request.uri_mut() = uri.parse()?;
    }

    return Ok(ControlFlow::Continue(request));
}

async fn process_supergraph_response_stage(
    mut response: supergraph::Response,
    wasm_engine: Arc<ArcSwap<Mutex<WasmEngine>>>,
    sdl: Arc<String>,
) -> Result<supergraph::Response, BoxError> {
    let (mut parts, body) = response.response.into_parts();
    let (first, rest): (Option<graphql::Response>, graphql::ResponseStream) =
        body.into_future().await;

    let first = first.ok_or_else(|| {
        BoxError::from("Coprocessor cannot convert body into future due to problem with first part")
    })?;

    let body_to_send = serde_json::to_value(&first).expect("serialization will not fail");
    let headers_to_send = externalize_header_map(&parts.headers)?;
    let context_to_send = response.context.clone();
    let sdl_to_send = sdl.clone().to_string();
    let status_to_send = parts.status.as_u16();

    let payload = json!({
        "version": EXTERNALIZABLE_VERSION,
        "stage": PipelineStep::RouterResponse,
        "control": Control::default(),
        "id": response.context.id.clone(),
        "headers": headers_to_send,
        "body": body_to_send,
        "context": context_to_send,
        "sdl": sdl_to_send,
        "status": status_to_send,
        "has_next": first.has_next
    });
    let payload_string = to_string(&payload).unwrap();

    let wasm_instance = wasm_engine.load();
    let mut locked_instance = wasm_instance.lock().await;
    let plugin = &mut locked_instance.plugin;

    let function_result = plugin.call::<&str, &str>("SupergraphResponse", &payload_string)?;
    let returned_payload: RouterExtensionPayload<Value> = serde_json::from_str(function_result)?;

    let new_body: graphql::Response = handle_graphql_response(first, returned_payload.body)?;

    if let Some(control) = returned_payload.control {
        parts.status = control.get_http_status()?
    }

    if let Some(context) = returned_payload.context {
        for (key, value) in context.try_into_iter()? {
            response
                .context
                .upsert_json_value(key, move |_current| value);
        }
    }

    if let Some(headers) = returned_payload.headers {
        parts.headers = internalize_header_map(headers)?;
    }

    // Clone all the bits we need
    let context = response.context.clone();
    let map_context = response.context.clone();

    // Map the rest of our body to process subsequent chunks of response
    let mapped_stream = rest
        .then(move |deferred_response| {
            //let generator_client = http_client.clone();
            let generator_wasm_engine = wasm_engine.clone();
            let generator_map_context = map_context.clone();
            let generator_sdl_to_send = sdl_to_send.clone();
            let generator_id = map_context.id.clone();

            async move {
                let body_to_send =
                    serde_json::to_value(&deferred_response).expect("serialization will not fail");
                let context_to_send = generator_map_context.clone();

                // Note: We deliberately DO NOT send headers or status_code even if the user has
                // requested them. That's because they are meaningless on a deferred response and
                // providing them will be a source of confusion.
                let payload = json!({
                    "version": EXTERNALIZABLE_VERSION,
                    "stage": PipelineStep::RouterResponse,
                    "control": Control::default(),
                    "id": generator_id,
                    "body": body_to_send,
                    "context": context_to_send,
                    "sdl": generator_sdl_to_send,
                    "has_next": deferred_response.has_next
                });
                let payload_string = to_string(&payload).unwrap();

                let wasm_instance = generator_wasm_engine.load();
                let mut locked_instance = wasm_instance.lock().await;
                let plugin = &mut locked_instance.plugin;
                let function_result =
                    plugin.call::<&str, &str>("SupergraphResponse", &payload_string)?;

                let returned_payload: RouterExtensionPayload<Value> =
                    serde_json::from_str(function_result)?;

                // Third, process our reply and act on the contents. Our processing logic is
                // that we replace "bits" of our incoming response with the updated bits if they
                // are present in our returned_payload. If they aren't present, just use the
                // bits that we sent to the WASM plugin.
                let new_deferred_response: graphql::Response =
                    handle_graphql_response(deferred_response, returned_payload.body)?;

                if let Some(context) = returned_payload.context {
                    for (key, value) in context.try_into_iter()? {
                        generator_map_context.upsert_json_value(key, move |_current| value);
                    }
                }

                // We return the deferred_response into our stream of response chunks
                Ok(new_deferred_response)
            }
        })
        .map(|res: Result<graphql::Response, BoxError>| match res {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("WASM error handling deferred supergraph response: {e}");
                graphql::Response::builder()
                    .error(
                        Error::builder()
                            .message("Internal error handling deferred response")
                            .extension_code("INTERNAL_ERROR")
                            .build(),
                    )
                    .build()
            }
        });

    // Create our response stream which consists of our first body chained with the
    // rest of the responses in our mapped stream.
    let stream = once(ready(new_body)).chain(mapped_stream).boxed();

    // Finally, return a response which has a Body that wraps our stream of response chunks.
    Ok(supergraph::Response {
        context,
        response: http::Response::from_parts(parts, stream),
    })
}

register_plugin!("apollo", "wasm", Wasm);
