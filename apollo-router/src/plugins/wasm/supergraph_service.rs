use crate::error::Error;
use crate::graphql;
use crate::plugins::coprocessor::handle_graphql_response;
use crate::plugins::coprocessor::internalize_header_map;
use crate::plugins::wasm::Control;
use crate::plugins::wasm::WASM_VERSION;
use crate::services::external::externalize_header_map;
use crate::services::external::PipelineStep;
use crate::services::supergraph;
use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::future;
use futures::future::ready;
use futures::stream;
use futures::stream::once;
use futures::StreamExt;
use serde_json::json;
use serde_json::to_string;
use serde_json::Value;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::BoxError;

use super::RouterExtensionPayload;
use super::WasmEngine;

pub(crate) async fn process_supergraph_request_stage(
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
        "version": WASM_VERSION,
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

pub(crate) async fn process_supergraph_response_stage(
    response: supergraph::Response,
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
        "version": WASM_VERSION,
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
                    "version": WASM_VERSION,
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
