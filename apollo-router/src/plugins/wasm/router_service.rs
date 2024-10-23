use crate::error::Error;
use crate::plugins::coprocessor::internalize_header_map;
use crate::plugins::wasm::Control;
use crate::plugins::wasm::WASM_VERSION;
use crate::services::external::externalize_header_map;
use crate::services::external::PipelineStep;
use crate::services::router;
use crate::services::router::body::get_body_bytes;
use crate::services::router::body::RouterBody;
use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::future::ready;
use futures::stream::once;
use futures::StreamExt;
use futures::TryStreamExt;
use serde_json::json;
use serde_json::to_string;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::BoxError;

use super::RouterExtensionPayload;
use super::WasmEngine;
use super::WASM_DESERIALIZATION_ERROR_EXTENSION;
use super::WASM_ERROR_EXTENSION;

pub(crate) async fn process_router_request_stage(
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
        "version": WASM_VERSION,
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

pub(crate) async fn process_router_response_stage(
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
        "version": WASM_VERSION,
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
                    "version": WASM_VERSION,
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
