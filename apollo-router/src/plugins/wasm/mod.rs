use crate::layers::async_checkpoint::OneShotAsyncCheckpointLayer;
use crate::plugin::Plugin;
use crate::plugin::PluginInit;
use crate::register_plugin;
use crate::services::router;
use arc_swap::ArcSwap;
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
use wasi_common::sync::WasiCtxBuilder;
use wasi_common::WasiCtx;
use wasmtime::*;

struct WasmEngine {
    engine: Engine,
    store: Store<WasiCtx>,
    instance: Instance,
    linker: Linker<WasiCtx>,
}

fn read_string(data: &[u8], ptr: usize) -> String {
    let mut end = ptr;
    while end < data.len() && data[end] != 0 {
        end += 1;
    }
    String::from_utf8_lossy(&data[ptr..end]).into_owned()
}

fn get_string_from_caller(caller: &mut Caller<'_, WasiCtx>, ptr: i32) -> String {
    let memory = caller
        .get_export("memory")
        .and_then(|ext| ext.into_memory())
        .expect("Memory not found");

    let data = memory.data(&caller);
    let message: String = read_string(data, ptr as usize);

    message
}

fn create_abort_function(store: &mut Store<WasiCtx>) -> Func {
    Func::wrap(
        store,
        |mut caller: Caller<'_, WasiCtx>, message: i32, fileName: i32, line: i32, column: i32| {
            println!("Abort function prt: {}", message);
            Ok(())
        },
    )
}

fn create_log_function(store: &mut Store<WasiCtx>) -> Func {
    Func::wrap(store, |mut caller: Caller<'_, WasiCtx>, ptr: i32| {
        let message = get_string_from_caller(&mut caller, ptr);
        println!("Log function prt: {}", message);
        Ok(())
    })
}
/*
fn create_abort_function(store: &mut Store<WasiCtx>) -> Func {
    Func::wrap(store, |caller: Caller<'_, WasiCtx>, ptr: i32| {
        println!("console.log: {}", ptr); // Print to Rust's stdout
    })
}
*/

impl WasmEngine {
    fn new(base_path: PathBuf, main: String) -> Result<Self> {
        let mut full_main_path = PathBuf::from(base_path);
        full_main_path.push(main);

        tracing::info!("Creating WASM engine at {:?}", full_main_path);

        let engine = Engine::default();

        let mut linker = Linker::new(&engine);
        wasi_common::sync::add_to_linker(&mut linker, |s| s)?;

        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args()?
            .build();
        let mut store = Store::new(&engine, wasi);
        WasmEngine::define_functions(&mut store, &mut linker)?;

        let module = Module::from_file(&engine, full_main_path)?;
        let instance = linker.instantiate(&mut store, &module)?;

        Ok(Self {
            engine,
            store,
            instance,
            linker,
        })
    }

    fn define_functions(store: &mut Store<WasiCtx>, linker: &mut Linker<WasiCtx>) -> Result<()> {
        let log_func = create_log_function(store);
        let abort_func = create_abort_function(store);

        {
            let store_ref = &mut *store; // Create a mutable reference to `store`
            linker.define(store_ref, "env", "console.log", log_func)?;
        }
        {
            let store_ref = &mut *store; // Create a mutable reference to `store`
            linker.define(store_ref, "env", "abort", abort_func)?;
        }

        Ok(())
    }

    fn create_string_pointer(&mut self, input_str: &str) -> Result<i32> {
        let memory = self.instance.get_memory(&mut self.store, "memory").unwrap();
        let alloc_fn = self
            .instance
            .get_func(&mut self.store, "__new")
            .unwrap()
            .typed::<(i32, i32), i32>(&self.store)?;

        let len = input_str.len() as i32;
        let ptr = alloc_fn.call(&mut self.store, (len, 1))?;

        memory.write(&mut self.store, ptr as usize, input_str.as_bytes())?;

        Ok(ptr)
    }

    fn get_string_pointer(&mut self, ptr: i32) -> Result<String> {
        let memory = self.instance.get_memory(&mut self.store, "memory").unwrap();
        let data = memory.data(&self.store);
        let message: String = read_string(data, ptr as usize);

        Ok(message)
    }

    fn call_function<Params, Results>(
        &mut self,
        function_name: &str,
        params: Params,
    ) -> Result<Results>
    where
        Params: WasmParams,
        Results: WasmResults,
    {
        let function_instance = self
            .instance
            .get_func(&mut self.store, function_name)
            .expect("`add` was not an exported function");
        let function_instance = function_instance.typed::<Params, Results>(&mut self.store)?;
        let result = function_instance.call(&mut self.store, params);
        result
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
    /// The directory where Rhai scripts can be found
    base_path: Option<PathBuf>,
    /// The main entry point for Rhai script evaluation
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

        let wasm_engine = Arc::new(ArcSwap::from_pointee(Mutex::new(WasmEngine::new(
            base_path, main,
        )?)));

        let park_flag = Arc::new(AtomicBool::new(false));
        let watching_flag = park_flag.clone();

        let watcher_handle = std::thread::spawn(move || {
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
                            if matches!(
                                event.kind,
                                EventKind::Modify(ModifyKind::Metadata(MetadataKind::WriteTime))
                                    | EventKind::Modify(ModifyKind::Data(DataChange::Any))
                                    | EventKind::Create(_)
                                    | EventKind::Remove(_)
                            ) {
                                tracing::info!("updating WASM execution");
                            }
                        }
                        Err(e) => tracing::error!("wasm watching event error: {:?}", e),
                    }
                },
                config,
            )
            .unwrap_or_else(|_| panic!("could not create watch on: {watched_path:?}"));
            watcher
                .watch(&watched_path, RecursiveMode::Recursive)
                .unwrap_or_else(|_| panic!("could not watch: {watched_path:?}"));
            // Park the thread until this Rhai instance is dropped (see Drop impl)
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
                        let payload = json!({
                            "version": "1",
                            "stage": "RouterRequest"
                        });
                        let payload_string = to_string(&payload).unwrap();
                        let ptr = wasm_engine_clone
                            .load()
                            .lock()
                            .unwrap()
                            .create_string_pointer(&payload_string)?;
                        let function_result_ptr = wasm_engine_clone
                            .load()
                            .lock()
                            .unwrap()
                            .call_function::<(i32,), i32>("RouterRequest", (ptr,))?;
                        let function_result = wasm_engine_clone
                            .load()
                            .lock()
                            .unwrap()
                            .get_string_pointer(function_result_ptr)?;
                        tracing::info!(
                            "WASM execution: Called RouterRequest and got {function_result}"
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
