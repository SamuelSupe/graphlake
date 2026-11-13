use std::{
    ffi::{CStr, CString, c_char, c_int},
    ptr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use arrow_array::{RecordBatch, RecordBatchReader, ffi_stream::FFI_ArrowArrayStream};
use futures::TryStreamExt;
use graphlake_engine::{
    Catalog, Engine, GraphOptions, Language, QueryOptions, QueryResult,
    arrow::{datatypes::SchemaRef, error::ArrowError},
};
use tokio::runtime::{Builder, Runtime};

pub struct GraphLakeEngine {
    engine: Engine,
    runtime: Arc<Runtime>,
}

struct Reader {
    result: Option<QueryResult>,
    schema: SchemaRef,
    runtime: Arc<Runtime>,
}

impl Iterator for Reader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.result.as_mut()?;
        match self.runtime.block_on(result.batches.try_next()) {
            Ok(Some(batch)) => Some(Ok(batch)),
            completion => {
                let _runtime = self.runtime.enter();
                self.result.take();
                completion
                    .err()
                    .map(|error| Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl RecordBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let _runtime = self.runtime.enter();
        self.result.take();
    }
}

fn error_message(error: anyhow::Error) -> *mut c_char {
    CString::new(format!("{error:#}").replace('\0', "\\0"))
        .expect("NUL replaced")
        .into_raw()
}

unsafe fn input<'a>(value: *const c_char, name: &str) -> Result<&'a str> {
    ensure!(!value.is_null(), "{name} pointer is null");
    // The caller supplies an initialized, NUL-terminated C string.
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .with_context(|| format!("{name} must be UTF-8"))
}

/// # Safety
/// The caller supplies a readable NUL-terminated path and a writable error slot or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graphlake_engine_open(
    catalog: *const c_char,
    error: *mut *mut c_char,
) -> *mut GraphLakeEngine {
    if !error.is_null() {
        unsafe { error.write(ptr::null_mut()) };
    }
    let result = (|| {
        let catalog = unsafe { input(catalog, "catalog path") }?;
        let engine = Engine::new(Catalog::load(catalog)?)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        Ok(GraphLakeEngine {
            engine,
            runtime: Arc::new(runtime),
        })
    })();
    match result {
        Ok(engine) => Box::into_raw(Box::new(engine)),
        Err(failure) => {
            if !error.is_null() {
                unsafe { error.write(error_message(failure)) };
            }
            ptr::null_mut()
        }
    }
}

/// # Safety
/// The caller supplies a live engine, a readable NUL-terminated statement, a writable
/// empty Arrow stream slot and an optional writable error slot. Do not run on a Tokio thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graphlake_engine_query(
    engine: *const GraphLakeEngine,
    language: c_int,
    statement: *const c_char,
    timeout_ms: u64,
    memory_bytes: usize,
    output: *mut FFI_ArrowArrayStream,
    error: *mut *mut c_char,
) -> c_int {
    unsafe {
        graphlake_engine_query_with_graph(
            engine,
            language,
            statement,
            timeout_ms,
            memory_bytes,
            ptr::null(),
            output,
            error,
        )
    }
}

/// # Safety
/// The pointers obey the contract of graphlake_engine_query. Graph options are null or
/// a readable, NUL-terminated JSON string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graphlake_engine_query_with_graph(
    engine: *const GraphLakeEngine,
    language: c_int,
    statement: *const c_char,
    timeout_ms: u64,
    memory_bytes: usize,
    graph_options: *const c_char,
    output: *mut FFI_ArrowArrayStream,
    error: *mut *mut c_char,
) -> c_int {
    unsafe {
        graphlake_engine_query_with_parameters(
            engine,
            language,
            statement,
            timeout_ms,
            memory_bytes,
            graph_options,
            ptr::null(),
            output,
            error,
        )
    }
}

/// # Safety
/// The pointers obey the contract of graphlake_engine_query_with_graph. Parameters
/// are null or a readable, NUL-terminated JSON object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graphlake_engine_query_with_parameters(
    engine: *const GraphLakeEngine,
    language: c_int,
    statement: *const c_char,
    timeout_ms: u64,
    memory_bytes: usize,
    graph_options: *const c_char,
    parameters: *const c_char,
    output: *mut FFI_ArrowArrayStream,
    error: *mut *mut c_char,
) -> c_int {
    if !error.is_null() {
        unsafe { error.write(ptr::null_mut()) };
    }
    let result = (|| {
        let engine = unsafe { engine.as_ref() }.context("engine pointer is null")?;
        ensure!(!output.is_null(), "Arrow stream pointer is null");
        let statement = unsafe { input(statement, "statement") }?;
        let language = match language {
            0 => Language::Cypher,
            1 => Language::Sql,
            2 => Language::Gremlin,
            _ => bail!("unsupported query language {language}"),
        };
        let mut options = QueryOptions::default();
        if !graph_options.is_null() {
            options.graph =
                GraphOptions::from_json(unsafe { input(graph_options, "graph options") }?)?;
        }
        if !parameters.is_null() {
            options =
                options.with_parameters_json(unsafe { input(parameters, "Cypher parameters") }?)?;
        }
        if timeout_ms != 0 {
            options.timeout = Duration::from_millis(timeout_ms);
        }
        if memory_bytes != 0 {
            options.memory_bytes = memory_bytes;
        }
        let result = engine
            .runtime
            .block_on(engine.engine.query(language, statement, options))?;
        let reader = Reader {
            schema: result.batches.schema(),
            result: Some(result),
            runtime: Arc::clone(&engine.runtime),
        };
        Ok(FFI_ArrowArrayStream::new(Box::new(reader)))
    })();
    match result {
        Ok(stream) => {
            unsafe { output.write(stream) };
            0
        }
        Err(failure) => {
            if !error.is_null() {
                unsafe { error.write(error_message(failure)) };
            }
            1
        }
    }
}

/// # Safety
/// The pointer is null or a live handle from graphlake_engine_open, released at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graphlake_engine_close(engine: *mut GraphLakeEngine) {
    if !engine.is_null() {
        drop(unsafe { Box::from_raw(engine) });
    }
}

/// # Safety
/// The pointer is null or a GraphLake error string, released at most once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graphlake_error_free(error: *mut c_char) {
    if !error.is_null() {
        drop(unsafe { CString::from_raw(error) });
    }
}
