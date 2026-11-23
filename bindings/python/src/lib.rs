use std::{
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use arrow_pyarrow::ToPyArrow;
use futures::TryStreamExt;
use graphlake_engine::{
    CancellationToken, Catalog, Direction, Engine, GraphAlgorithm, GraphOptions, GraphProjection,
    Language, ProjectionAlgorithm, QueryOptions, QueryResult, Route, ScalarValue, SourceVersion,
    Traversal, TraversalKind, TraversalStrategy, VertexKey, arrow::datatypes::SchemaRef,
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};
use tokio::runtime::{Builder, Runtime};

#[pyclass(name = "Engine", frozen)]
struct PyEngine {
    engine: Engine,
    runtime: Arc<Runtime>,
    options: QueryOptions,
}

#[pymethods]
impl PyEngine {
    #[new]
    #[pyo3(signature = (catalog, *, timeout_ms=60_000, memory_mib=256, cache_dir=None, cache_scope=None, logical_partition=None, snapshot_time=None, require_consistent_snapshot=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        catalog: PathBuf,
        timeout_ms: u64,
        memory_mib: usize,
        cache_dir: Option<PathBuf>,
        cache_scope: Option<String>,
        logical_partition: Option<&str>,
        snapshot_time: Option<String>,
        require_consistent_snapshot: bool,
    ) -> PyResult<Self> {
        if timeout_ms == 0 || memory_mib == 0 {
            return Err(PyValueError::new_err(
                "timeout_ms and memory_mib must be positive",
            ));
        }
        let memory_bytes = memory_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| PyValueError::new_err("memory_mib is too large"))?;
        let mut engine =
            Engine::new(Catalog::load(catalog).map_err(runtime_error)?).map_err(runtime_error)?;
        match (cache_dir, cache_scope) {
            (Some(dir), Some(scope)) => {
                engine = engine.with_cache(dir, &scope).map_err(runtime_error)?
            }
            (None, None) => {}
            _ => {
                return Err(PyValueError::new_err(
                    "cache_dir and cache_scope must be provided together",
                ));
            }
        }
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(8 * 1024 * 1024)
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        Ok(Self {
            engine,
            runtime: Arc::new(runtime),
            options: QueryOptions {
                timeout: Duration::from_millis(timeout_ms),
                memory_bytes,
                graph: GraphOptions {
                    logical_partition: logical_partition
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(runtime_error)?
                        .unwrap_or_default(),
                    snapshot_time,
                    require_consistent_snapshot,
                },
                ..Default::default()
            },
        })
    }

    #[pyo3(signature = (statement, *, language="cypher", parameters=None))]
    fn query(
        &self,
        py: Python<'_>,
        statement: &str,
        language: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyQuery> {
        let language = match language {
            "cypher" => Language::Cypher,
            "sql" => Language::Sql,
            "gremlin" => Language::Gremlin,
            _ => {
                return Err(PyValueError::new_err(
                    "language must be cypher, sql or gremlin",
                ));
            }
        };
        let parameters = parameters
            .map(|value| -> PyResult<_> {
                let json = PyModule::import(py, "json")?
                    .getattr("dumps")?
                    .call1((value,))?
                    .extract::<String>()?;
                serde_json::from_str(&json).map_err(runtime_error)
            })
            .transpose()?;
        self.run(py, |mut options| {
            options.parameters = parameters.unwrap_or_default();
            self.engine.query(language, statement, options)
        })
    }

    #[pyo3(signature = (edge, start, max_hops, *, mode="reachable", target=None, min_hops=1, incoming=false, strategy="auto"))]
    #[allow(clippy::too_many_arguments)]
    fn traverse(
        &self,
        py: Python<'_>,
        edge: String,
        start: Vec<String>,
        max_hops: usize,
        mode: &str,
        target: Option<Vec<String>>,
        min_hops: usize,
        incoming: bool,
        strategy: &str,
    ) -> PyResult<PyQuery> {
        let kind = match (mode, target) {
            ("reachable", None) => TraversalKind::Reachable,
            ("paths", None) => TraversalKind::Paths { min_hops },
            ("shortest", Some(target)) => TraversalKind::Shortest {
                target: key(target),
            },
            _ => {
                return Err(PyValueError::new_err(
                    "mode must be reachable, paths, or shortest with a target",
                ));
            }
        };
        let strategy = access(strategy)?;
        let request = Traversal {
            edge,
            start: key(start),
            max_hops,
            kind,
            direction: if incoming {
                Direction::Incoming
            } else {
                Direction::Outgoing
            },
        };
        self.run(py, |options| {
            self.engine
                .traverse_with_strategy(request, options, strategy)
        })
    }

    #[pyo3(signature = (edges, algorithm, *, nodes=Vec::new(), weight=None, direction="outgoing", settings="{}"))]
    #[allow(clippy::too_many_arguments)]
    fn analyze_projection(
        &self,
        py: Python<'_>,
        edges: Vec<String>,
        algorithm: &str,
        nodes: Vec<String>,
        weight: Option<String>,
        direction: &str,
        settings: &str,
    ) -> PyResult<PyQuery> {
        let algorithm = match algorithm {
            "pagerank" => {
                #[derive(serde::Deserialize)]
                #[serde(default, deny_unknown_fields)]
                struct PageRank {
                    damping: f64,
                    tolerance: f64,
                    max_iterations: usize,
                }
                impl Default for PageRank {
                    fn default() -> Self {
                        Self {
                            damping: 0.85,
                            tolerance: 1e-9,
                            max_iterations: 100,
                        }
                    }
                }
                let PageRank {
                    damping,
                    tolerance,
                    max_iterations,
                } = settings_json(settings)?;
                ProjectionAlgorithm::PageRank {
                    damping,
                    tolerance,
                    max_iterations,
                }
            }
            "weak_components" => {
                let settings: serde_json::Map<String, serde_json::Value> = settings_json(settings)?;
                if !settings.is_empty() {
                    return Err(PyValueError::new_err(
                        "weak_components has no algorithm settings",
                    ));
                }
                ProjectionAlgorithm::WeaklyConnectedComponents
            }
            "louvain" => ProjectionAlgorithm::Louvain(settings_json(settings)?),
            "leiden" => ProjectionAlgorithm::Leiden(settings_json(settings)?),
            "node2vec" => ProjectionAlgorithm::Node2Vec(settings_json(settings)?),
            "label_propagation" => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Settings {
                    max_iterations: Option<usize>,
                }
                ProjectionAlgorithm::LabelPropagation {
                    max_iterations: settings_json::<Settings>(settings)?
                        .max_iterations
                        .unwrap_or(20),
                }
            }
            _ => return Err(PyValueError::new_err("unknown projected graph algorithm")),
        };
        let projection = GraphProjection {
            nodes,
            edges,
            weight,
            direction: projection_direction(direction)?,
        };
        self.run(py, |options| {
            self.engine
                .analyze_projection(projection, algorithm, options)
        })
    }

    #[pyo3(signature = (edges, start_label, start, target_label, target, *, k=1, weight=None, direction="outgoing", strategy="auto"))]
    #[allow(clippy::too_many_arguments)]
    fn route(
        &self,
        py: Python<'_>,
        edges: Vec<String>,
        start_label: String,
        start: Vec<String>,
        target_label: String,
        target: Vec<String>,
        k: usize,
        weight: Option<String>,
        direction: &str,
        strategy: &str,
    ) -> PyResult<PyQuery> {
        let projection = GraphProjection {
            edges,
            weight,
            direction: projection_direction(direction)?,
            ..Default::default()
        };
        let route = Route {
            start: VertexKey {
                label: start_label,
                key: key(start),
            },
            target: VertexKey {
                label: target_label,
                key: key(target),
            },
            k,
        };
        let strategy = strategy.parse().map_err(runtime_error)?;
        self.run(py, |options| {
            self.engine
                .route_with_strategy(projection, route, strategy, options)
        })
    }

    #[pyo3(signature = (edge, algorithm, *, strategy="auto", damping=0.85, tolerance=1e-9, max_iterations=100))]
    #[allow(clippy::too_many_arguments)]
    fn analyze<'py>(
        &self,
        py: Python<'py>,
        edge: &str,
        algorithm: &str,
        strategy: &str,
        damping: f64,
        tolerance: f64,
        max_iterations: usize,
    ) -> PyResult<PyQuery> {
        let algorithm = match algorithm {
            "pagerank" => GraphAlgorithm::PageRank {
                damping,
                tolerance,
                max_iterations,
            },
            "weak_components" => GraphAlgorithm::WeaklyConnectedComponents,
            _ => {
                return Err(PyValueError::new_err(
                    "algorithm must be pagerank or weak_components",
                ));
            }
        };
        let strategy = access(strategy)?;
        self.run(py, |options| {
            self.engine
                .analyze_graph(edge, algorithm, options, strategy)
        })
    }

    fn refresh<'py>(&self, py: Python<'py>, edge: &str) -> PyResult<Bound<'py, PyAny>> {
        let options = self.options.clone();
        let report = py
            .detach(|| self.runtime.block_on(self.engine.refresh(edge, options)))
            .map_err(runtime_error)?;
        to_python(py, &report)
    }
}

impl PyEngine {
    fn run<F>(&self, py: Python<'_>, start: impl FnOnce(QueryOptions) -> F) -> PyResult<PyQuery>
    where
        F: Future<Output = anyhow::Result<QueryResult>> + Send,
    {
        let cancellation = CancellationToken::new();
        let options = QueryOptions {
            cancellation: cancellation.clone(),
            ..self.options.clone()
        };
        let query = start(options);
        let result = py
            .detach(|| self.runtime.block_on(query))
            .map_err(runtime_error)?;
        Ok(PyQuery {
            schema: result.batches.schema(),
            versions: result.versions.clone(),
            plan: result.physical_plan.clone(),
            result: Mutex::new(Some(result)),
            runtime: Arc::clone(&self.runtime),
            cancellation,
        })
    }
}

#[pyclass(name = "Query", frozen)]
struct PyQuery {
    result: Mutex<Option<QueryResult>>,
    runtime: Arc<Runtime>,
    cancellation: CancellationToken,
    schema: SchemaRef,
    versions: Vec<SourceVersion>,
    plan: String,
}

#[pymethods]
impl PyQuery {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let batch = py
            .detach(|| {
                let mut result = self
                    .result
                    .lock()
                    .map_err(|_| anyhow::anyhow!("query state is unavailable"))?;
                let Some(result) = result.as_mut() else {
                    return Ok::<_, anyhow::Error>(None);
                };
                Ok(self.runtime.block_on(result.batches.try_next())?)
            })
            .map_err(runtime_error)?;
        batch.map(|batch| batch.to_pyarrow(py)).transpose()
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _ty: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }

    #[getter]
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    #[getter]
    fn versions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_python(py, &self.versions)
    }

    #[getter]
    fn physical_plan(&self) -> &str {
        &self.plan
    }

    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let metrics = py
            .detach(|| {
                let result = self
                    .result
                    .lock()
                    .map_err(|_| anyhow::anyhow!("query state is unavailable"))?;
                Ok::<_, anyhow::Error>(result.as_ref().context("query has been closed")?.metrics())
            })
            .map_err(runtime_error)?;
        to_python(py, &metrics)
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.cancel();
        py.detach(|| {
            let _runtime = self.runtime.enter();
            self.result
                .lock()
                .map_err(|_| anyhow::anyhow!("query state is unavailable"))?
                .take();
            Ok::<_, anyhow::Error>(())
        })
        .map_err(runtime_error)
    }
}

impl Drop for PyQuery {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let _runtime = self.runtime.enter();
        if let Ok(state) = self.result.get_mut() {
            state.take();
        }
    }
}

fn key(values: Vec<String>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .map(|value| ScalarValue::Utf8(Some(value)))
        .collect()
}

fn settings_json<T: serde::de::DeserializeOwned>(settings: &str) -> PyResult<T> {
    serde_json::from_str(settings).map_err(|error| PyValueError::new_err(error.to_string()))
}

fn projection_direction(direction: &str) -> PyResult<Direction> {
    match direction {
        "outgoing" => Ok(Direction::Outgoing),
        "incoming" => Ok(Direction::Incoming),
        "undirected" => Ok(Direction::Undirected),
        _ => Err(PyValueError::new_err(
            "direction must be outgoing, incoming, or undirected",
        )),
    }
}

fn access(strategy: &str) -> PyResult<TraversalStrategy> {
    Ok(match strategy {
        "auto" => TraversalStrategy::Auto,
        "scan" => TraversalStrategy::Scan,
        "batch" => TraversalStrategy::Batch,
        "cached" => TraversalStrategy::Cached,
        _ => {
            return Err(PyValueError::new_err(
                "strategy must be auto, scan, batch or cached",
            ));
        }
    })
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("{error:#}"))
}

fn to_python<'py>(py: Python<'py>, value: &impl serde::Serialize) -> PyResult<Bound<'py, PyAny>> {
    let text = serde_json::to_string(value).map_err(runtime_error)?;
    PyModule::import(py, "json")?.call_method1("loads", (text,))
}

#[pymodule]
fn graphlake(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyEngine>()?;
    module.add_class::<PyQuery>()?;
    Ok(())
}
