use pyo3::prelude::*;
use std::collections::HashMap;

mod controller;
mod ipc;
mod scheduler;
mod shm;

#[pyfunction]
fn engine_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyfunction]
fn hello(name: &str) -> String {
    format!("hello, {name}")
}

#[pyfunction]
fn worker_main(py: Python<'_>) -> PyResult<i32> {
    // Keep worker harness implementation in lib.rs for now.
    crate::worker_main_impl(py)
}

#[pyfunction]
fn run_session<'py>(
    py: Python<'py>,
    nodeids: Vec<String>,
    units: Option<Vec<Vec<String>>>,
    num_workers: i64,
    scheduler: String,
    reuse_mode: String,
    worker_kind: String,
    debug: bool,
) -> PyResult<Bound<'py, pyo3::types::PyList>> {
    // Best-effort: load average durations once per session when using the smart scheduler.
    let mut avg: HashMap<String, f64> = HashMap::new();
    if scheduler.trim().eq_ignore_ascii_case("smart") {
        let ts_mod = py.import("pytest_rxdist.timing_store")?;
        let path_mod = py.import("pathlib")?;
        let cwd = path_mod.getattr("Path")?.call0()?;
        let default_path = ts_mod.getattr("default_timings_path")?.call1((cwd,))?;
        let store_cls = ts_mod.getattr("TimingStore")?;
        if default_path.getattr("exists")?.call0()?.extract::<bool>()? {
            let store = store_cls.getattr("open")?.call1((default_path,))?;
            let map = store.call_method1("avg_durations", (nodeids.clone(),))?;
            avg = map.extract::<HashMap<String, f64>>()?;
            let _ = store.call_method0("close");
        }
    }

    let units = units.unwrap_or_default();
    let out = controller::run_session(
        py,
        nodeids,
        units,
        num_workers.max(1) as usize,
        scheduler.trim().to_lowercase().as_str(),
        reuse_mode.trim().to_lowercase().as_str(),
        worker_kind.trim().to_lowercase().as_str(),
        debug,
        avg,
    )?;
    let list = pyo3::types::PyList::empty(py);
    for v in out {
        list.append(value_to_py(py, &v)?)?;
    }
    Ok(list)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(engine_version, m)?)?;
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_function(wrap_pyfunction!(run_session, m)?)?;
    m.add_function(wrap_pyfunction!(worker_main, m)?)?;
    Ok(())
}

// Existing worker harness impl (kept local to avoid further module churn yet).
fn worker_main_impl(py: Python<'_>) -> PyResult<i32> {
    let ipc_mode = std::env::var("PYTEST_RXDIST_IPC").unwrap_or_else(|_| "baseline".to_string());
    let threshold = std::env::var("PYTEST_RXDIST_SHM_THRESHOLD_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8192);

    let hello_payload = serde_json::json!({"pid": std::process::id()});
    ipc::write_frame(std::io::stdout().lock(), "hello", &hello_payload)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ipc hello failed: {e}")))?;

    let worker_mod = py.import("pytest_rxdist.worker")?;
    let run_one = worker_mod.getattr("run_one")?;

    loop {
        let (msg_type, payload) = match ipc::read_frame(std::io::stdin().lock()) {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(0),
            Err(_) => return Ok(0),
        };

        match msg_type.as_str() {
            "shutdown" => return Ok(0),
            "run" => {
                let nodeid = payload
                    .get("nodeid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let r = run_one.call1((nodeid.clone(),))?;
                let out_nodeid: String = r.getattr("nodeid")?.extract()?;
                let outcome: String = r.getattr("outcome")?.extract()?;
                let duration_s: f64 = r.getattr("duration_s")?.extract()?;
                let returncode: i64 = r.getattr("returncode")?.extract()?;
                let stdout_s: String = r.getattr("stdout")?.extract()?;
                let stderr_s: String = r.getattr("stderr")?.extract()?;

                let mut payload = serde_json::json!({
                    "nodeid": out_nodeid,
                    "outcome": outcome,
                    "duration_s": duration_s,
                    "returncode": returncode,
                });

                if ipc_mode.trim().to_lowercase() == "shm" {
                    payload["stdout_blob"] = crate::controller::blobify_text(py, &stdout_s, threshold)?;
                    payload["stderr_blob"] = crate::controller::blobify_text(py, &stderr_s, threshold)?;
                } else {
                    payload["stdout"] = serde_json::Value::String(stdout_s);
                    payload["stderr"] = serde_json::Value::String(stderr_s);
                }

                ipc::write_frame(std::io::stdout().lock(), "result", &payload)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ipc send failed: {e}")))?;
            }
            "run_batch" => {
                let nodeids = payload
                    .get("nodeids")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut results: Vec<serde_json::Value> = Vec::with_capacity(nodeids.len());
                for v in nodeids {
                    let nodeid = v.as_str().unwrap_or("").to_string();
                    let r = run_one.call1((nodeid.clone(),))?;
                    let out_nodeid: String = r.getattr("nodeid")?.extract()?;
                    let outcome: String = r.getattr("outcome")?.extract()?;
                    let duration_s: f64 = r.getattr("duration_s")?.extract()?;
                    let returncode: i64 = r.getattr("returncode")?.extract()?;
                    let stdout_s: String = r.getattr("stdout")?.extract()?;
                    let stderr_s: String = r.getattr("stderr")?.extract()?;

                    let mut payload = serde_json::json!({
                        "nodeid": out_nodeid,
                        "outcome": outcome,
                        "duration_s": duration_s,
                        "returncode": returncode,
                    });
                    if ipc_mode.trim().to_lowercase() == "shm" {
                        payload["stdout_blob"] = crate::controller::blobify_text(py, &stdout_s, threshold)?;
                        payload["stderr_blob"] = crate::controller::blobify_text(py, &stderr_s, threshold)?;
                    } else {
                        payload["stdout"] = serde_json::Value::String(stdout_s);
                        payload["stderr"] = serde_json::Value::String(stderr_s);
                    }
                    results.push(payload);
                }
                let batch_payload = serde_json::json!({"results": results});
                ipc::write_frame(std::io::stdout().lock(), "results_batch", &batch_payload)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ipc send failed: {e}")))?;
            }
            _ => {}
        }
    }
}

fn value_to_py<'py>(py: Python<'py>, v: &serde_json::Value) -> PyResult<Bound<'py, pyo3::PyAny>> {
    use pyo3::types::{PyDict, PyList};
    let obj: PyObject = match v {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => b.into_py(py),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py(py)
            } else if let Some(u) = n.as_u64() {
                u.into_py(py)
            } else {
                n.as_f64().unwrap_or(0.0).into_py(py)
            }
        }
        serde_json::Value::String(s) => s.into_py(py),
        serde_json::Value::Array(arr) => {
            let l = PyList::empty(py);
            for x in arr {
                l.append(value_to_py(py, x)?)?;
            }
            l.into_py(py)
        }
        serde_json::Value::Object(map) => {
            let d = PyDict::new(py);
            for (k, x) in map.iter() {
                d.set_item(k, value_to_py(py, x)?)?;
            }
            d.into_py(py)
        }
    };
    Ok(obj.into_bound(py))
}
