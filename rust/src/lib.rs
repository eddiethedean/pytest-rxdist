use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList};
use serde_json::Value;
use std::io::{Read, Write};

#[pyfunction]
fn engine_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyfunction]
fn hello(name: &str) -> String {
    format!("hello, {name}")
}

fn read_exact_or_eof(mut r: impl Read, n: usize) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = vec![0u8; n];
    let mut read_total = 0usize;
    while read_total < n {
        let m = r.read(&mut buf[read_total..])?;
        if m == 0 {
            if read_total == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"));
        }
        read_total += m;
    }
    Ok(Some(buf))
}

fn read_frame(mut r: impl Read) -> std::io::Result<Option<(String, Value)>> {
    let hdr = match read_exact_or_eof(&mut r, 4)? {
        None => return Ok(None),
        Some(h) => h,
    };
    let size = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let data = read_exact_or_eof(&mut r, size)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"))?;
    // Decode as a generic map to match Python msgpack expectations.
    let obj: Value = rmp_serde::from_slice(&data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("msgpack decode: {e}")))?;
    let msg_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let payload = obj.get("payload").cloned().unwrap_or_else(|| serde_json::json!({}));
    Ok(Some((msg_type, payload)))
}

fn write_frame(mut w: impl Write, msg_type: &str, payload: &Value) -> std::io::Result<()> {
    let frame = serde_json::json!({"type": msg_type, "payload": payload});
    let data = rmp_serde::to_vec(&frame)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("msgpack encode: {e}")))?;
    let size = (data.len() as u32).to_be_bytes();
    w.write_all(&size)?;
    w.write_all(&data)?;
    w.flush()?;
    Ok(())
}

fn blobify_text(py: Python<'_>, text: &str, threshold: usize) -> PyResult<Value> {
    if text.is_empty() {
        return Ok(serde_json::json!({"kind":"inline","text":""}));
    }
    let encoded_len = text.as_bytes().len();
    if encoded_len < threshold {
        return Ok(serde_json::json!({"kind":"inline","text":text}));
    }
    let shm_mod = py.import("pytest_rxdist.shm")?;
    let write_fn = shm_mod.getattr("write_text_to_shm")?;
    let r = write_fn.call1((text,))?;
    let name: String = r.getattr("name")?.extract()?;
    let size: usize = r.getattr("size")?.extract()?;
    let encoding: String = r.getattr("encoding")?.extract()?;
    Ok(serde_json::json!({"kind":"shm","name":name,"size":size,"encoding":encoding}))
}

#[pyfunction]
fn worker_main(py: Python<'_>) -> PyResult<i32> {
    // Rust worker harness that embeds Python and calls pytest_rxdist.worker.run_one.
    // IPC schema matches src/pytest_rxdist/_worker_main.py (length-prefixed msgpack frames).
    let ipc_mode = std::env::var("PYTEST_RXDIST_IPC").unwrap_or_else(|_| "baseline".to_string());
    let threshold = std::env::var("PYTEST_RXDIST_SHM_THRESHOLD_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8192);

    // Send hello
    let hello_payload = serde_json::json!({"pid": std::process::id()});
    write_frame(std::io::stdout().lock(), "hello", &hello_payload)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ipc hello failed: {e}")))?;

    let worker_mod = py.import("pytest_rxdist.worker")?;
    let run_one = worker_mod.getattr("run_one")?;

    loop {
        let (msg_type, payload) = match read_frame(std::io::stdin().lock()) {
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
                    let so = blobify_text(py, &stdout_s, threshold)?;
                    let se = blobify_text(py, &stderr_s, threshold)?;
                    payload["stdout_blob"] = so;
                    payload["stderr_blob"] = se;
                } else {
                    payload["stdout"] = Value::String(stdout_s);
                    payload["stderr"] = Value::String(stderr_s);
                }

                write_frame(std::io::stdout().lock(), "result", &payload)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ipc send failed: {e}")))?;
            }
            "run_batch" => {
                let nodeids = payload
                    .get("nodeids")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut results: Vec<Value> = Vec::with_capacity(nodeids.len());
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
                        payload["stdout_blob"] = blobify_text(py, &stdout_s, threshold)?;
                        payload["stderr_blob"] = blobify_text(py, &stderr_s, threshold)?;
                    } else {
                        payload["stdout"] = Value::String(stdout_s);
                        payload["stderr"] = Value::String(stderr_s);
                    }
                    results.push(payload);
                }
                let batch_payload = serde_json::json!({"results": results});
                write_frame(std::io::stdout().lock(), "results_batch", &batch_payload)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ipc send failed: {e}")))?;
            }
            _ => {}
        }
    }
}

#[pyfunction]
fn run_session<'py>(
    py: Python<'py>,
    nodeids: Bound<'py, PyAny>,
    units: Option<Bound<'py, PyAny>>,
    num_workers: i64,
    scheduler: String,
    reuse_mode: String,
    debug: bool,
) -> PyResult<Bound<'py, PyList>> {
    // MVP Rust engine: delegate execution to existing Python controller to prove the boundary
    // and provide a stable place to incrementally port logic to Rust.
    //
    // Later phases replace this delegation with native Rust controller/scheduler/IPC.
    let ctrl_mod = py.import("pytest_rxdist.controller")?;
    let ctrl_cls = ctrl_mod.getattr("RXDistController")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("num_workers", num_workers)?;
    kwargs.set_item("scheduler", scheduler)?;
    kwargs.set_item("reuse_mode", reuse_mode)?;
    kwargs.set_item("debug", debug)?;
    let ctrl = ctrl_cls.call((), Some(&kwargs))?;

    let results = if let Some(units_any) = units {
        // controller.run(nodeids, units=units)
        let run_kwargs = PyDict::new(py);
        run_kwargs.set_item("units", units_any)?;
        ctrl.call_method("run", (nodeids,), Some(&run_kwargs))?
    } else {
        ctrl.call_method("run", (nodeids,), None)?
    };

    Ok(results.downcast_into::<PyList>()?)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(engine_version, m)?)?;
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_function(wrap_pyfunction!(run_session, m)?)?;
    m.add_function(wrap_pyfunction!(worker_main, m)?)?;
    Ok(())
}
