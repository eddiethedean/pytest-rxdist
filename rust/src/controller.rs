use crate::ipc;
use crate::scheduler;
use crate::shm;
use pyo3::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct Worker {
    pub idx: usize,
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
}

fn failure_result(nodeid: &str, why: &str) -> Value {
    serde_json::json!({
        "nodeid": nodeid,
        "outcome": "failed",
        "duration_s": 0.0,
        "returncode": 1,
        "stdout": "",
        "stderr": why,
    })
}

pub fn blobify_text(py: Python<'_>, text: &str, threshold: usize) -> PyResult<Value> {
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

fn spawn_worker(py_exe: &str, kind: &str, env: &HashMap<String, String>, idx: usize) -> std::io::Result<Worker> {
    let mut cmd = Command::new(py_exe);
    if kind == "rust" {
        cmd.arg("-c")
            .arg("from pytest_rxdist import _core; raise SystemExit(_core.worker_main())");
    } else {
        cmd.arg("-m").arg("pytest_rxdist._worker_main");
    }
    cmd.envs(env.iter().map(|(k, v)| (k, v)));
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit());
    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no stdin"))?;
    let stdout = child.stdout.take().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no stdout"))?;
    Ok(Worker { idx, child, stdin, stdout })
}

fn wait_hello(mut r: impl Read) -> std::io::Result<()> {
    while let Some((t, _p)) = ipc::read_frame(&mut r)? {
        if t == "hello" {
            return Ok(());
        }
    }
    Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF waiting for hello"))
}

fn send_run(w: &mut Worker, nodeids: &[String]) -> std::io::Result<()> {
    let payload = if nodeids.len() == 1 {
        serde_json::json!({"nodeid": nodeids[0]})
    } else {
        serde_json::json!({"nodeids": nodeids})
    };
    let ty = if nodeids.len() == 1 { "run" } else { "run_batch" };
    ipc::write_frame(&mut w.stdin, ty, &payload)
}

fn send_shutdown(w: &mut Worker) {
    let _ = ipc::write_frame(&mut w.stdin, "shutdown", &serde_json::json!({}));
}

fn wait_results_for(
    stdout: &mut impl Read,
    expected: &[String],
) -> std::io::Result<Vec<Value>> {
    let mut want: HashSet<String> = expected.iter().cloned().collect();
    let mut got: Vec<Value> = Vec::new();
    while !want.is_empty() {
        let (ty, payload) = match ipc::read_frame(&mut *stdout)? {
            None => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF")),
            Some(x) => x,
        };
        if ty == "result" {
            if let Some(nid) = payload.get("nodeid").and_then(|v| v.as_str()) {
                if want.remove(nid) {
                    got.push(payload);
                }
            }
        } else if ty == "results_batch" {
            if let Some(arr) = payload.get("results").and_then(|v| v.as_array()) {
                for r in arr {
                    if let Some(nid) = r.get("nodeid").and_then(|v| v.as_str()) {
                        if want.remove(nid) {
                            got.push(r.clone());
                        }
                    }
                }
            }
        }
    }
    Ok(got)
}

pub fn run_session(
    py: Python<'_>,
    nodeids: Vec<String>,
    units: Vec<Vec<String>>,
    num_workers: usize,
    scheduler_mode: &str,
    reuse_mode: &str,
    worker_kind: &str,
    debug: bool,
    avg: HashMap<String, f64>,
) -> PyResult<Vec<Value>> {
    let ipc_mode = std::env::var("PYTEST_RXDIST_IPC").unwrap_or_else(|_| "baseline".to_string());
    let ipc_mode = ipc_mode.trim().to_lowercase();
    let batch_size_env = std::env::var("PYTEST_RXDIST_IPC_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);
    let batch_size = batch_size_env.max(1);

    let sys = py.import("sys")?;
    let py_exe: String = sys.getattr("executable")?.extract()?;

    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.insert("PYTEST_RXDIST_WORKER".to_string(), "1".to_string());
    env.insert("PYTEST_RXDIST_REUSE".to_string(), reuse_mode.to_string());
    env.insert("PYTEST_RXDIST_IPC".to_string(), ipc_mode.clone());

    let units_effective = if units.is_empty() {
        nodeids.iter().map(|n| vec![n.clone()]).collect::<Vec<_>>()
    } else {
        units
    };

    // Spawn workers + handshake.
    let mut workers: Vec<Worker> = Vec::new();
    for i in 0..num_workers.max(1) {
        let mut w = spawn_worker(&py_exe, worker_kind, &env, i)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("spawn worker failed: {e}")))?;
        wait_hello(&mut w.stdout)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("worker hello failed: {e}")))?;
        workers.push(w);
    }

    let results: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));

    // Build per-worker nodeid sequence for smart scheduler; baseline uses shared unit queue.
    let mut per_worker: Option<Vec<Vec<String>>> = None;
    if scheduler_mode == "smart" {
        let sched = scheduler::smart_schedule_units(&units_effective, workers.len(), &avg);
        per_worker = Some(sched.per_worker);
        if debug {
            // best-effort: no stdout printing from Rust controller (pytest terminalreporter is Python side)
            let _ = sched;
        }
    }

    if let Some(per_worker_nodeids) = per_worker {
        // Smart: each worker gets a fixed sequence, but we still batch for IPC.
        let mut threads = Vec::new();
        for mut w in workers {
            let queue = per_worker_nodeids.get(w.idx).cloned().unwrap_or_default();
            let results_out = results.clone();
            let ipc_mode_local = ipc_mode.clone();
            let reuse_mode_local = reuse_mode.to_string();
            let t = thread::spawn(move || {
                let mut respawned = false;
                let mut i = 0usize;
                while i < queue.len() {
                    let batch = queue[i..(i + batch_size).min(queue.len())].to_vec();
                    if send_run(&mut w, &batch).is_err() {
                        let mut out = results_out.lock().unwrap();
                        out.push(failure_result(&batch[0], "worker died before receiving work"));
                        for nid in queue[i + 1..].iter() {
                            out.push(failure_result(nid, "worker died before running test"));
                        }
                        break;
                    }
                    match wait_results_for(&mut w.stdout, &batch) {
                        Ok(mut rs) => {
                            results_out.lock().unwrap().append(&mut rs);
                            i += batch.len();
                        }
                        Err(_) => {
                            if reuse_mode_local == "safe" && !respawned {
                                respawned = true;
                                // TODO: implement real respawn; for now mark failures for remaining.
                            }
                            let mut out = results_out.lock().unwrap();
                            out.push(failure_result(&batch[0], "worker died before reporting result"));
                            for nid in queue[i + 1..].iter() {
                                out.push(failure_result(nid, "worker died before running test"));
                            }
                            break;
                        }
                    }
                }
                send_shutdown(&mut w);
                let _ = w.child.kill();
                let _ = w.child.wait();
                let _ = ipc_mode_local;
            });
            threads.push(t);
        }
        for t in threads {
            let _ = t.join();
        }
    } else {
        // Baseline: shared unit queue; units are atomic, but within a unit we batch nodeids.
        let queue: Arc<Mutex<VecDeque<Vec<String>>>> = Arc::new(Mutex::new(units_effective.into()));
        let mut threads = Vec::new();
        for mut w in workers {
            let q = queue.clone();
            let results_out = results.clone();
            let reuse_mode_local = reuse_mode.to_string();
            let t = thread::spawn(move || {
                let mut respawned = false;
                loop {
                    let unit = { q.lock().unwrap().pop_front() };
                    let Some(unit) = unit else {
                        send_shutdown(&mut w);
                        break;
                    };

                    let mut i = 0usize;
                    while i < unit.len() {
                        let batch = unit[i..(i + batch_size).min(unit.len())].to_vec();
                        if send_run(&mut w, &batch).is_err() {
                            let mut out = results_out.lock().unwrap();
                            out.push(failure_result(&batch[0], "worker died before receiving work"));
                            for nid in unit[i + 1..].iter() {
                                out.push(failure_result(nid, "worker died before running test"));
                            }
                            while let Some(remaining_unit) = q.lock().unwrap().pop_front() {
                                for nid in remaining_unit {
                                    out.push(failure_result(&nid, "worker died before running test"));
                                }
                            }
                            return;
                        }
                        match wait_results_for(&mut w.stdout, &batch) {
                            Ok(mut rs) => {
                                results_out.lock().unwrap().append(&mut rs);
                                i += batch.len();
                            }
                            Err(_) => {
                                if reuse_mode_local == "safe" && !respawned {
                                    respawned = true;
                                }
                                let mut out = results_out.lock().unwrap();
                                out.push(failure_result(&batch[0], "worker died before reporting result"));
                                for nid in unit[i + 1..].iter() {
                                    out.push(failure_result(nid, "worker died before running test"));
                                }
                                while let Some(remaining_unit) = q.lock().unwrap().pop_front() {
                                    for nid in remaining_unit {
                                        out.push(failure_result(&nid, "worker died before running test"));
                                    }
                                }
                                return;
                            }
                        }
                    }
                }
                let _ = w.child.kill();
                let _ = w.child.wait();
            });
            threads.push(t);
        }
        for t in threads {
            let _ = t.join();
        }
    }

    let mut out = results.lock().unwrap().clone();
    // Decode shm blobs under the GIL.
    for r in out.iter_mut() {
        let _ = shm::decode_blobs_in_result(py, &ipc_mode, r);
    }
    Ok(out)
}

