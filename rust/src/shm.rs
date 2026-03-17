use pyo3::prelude::*;
use serde_json::Value;

pub fn decode_blobs_in_result(py: Python<'_>, ipc_mode: &str, payload: &mut Value) -> PyResult<()> {
    if ipc_mode != "shm" {
        return Ok(());
    }
    let obj = payload.as_object_mut().unwrap();
    for key in ["stdout_blob", "stderr_blob"] {
        let blob = match obj.get(key) {
            Some(v) => v.clone(),
            None => continue,
        };
        let kind = blob.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let text = if kind == "inline" {
            blob.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string()
        } else if kind == "shm" {
            let name = blob.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let size = blob.get("size").and_then(|v| v.as_i64()).unwrap_or(0);
            let encoding = blob.get("encoding").and_then(|v| v.as_str()).unwrap_or("utf-8");

            let shm_mod = py.import("pytest_rxdist.shm")?;
            let ref_cls = shm_mod.getattr("ShmTextRef")?;
            let ref_obj = ref_cls.call1((name, size, encoding))?;
            let read_fn = shm_mod.getattr("read_text_from_shm")?;
            let cleanup_fn = shm_mod.getattr("cleanup_shm")?;
            let out: String = read_fn.call1((ref_obj.clone(),))?.extract()?;
            let _ = cleanup_fn.call1((ref_obj,));
            out
        } else {
            "".to_string()
        };

        if key == "stdout_blob" {
            obj.insert("stdout".to_string(), Value::String(text));
        } else {
            obj.insert("stderr".to_string(), Value::String(text));
        }
    }
    Ok(())
}

