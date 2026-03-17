use pyo3::prelude::*;

#[pyfunction]
fn engine_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyfunction]
fn hello(name: &str) -> String {
    format!("hello, {name}")
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(engine_version, m)?)?;
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    Ok(())
}
