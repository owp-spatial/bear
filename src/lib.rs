use pyo3::prelude::*;

mod plugins;

#[pymodule]
fn _plugins(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
