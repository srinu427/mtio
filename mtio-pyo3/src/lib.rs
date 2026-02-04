use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
mod mtio {
    use std::{io, path::Path};

    use mtio_sys::rayon;
    use pyo3::prelude::*;

    #[pyfunction]
    fn du(py: Python<'_>, path: String, threads: usize) -> PyResult<u64> {
        py.detach(move || {
            let tp = rayon::ThreadPoolBuilder::default()
                .num_threads(threads)
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))
                .map_err(PyErr::from)?;
            let size = tp.install(|| mtio_sys::du::du(Path::new(&path)).map_err(PyErr::from))?;
            Ok(size)
        })
    }
}
