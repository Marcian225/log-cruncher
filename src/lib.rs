use pyo3::prelude::*;
use serde::Deserialize;


/// A Python module implemented in Rust.
#[pymodule]
mod log_cruncher {

    use pyo3::{exceptions::PyValueError, prelude::*};

    use crate::{LogEntry, parse_csv};

    /// Formats the sum of two numbers as string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }


    #[pyfunction]
    fn process_logs_csv(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }

}

#[derive(Deserialize, Debug, Clone)]
#[pyclass]
struct LogEntry {
    timestamp: String,
    level: String,
    message: String,
    user_id: u32,
}

fn parse_csv(file_path: &str) -> Result<Vec<LogEntry>, csv::Error>{
    let mut reader = csv::Reader::from_path(file_path)?;

    let mut output: Vec<LogEntry> = vec![];


    for result in reader.deserialize(){
        let record = result?;
        output.push(record);
    }

    return Ok(output);

}