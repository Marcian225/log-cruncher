use pyo3::prelude::*;
use serde::Deserialize;
use rayon::prelude::*;
use std::io::{BufRead, BufReader};
use std::fs::File;


/// A Python module implemented in Rust.
#[pymodule]
mod log_cruncher {

    use pyo3::{exceptions::PyValueError, prelude::*};

    use crate::{LogEntry, filter_errors, parse_csv, parse_json_lines};

    #[pyfunction]
    fn process_logs_csv(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[pyfunction]
    fn get_error_logs(file_path: &str) -> PyResult<Vec<LogEntry>>{
        let logs = parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(filter_errors(logs))
    }

    #[pyfunction]

    fn process_logs_json(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_json_lines(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }

}

#[derive(Deserialize, Debug, Clone)]
#[pyclass]
struct LogEntry {
    #[pyo3(get)]
    timestamp: String,
    #[pyo3(get)]
    level: String,
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    user_id: u32,
}

#[pymethods]
impl LogEntry {
    fn __repr__(&self) -> String {
        format!("LogEntry {{ timestamp: {}, level: {}, message: {}, user_id: {} }}", self.timestamp, self.level, self.message, self.user_id)
    }
}


// Function to parse CSV file and return a vector of LogEntry structs
fn parse_csv(file_path: &str) -> Result<Vec<LogEntry>, csv::Error>{
    let mut reader = csv::Reader::from_path(file_path)?;
    let mut output: Vec<LogEntry> = vec![];
    
    for result in reader.deserialize(){
        let record = result?;
        output.push(record);
    }
    
    return Ok(output);
}

// function to filter log entries for errors parallel using rayon into_par_iter()
// example function to extract specific log entries 
fn filter_errors(logs: Vec<LogEntry>) -> Vec<LogEntry> {
    logs.into_par_iter().filter(|log| log.level =="ERROR").collect()
}



fn parse_json_lines(file_path: &str) -> Result<Vec<LogEntry>, std::io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut output: Vec<LogEntry> = vec![];

    for line in reader.lines(){
        let line_str = line?;
        
        match serde_json::from_str::<LogEntry>(&line_str) {
            Ok(record) => output.push(record),
            Err(err) => eprintln!("Failed to parse line: {}. Error: {:?}", line_str, err),
        }
    }

    return Ok(output)
}