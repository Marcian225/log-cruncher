use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use serde::Deserialize;
use rayon::prelude::*;
use std::io::{BufRead, BufReader, Lines};
use std::fs::File;
use csv::Reader;


/// A Python module implemented in Rust.
#[pymodule]
mod log_cruncher {

    use pyo3::{exceptions::PyValueError, prelude::*};

    use crate::{LogEntry, filter_by_level, parse_csv, parse_json_lines};

    #[pyfunction] // Process whole CSV file and return all log entries as a list of LogEntry objects
    fn process_logs_csv(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[pyfunction] // Process whole JSONL file and return all log entries as a list of LogEntry objects
    fn process_logs_json(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_json_lines(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }
 
    #[pyfunction] // Filter log entries by level from a CSV file and return the filtered list of LogEntry objects
    fn filter_csv_by_level(file_path: &str, target_level: &str) -> PyResult<Vec<LogEntry>>{
        let logs = parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(filter_by_level(logs, target_level))
    }

    #[pyfunction] // Filter log entries by level from a JSONL file and return the filtered list of LogEntry objects
    fn filter_json_by_level(file_path: &str, target_level: &str) -> PyResult<Vec<LogEntry>>{
        let logs = parse_json_lines(file_path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(filter_by_level(logs, target_level))
    }

    #[pymodule_export]
    use crate::BatchLogProcessor;

}

enum LogReader {
    Csv(Reader<File>),
    Jsonl(Lines<BufReader<File>>),
}
#[pyclass(unsendable)]
struct BatchLogProcessor {
    reader: LogReader,
    target_level: Option<String>,
    chunk_size: usize,
}
#[pymethods]
impl BatchLogProcessor {
    #[new]
    #[pyo3(signature = (file_path, target_level = None, chunk_size=100_000))]
    fn new(file_path: &str, target_level: Option<String>, chunk_size: usize) -> PyResult<Self> {
        let file = File::open(file_path).map_err(|e| PyValueError::new_err(e.to_string()))?;


        let reader = if file_path.ends_with(".csv"){
            LogReader::Csv(csv::Reader::from_reader(file))
        }
        else if file_path.ends_with(".jsonl"){
            LogReader::Jsonl(BufReader::new(file).lines())
        }
        else{
            return Err(PyValueError::new_err("Unsupported file format. Only .csv and .jsonl are supported."));
        };

        Ok(BatchLogProcessor { reader, target_level, chunk_size })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Vec<LogEntry>>> {
        let chunk_size = slf.chunk_size;
        let mut current_chunk = Vec::with_capacity(slf.chunk_size);


        match &mut slf.reader {
            LogReader::Csv(reader) => {
                for result in reader.deserialize::<LogEntry>() {
                    match result {
                        Ok(log) => current_chunk.push(log),
                        Err(err) => eprintln!("Failed to parse CSV row: {:?}", err),
                    }
                }
            },
            LogReader::Jsonl(lines) => {
                for line in lines.by_ref().take(chunk_size) {
                    match line {
                        Ok(line_str) => match serde_json::from_str::<LogEntry>(&line_str) {
                            Ok(log) => current_chunk.push(log),
                            Err(err) => eprintln!("Failed to parse line: {}. Error: {:?}", line_str, err),
                        },
                        Err(err) => eprintln!("Failed to read line: {:?}", err),
                    }
                }
            },
        }


        // If the file is empty and we read nothing, stop the iterator
        if current_chunk.is_empty() {
            return Ok(None);
        }

        
        let final_chunk = match &slf.target_level {
            Some(level) => filter_by_level(current_chunk, level),
            None => current_chunk, // Pass the entire chunk straight through
        };

        Ok(Some(final_chunk))
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

// Function to parse JSONL file and return a vector of LogEntry structs
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

// function to filter log entries for targetet level parallel using rayon into_par_iter()
fn filter_by_level(logs: Vec<LogEntry>, target_level: &str) -> Vec<LogEntry> {
    logs.into_par_iter().filter(|log| log.level == target_level).collect()
}
#[cfg(test)]

mod tests{
    use super::*;

    #[test]
    fn test_parse_json_lines_valid() {
        let logs = parse_json_lines("test_logs.jsonl").expect("Failed to read JSONL file");

        let expected_levels = ["INFO", "ERROR", "WARN", "DEBUG", "INFO", "ERROR", "FATAL", "INFO", "WARN", "DEBUG"];
        let expected_user_ids = [101, 102, 102, 101, 101, 105, 0, 0, 0, 0];

        assert_eq!(logs.len(), 10, "Should have parsed exactly 10 lines");
        for (i, log) in logs.iter().enumerate() {
            assert_eq!(log.level, expected_levels[i], "JSONL line {} level mismatch", i + 1);
            assert_eq!(log.user_id, expected_user_ids[i], "JSONL line {} user_id mismatch", i + 1);
        }

        assert_eq!(logs[0].message, "User logged in");
        assert_eq!(logs[9].message, "Flushing old sessions");
    }

    #[test]
    fn test_parse_csv_valid() {
        let logs = parse_csv("test_logs.csv").expect("Failed to read CSV file");

        let expected_levels = ["INFO", "ERROR", "WARN", "DEBUG", "INFO", "ERROR", "FATAL", "INFO", "WARN", "DEBUG"];
        let expected_user_ids = [101, 102, 102, 101, 101, 105, 0, 0, 0, 0];

        assert_eq!(logs.len(), 10, "Should have parsed exactly 10 lines");
        for (i, log) in logs.iter().enumerate() {
            assert_eq!(log.level, expected_levels[i], "CSV line {} level mismatch", i + 1);
            assert_eq!(log.user_id, expected_user_ids[i], "CSV line {} user_id mismatch", i + 1);
        }

        assert_eq!(logs[0].timestamp, "2026-03-27T10:00:00Z");
        assert_eq!(logs[9].message, "Flushing old sessions");
    }

    #[test]
    fn test_parse_functions_roundtrip() {
        let csv_logs = parse_csv("test_logs.csv").expect("Failed to parse CSV");
        let json_logs = parse_json_lines("test_logs.jsonl").expect("Failed to parse JSONL");

        assert_eq!(csv_logs.len(), 10);
        assert_eq!(json_logs.len(), 10);
        assert_eq!(csv_logs[1].level, "ERROR");
        assert_eq!(json_logs[1].level, "ERROR");

        let errors = filter_by_level(csv_logs.clone(), "ERROR");
        assert_eq!(errors.len(), 2, "Should be exactly 2 error-level entries (ERROR only)");
    }
}