use std::error::Error;
use std::path::Path;

/// Supported file extensions that can be read as a polars lazyframe
#[derive(Clone)]
pub enum FileType {
    Parquet,
    Csv,
}

pub fn extract_file_name(file_path: &str) -> Option<&str> {
    Path::new(file_path)
        .file_stem()
        .and_then(|name| name.to_str())
}

pub fn extract_file_type(file_path: &str) -> Result<FileType, Box<dyn Error>> {
    let file_type = Path::new(file_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or("Unable to read file extension")?;

    match file_type {
        "parquet" => Ok(FileType::Parquet),
        "csv" => Ok(FileType::Csv),
        _ => Err("File type not supported (parquet and csv only)".into()),
    }
}
