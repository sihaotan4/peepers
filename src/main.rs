mod control;
mod file_utils;
mod models;
mod render;

use clap::{arg, command, value_parser};
use control::event_loop;
use std::env;

fn main() {
    let matches = command!()
        .arg(arg!([filepath] "Required filepath to operate on").required(true))
        .arg(
            arg!([row_count])
                .short('r')
                .value_parser(value_parser!(usize))
                .default_value("7"),
        )
        .arg(
            arg!([col_count])
                .short('c')
                .value_parser(value_parser!(usize))
                .default_value("5"),
        )
        .get_matches();

    let filepath = matches
        .get_one::<String>("filepath")
        .expect("Filepath should be provided");

    let display_rows = matches.get_one::<usize>("row_count").unwrap().to_owned();

    let display_cols = matches.get_one::<usize>("col_count").unwrap().to_owned();

    configure_polars_formatting();

    event_loop(&filepath, display_rows, display_cols).expect("Cannot handle event");
}

fn configure_polars_formatting() {
    env::set_var("POLARS_FMT_TABLE_FORMATTING", "UTF8_FULL");
    env::set_var("POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION", "1");
    env::set_var("POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE", "1");
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1");
    env::set_var("POLARS_FMT_STR_LEN", "50");
    env::set_var("POLARS_FMT_MAX_ROWS", "-1"); // show all
    env::set_var("POLARS_TABLE_WIDTH", "9999");
}
