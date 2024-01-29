use crate::models::{ModifierState, PeepFrame};
use polars::prelude::*;
use std::error::Error;

pub fn render(peep_frame: &PeepFrame) -> Result<(), Box<dyn Error>> {
    let header = render_header(peep_frame);

    let view = render_view_data(peep_frame);

    let table = render_table(peep_frame)?;

    let guide = render_controls_guide();

    let output = format!("{}\n{}\n{}\n{}", header, view, table, guide);

    println!("{}", output);

    Ok(())
}

fn render_header(peep_frame: &PeepFrame) -> String {
    match peep_frame.modifier_state {
        ModifierState::Original => {
            format!("Table name: '{}'", peep_frame.file_name)
        }
        ModifierState::Queried => {
            format!("SQL result: '{}'", peep_frame.file_name)
        }
        ModifierState::RandomRows => {
            format!("Sampling rows: '{}'", peep_frame.file_name)
        }
    }
}

fn render_table(peep_frame: &PeepFrame) -> Result<String, PolarsError> {
    let (start, end) = peep_frame.col_slice_state;
    let selected_columns: Vec<String> = peep_frame.col_names[start..end].into_vec();

    let df = peep_frame
        .current_lazy_frame
        .clone()
        .slice(
            peep_frame.row_slice_state.0 as i64,
            peep_frame.display_rows as u32,
        )
        .select([cols(selected_columns)])
        .with_streaming(true)
        .collect()?;

    Ok(df.to_string())
}

fn render_view_data(peep_frame: &PeepFrame) -> String {
    // For RandomRows, make it explicit for the user
    let row_data = match peep_frame.modifier_state {
        ModifierState::Original | ModifierState::Queried => {
            format!(
                "(Rows: {} to {}) out of {} | ",
                peep_frame.row_slice_state.0, peep_frame.row_slice_state.1, peep_frame.max_rows
            )
        }
        ModifierState::RandomRows => "".to_string(),
    };

    let (start, end) = peep_frame.col_slice_state;
    let col_range = std::ops::Range { start, end };

    let mut col_data: String = String::new();

    if peep_frame.max_cols < 20 {
        for i in 0..peep_frame.max_cols {
            if col_range.contains(&i) {
                col_data += "■ ";
            } else {
                col_data += "□ ";
            }
        }
    } else {
        col_data = format!(
            "({} to {}) out of {}",
            peep_frame.col_slice_state.0, peep_frame.col_slice_state.1, peep_frame.max_cols
        )
    }

    format!("{row_data}Columns shown: {col_data}")
}

fn render_controls_guide() -> String {
    "Arrow keys | [s]ql | [r]andom | [t]ail | [h]ead | [o]riginal | [q]uit".to_string()
}
