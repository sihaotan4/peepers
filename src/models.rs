use crate::file_utils;
use polars::{lazy::dsl::GetOutput, prelude::*};
use polars_sql::SQLContext;
use rand::prelude::*;
use std::error::Error;

#[derive(Clone)]
pub enum ModifierState {
    Original,
    Queried,
    RandomRows,
}

/// LazyFrame combining table metadata and a stateful view window
#[derive(Clone)]
pub struct PeepFrame {
    pub file_name: String,
    pub current_lazy_frame: LazyFrame,
    pub original_lazy_frame: LazyFrame,
    pub max_rows: usize,
    pub max_cols: usize,
    pub col_names: Vec<String>,
    pub row_slice_state: (usize, usize),
    pub col_slice_state: (usize, usize),
    pub modifier_state: ModifierState,
    pub display_rows: usize,
    display_cols: usize,
}
impl PeepFrame {
    /// Constructs a `PeepFrame` from a file.
    /// This function takes a file path and the number of rows and columns to display as arguments,
    /// and returns a `Result` containing a `PeepFrame` instance or an error.
    /// This function will return an error if the file cannot be read, or if the file type is not supported.
    pub fn from_file(
        file_path: &str,
        display_rows: usize,
        display_cols: usize,
    ) -> Result<PeepFrame, Box<dyn Error>> {
        let file_name = file_utils::extract_file_name(file_path)
            .ok_or("File should have a name")?
            .to_string();

        let lf = match file_utils::extract_file_type(file_path)? {
            file_utils::FileType::Parquet => {
                LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?
            }
            file_utils::FileType::Csv => LazyCsvReader::new(file_path).finish()?,
        };

        let max_rows = lf
            .clone()
            .select([count().alias("count")])
            .collect()
            .unwrap()
            .column("count")
            .unwrap()
            .u32()
            .unwrap()
            .get(0)
            .unwrap() as usize;

        let schema = lf.clone().schema()?;

        let col_names: Vec<String> = schema.get_names().iter().map(|s| s.to_string()).collect();

        let max_cols = col_names.len();

        Ok(PeepFrame {
            file_name,
            current_lazy_frame: lf.clone(),
            original_lazy_frame: lf,
            max_rows,
            max_cols,
            col_names,
            row_slice_state: (0, display_rows.min(max_rows)),
            col_slice_state: (0, display_cols.min(max_cols)),
            modifier_state: ModifierState::Original,
            display_rows,
            display_cols,
        })
    }

    /// Updates the `PeepFrame` with a new `LazyFrame`.
    /// This function takes a `LazyFrame` as an argument, calculates the total number of rows and columns,
    /// extracts the column names, and updates the `PeepFrame`'s fields accordingly.
    /// This function will return an error if the `LazyFrame`'s schema cannot be retrieved.
    /// Takes modifier state as param, forcing both lf and state to be changed together
    fn update_with(&mut self, lf: &LazyFrame, new_modifier_state: ModifierState) -> Result<(), Box<dyn Error>> {
        let max_rows = lf
            .clone()
            .select([count().alias("count")])
            .collect()?
            .column("count")?
            .u32()?
            .get(0)
            .ok_or("Unable to get row count")? as usize;

        let schema = lf.clone().schema()?;

        let col_names: Vec<String> = schema.get_names().iter().map(|s| s.to_string()).collect();

        let max_cols = col_names.len();

        self.current_lazy_frame = lf.clone();
        self.max_rows = max_rows;
        self.max_cols = max_cols;
        self.col_names = col_names;
        self.row_slice_state = (0, self.display_rows.min(max_rows));

        self.modifier_state = new_modifier_state;

        match self.modifier_state {
            ModifierState::Original | ModifierState::Queried => {
                // if transitioning to queried/original, reset the col view
                self.col_slice_state = (0, self.display_cols.min(max_cols));
            }
            ModifierState::RandomRows => {
                // if transitioning to random rows, keep the current col view
                let a = self.col_slice_state.0;
                self.col_slice_state = (a, (a + self.display_cols).min(max_cols));
            }
        }

        Ok(())
    }

    pub fn down(&mut self) {
        let (_, mut b) = self.row_slice_state;

        b = (b + self.display_rows).min(self.max_rows);

        let a = b.saturating_sub(self.display_rows);

        self.row_slice_state = (a, b);
    }

    pub fn up(&mut self) {
        let (mut a, _) = self.row_slice_state;

        a = a.saturating_sub(self.display_rows);

        let b = (a + self.display_rows).min(self.max_rows);

        self.row_slice_state = (a, b);
    }

    pub fn right(&mut self) {
        let (_, mut b) = self.col_slice_state;

        b = (b + self.display_cols).min(self.max_cols);

        let a = b.saturating_sub(self.display_cols);

        self.col_slice_state = (a, b);
    }

    pub fn left(&mut self) {
        let (mut a, _) = self.col_slice_state;

        a = a.saturating_sub(self.display_cols);

        let b = (a + self.display_cols).min(self.max_cols);

        self.col_slice_state = (a, b);
    }

    pub fn jump_to_tail(&mut self) {
        let b = self.max_rows;
        let a = b.saturating_sub(self.display_rows);
        self.row_slice_state = (a, b);
    }

    pub fn jump_to_head(&mut self) {
        self.row_slice_state = (0, self.display_rows.min(self.max_rows));
    }

    // each execution acts on the original lf, and updates the current lf
    // this prevents the current lf from having an increasingly complex plan
    // and higher likelihood of stack overflow
    pub fn execute_sql(&mut self, sql_query: &str) -> Result<(), Box<dyn Error>> {
        let mut context = SQLContext::new();
        context.register(&self.file_name, self.original_lazy_frame.clone());

        let new_lazy_frame = context.execute(sql_query)?;

        self.update_with(&new_lazy_frame, ModifierState::Queried)?;

        Ok(())
    }

    // each execution acts on the original lf, and updates the current lf
    // same considerations as execute_sql
    // but this one is not lazy? map materializes the whole frame (alternative is some complex row-wise sampling)
    pub fn shuffle_rows(&mut self) -> Result<(), Box<dyn Error>> {
        let first_col = self
            .col_names
            .first()
            .ok_or("PeepFrame should have column names")?;

        let sample_size = self.display_rows.clone();

        let new_lazy_frame = self
            .original_lazy_frame
            .clone()
            .with_column(
                col(first_col)
                    .map(
                        move |s| Ok(Some(sample_col(s, sample_size))),
                        GetOutput::default(),
                    )
                    .alias("_sample"),
            )
            .filter(col("_sample"))
            .select(&[col("*").exclude(["_sample"])]);

        // frame renders on every keypress and this lf will resample
        // this is a problem when you scroll through columns, the rows will change
        // solution is to "lock" this lazyframe by collecting first
        let new_lazy_frame = new_lazy_frame
            .slice(0_i64, self.display_rows as u32)
            .with_streaming(true)
            .collect()?
            .lazy();

        self.update_with(&new_lazy_frame, ModifierState::RandomRows)?;

        Ok(())
    }

    pub fn reset_to_original(&mut self) -> Result<(), Box<dyn Error>> {
        self.update_with(&self.original_lazy_frame.clone(), ModifierState::Original)?;

        Ok(())
    }
}

fn sample_col(s: Series, num_sample: usize) -> Series {
    let mut bool_vec: Vec<bool> = vec![true; num_sample];
    bool_vec.extend(vec![false; s.len() - num_sample]);

    let mut rng = rand::thread_rng();
    bool_vec.shuffle(&mut rng);

    Series::new("_sample", bool_vec)
}
