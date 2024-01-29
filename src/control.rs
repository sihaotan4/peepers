use crate::models::PeepFrame;
use crate::render;
use crossterm::event::{read, Event, KeyCode};
use crossterm::terminal::{self, ClearType};
use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use polars::error::PolarsError;

use std::error::Error;
use std::io::stdout;
use std::io::{self};

pub fn event_loop(
    filepath: &str,
    display_rows: usize,
    display_cols: usize,
) -> Result<(), Box<dyn Error>> {
    let mut peep_frame = PeepFrame::from_file(filepath, display_rows, display_cols)?;

    // initial render
    execute!(stdout(), EnterAlternateScreen)?;
    render::render(&peep_frame).expect("Unable to render");

    loop {
        // Blocking read
        let event = read()?;

        if event == Event::Key(KeyCode::Down.into()) {
            peep_frame.down();
        }
        if event == Event::Key(KeyCode::Up.into()) {
            peep_frame.up();
        }
        if event == Event::Key(KeyCode::Left.into()) {
            peep_frame.left();
        }
        if event == Event::Key(KeyCode::Right.into()) {
            peep_frame.right();
        }
        if event == Event::Key(KeyCode::Char('t').into()) {
            peep_frame.jump_to_tail();
        }
        if event == Event::Key(KeyCode::Char('h').into()) {
            peep_frame.jump_to_head();
        }
        if event == Event::Key(KeyCode::Char('r').into()) {
            peep_frame.shuffle_rows()?;
        }
        if event == Event::Key(KeyCode::Char('s').into()) {
            println!("SQL (enter 'q' to exit this mode):");
            loop {
                let mut input = String::new();

                io::stdin().read_line(&mut input).unwrap();

                let sql_query = input.trim();

                if sql_query == 'q'.to_string() {
                    break;
                }

                match peep_frame.execute_sql(sql_query) {
                    Ok(_) => break,
                    Err(err) => {
                        // for polars errors, use a more concise print
                        match err.is::<PolarsError>() {
                            true => {println!("{}", err)}
                            false => {println!("{:?}", err);}
                        }
                    }
                }
            }
        }
        if event == Event::Key(KeyCode::Char('o').into()) {
            peep_frame.reset_to_original()?;
        }
        if event == Event::Key(KeyCode::Char('q').into()) {
            execute!(stdout(), LeaveAlternateScreen)?;
            break;
        }

        // re-render
        clear_screen()?;
        render::render(&peep_frame)?;
    }

    Ok(())
}

fn clear_screen() -> Result<(), Box<dyn Error>> {
    let res = execute!(stdout(), terminal::Clear(ClearType::All))?;
    Ok(res)
}
