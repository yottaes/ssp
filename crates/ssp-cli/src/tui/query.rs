use crossterm::event::KeyCode;
use ratatui::layout::Constraint;
use ratatui::layout::Layout;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Paragraph, Row, Table, TableState,
};
use ratatui::Frame;

use crate::db::DuckDB;

use super::helpers::{prepare_table, resolve_quick_command, truncate};
use super::{App, Phase, PRESETS};

impl App {
    pub(super) fn transition_to_query(&mut self) {
        self.phase = Phase::Query;
        match DuckDB::open() {
            Ok(mut db) => {
                self.tables = db.register_views_tui().unwrap_or_default();
                self.db = Some(db);
            }
            Err(e) => {
                self.error_msg = Some(format!("Failed to open DuckDB: {e}"));
            }
        }
    }

    pub(super) fn execute_query(&mut self, sql: &str) {
        self.error_msg = None;
        self.table_state = TableState::default();

        if let Some(db) = &self.db {
            match db.execute_to_vecs(sql) {
                Ok((cols, data)) => {
                    self.columns = cols;
                    self.rows = data;
                    if !self.rows.is_empty() {
                        self.table_state.select(Some(0));
                    }
                }
                Err(e) => {
                    self.columns.clear();
                    self.rows.clear();
                    self.error_msg = Some(format!("{e}"));
                }
            }
        }
    }

    pub(super) fn handle_input_submit(&mut self) {
        let input = self.input.trim().to_string();
        if input.is_empty() {
            return;
        }

        let lower = input.to_lowercase();
        match lower.as_str() {
            "quit" | "exit" | "q" => {
                self.should_quit = true;
                return;
            }
            "help" | "?" => {
                self.columns = vec!["Command".into(), "Description".into()];
                self.rows = vec![
                    vec!["top [N]".into(), "Top N accounts by lamports".into()],
                    vec!["top mints [N]".into(), "Top N mints by supply".into()],
                    vec!["top tokens [N]".into(), "Top N token accounts".into()],
                    vec!["count [table]".into(), "Row count".into()],
                    vec!["schema <table>".into(), "Show columns".into()],
                    vec!["tables".into(), "List tables".into()],
                    vec!["F1-F5".into(), "Quick presets".into()],
                    vec!["Esc".into(), "Exit".into()],
                ];
                self.error_msg = None;
                self.table_state = TableState::default();
                self.table_state.select(Some(0));
                self.input.clear();
                self.cursor_pos = 0;
                return;
            }
            "tables" => {
                self.columns = vec!["Table".into(), "Rows".into()];
                self.rows = self
                    .tables
                    .iter()
                    .map(|(n, c)| vec![n.clone(), c.to_string()])
                    .collect();
                self.error_msg = None;
                self.table_state = TableState::default();
                if !self.rows.is_empty() {
                    self.table_state.select(Some(0));
                }
                self.input.clear();
                self.cursor_pos = 0;
                return;
            }
            _ => {}
        }

        let sql = resolve_quick_command(&lower).unwrap_or(input);
        self.execute_query(&sql);
        self.input.clear();
        self.cursor_pos = 0;
    }

    pub(super) fn render_query(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Query Mode ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(inner);

        // Presets
        let presets: Vec<Span> = PRESETS
            .iter()
            .flat_map(|(key, label, _)| {
                vec![
                    Span::styled(
                        format!(" {key} "),
                        Style::default().bg(Color::DarkGray).fg(Color::White),
                    ),
                    Span::raw(format!(" {label} ")),
                ]
            })
            .collect();
        frame.render_widget(Paragraph::new(Line::from(presets)), chunks[0]);

        // Results
        if let Some(err) = &self.error_msg {
            frame.render_widget(
                Paragraph::new(format!("  Error: {err}")).style(Style::default().fg(Color::Red)),
                chunks[1],
            );
        } else if !self.columns.is_empty() {
            let available = chunks[1].width.saturating_sub(4);
            let (col_widths, truncated) =
                prepare_table(&self.columns, &self.rows, available as usize);

            let header = Row::new(self.columns.iter().enumerate().map(|(i, n)| {
                let max = col_widths.get(i).copied().unwrap_or(10);
                Cell::from(truncate(n, max)).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
            }))
            .bottom_margin(1);

            let table_rows: Vec<Row> = truncated
                .iter()
                .map(|row| Row::new(row.iter().map(|c| Cell::from(c.as_str()))))
                .collect();

            let constraints: Vec<Constraint> = col_widths
                .iter()
                .map(|&w| Constraint::Length(w as u16))
                .collect();

            let table = Table::new(table_rows, constraints)
                .header(header)
                .row_highlight_style(Style::default().bg(Color::DarkGray))
                .highlight_symbol("▸ ");

            frame.render_stateful_widget(table, chunks[1], &mut self.table_state);
        } else {
            frame.render_widget(
                Paragraph::new("  Type a query or press F1-F5 for presets")
                    .style(Style::default().fg(Color::DarkGray)),
                chunks[1],
            );
        }

        // Input
        let input_line = Line::from(vec![
            Span::styled("ssp> ", Style::default().fg(Color::Cyan)),
            Span::raw(&self.input),
        ]);
        frame.render_widget(Paragraph::new(input_line), chunks[2]);

        let cursor_x =
            (chunks[2].x + 5 + self.cursor_pos as u16).min(chunks[2].right().saturating_sub(1));
        frame.set_cursor_position((cursor_x, chunks[2].y));

        // Status
        let tables_str = self
            .tables
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        frame.render_widget(
            Paragraph::new(format!(
                " {} rows │ {} │ Esc quit │ ? help",
                self.rows.len(),
                tables_str
            ))
            .style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }

    pub(super) fn key_query(&mut self, key: crossterm::event::KeyEvent) {
        match key.code {
            KeyCode::Esc => self.should_quit = true,
            KeyCode::Enter => self.handle_input_submit(),
            KeyCode::Char(c) => {
                self.input.insert(self.cursor_pos, c);
                self.cursor_pos += 1;
            }
            KeyCode::Backspace => {
                if self.cursor_pos > 0 {
                    self.cursor_pos -= 1;
                    self.input.remove(self.cursor_pos);
                }
            }
            KeyCode::Left => self.cursor_pos = self.cursor_pos.saturating_sub(1),
            KeyCode::Right => self.cursor_pos = (self.cursor_pos + 1).min(self.input.len()),
            KeyCode::Up => {
                let s = self.table_state.selected().unwrap_or(0);
                self.table_state.select(Some(s.saturating_sub(1)));
            }
            KeyCode::Down => {
                let s = self.table_state.selected().unwrap_or(0);
                let max = self.rows.len().saturating_sub(1);
                self.table_state.select(Some((s + 1).min(max)));
            }
            KeyCode::PageUp => {
                let s = self.table_state.selected().unwrap_or(0);
                self.table_state.select(Some(s.saturating_sub(20)));
            }
            KeyCode::PageDown => {
                let s = self.table_state.selected().unwrap_or(0);
                let max = self.rows.len().saturating_sub(1);
                self.table_state.select(Some((s + 20).min(max)));
            }
            KeyCode::F(n) if (1..=5).contains(&n) => {
                let idx = (n - 1) as usize;
                if idx < PRESETS.len() {
                    self.execute_query(PRESETS[idx].2);
                }
            }
            _ => {}
        }
    }
}
