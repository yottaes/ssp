use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Gauge, Paragraph, Row, Table, TableState,
};
use ratatui::{Frame, Terminal};

use crate::db::DuckDB;
use crate::pipeline::PipelineStats;

const PRESETS: &[(&str, &str, &str)] = &[
    (
        "F1",
        "top 10",
        "SELECT * FROM accounts ORDER BY lamports DESC LIMIT 10",
    ),
    (
        "F2",
        "top mints",
        "SELECT * FROM mints ORDER BY supply DESC LIMIT 10",
    ),
    (
        "F3",
        "top tokens",
        "SELECT * FROM token_accounts ORDER BY amount DESC LIMIT 10",
    ),
    (
        "F4",
        "count all",
        "SELECT 'accounts' as tbl, COUNT(*) as cnt FROM accounts UNION ALL SELECT 'mints', COUNT(*) FROM mints UNION ALL SELECT 'token_accounts', COUNT(*) FROM token_accounts",
    ),
    ("F5", "schema", "SELECT column_name, column_type FROM (DESCRIBE accounts)"),
];

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Processing,
    Query,
}

struct App {
    phase: Phase,
    stats: Arc<PipelineStats>,
    total_bytes: Option<u64>,
    start_time: Instant,
    should_quit: bool,

    // Query state
    db: Option<DuckDB>,
    input: String,
    cursor_pos: usize,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    table_state: TableState,
    error_msg: Option<String>,
    tables: Vec<(String, i64)>,
}

impl App {
    fn new(stats: Arc<PipelineStats>, total_bytes: Option<u64>) -> Self {
        Self {
            phase: Phase::Processing,
            stats,
            total_bytes,
            start_time: Instant::now(),
            should_quit: false,
            db: None,
            input: String::new(),
            cursor_pos: 0,
            columns: Vec::new(),
            rows: Vec::new(),
            table_state: TableState::default(),
            error_msg: None,
            tables: Vec::new(),
        }
    }

    fn transition_to_query(&mut self) {
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

    fn execute_query(&mut self, sql: &str) {
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

    fn handle_input_submit(&mut self) {
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
                    vec![
                        "top tokens [N]".into(),
                        "Top N token accounts by amount".into(),
                    ],
                    vec!["count [table]".into(), "Row count".into()],
                    vec!["schema <table>".into(), "Show columns".into()],
                    vec!["tables".into(), "List tables".into()],
                    vec!["F1-F5".into(), "Quick presets".into()],
                    vec!["Esc / Ctrl+C".into(), "Exit".into()],
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
                    .map(|(name, count)| vec![name.clone(), count.to_string()])
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

    // ── Rendering ──────────────────────────────────────────────

    fn render_processing(&self, frame: &mut Frame) {
        let area = frame.area();

        let outer = Block::default()
            .title(" SSP — Processing Snapshot ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Length(3), // gauge
            Constraint::Length(1), // spacer
            Constraint::Length(4), // stats
            Constraint::Length(1), // spacer
            Constraint::Min(5),   // pipeline health
            Constraint::Length(1), // footer
        ])
        .split(inner);

        // Progress gauge
        let bytes = self.stats.bytes_read.load(Ordering::Relaxed);
        let total = self.total_bytes.unwrap_or(0);
        let ratio = if total > 0 {
            (bytes as f64 / total as f64).min(1.0)
        } else {
            0.0
        };

        let label = if total > 0 {
            format!(
                "{:.2} / {:.2} GB  ({:.0}%)",
                bytes as f64 / 1e9,
                total as f64 / 1e9,
                ratio * 100.0
            )
        } else {
            format!("{:.2} GB", bytes as f64 / 1e9)
        };

        let gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Progress "),
            )
            .gauge_style(Style::default().fg(Color::Cyan).bg(Color::DarkGray))
            .ratio(ratio)
            .label(label);
        frame.render_widget(gauge, chunks[0]);

        // Stats
        let elapsed = self.start_time.elapsed();
        let secs = elapsed.as_secs_f64();
        let speed = if secs > 0.5 {
            bytes as f64 / secs / 1_048_576.0
        } else {
            0.0
        };
        let rows = self.stats.rows_parsed.load(Ordering::Relaxed);
        let eta = if ratio > 0.01 && ratio < 1.0 {
            let remaining = secs / ratio * (1.0 - ratio);
            format!("{}m {:02}s", remaining as u64 / 60, remaining as u64 % 60)
        } else {
            "—".into()
        };

        let stats = Paragraph::new(vec![
            Line::from(vec![
                Span::styled("  Speed     ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{speed:.1} MB/s"),
                    Style::default().fg(Color::Green),
                ),
            ]),
            Line::from(vec![
                Span::styled("  Rows      ", Style::default().fg(Color::DarkGray)),
                Span::raw(format_rows(rows)),
            ]),
            Line::from(vec![
                Span::styled("  Elapsed   ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!(
                    "{}m {:02}s",
                    elapsed.as_secs() / 60,
                    elapsed.as_secs() % 60
                )),
            ]),
            Line::from(vec![
                Span::styled("  ETA       ", Style::default().fg(Color::DarkGray)),
                Span::raw(eta),
            ]),
        ]);
        frame.render_widget(stats, chunks[2]);

        // Pipeline health
        let health = Paragraph::new(vec![
            Line::from(Span::styled(
                "  Pipeline Health",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "  ├ parsers blocked (tx)       {}",
                self.stats.parser_blocked_tx.load(Ordering::Relaxed)
            )),
            Line::from(format!(
                "  ├ parsers blocked (decoded)  {}",
                self.stats.parser_blocked_decoded.load(Ordering::Relaxed)
            )),
            Line::from(format!(
                "  ├ writers starved (acct)     {}",
                self.stats.writer_starved_acct.load(Ordering::Relaxed)
            )),
            Line::from(format!(
                "  └ writers starved (decoded)  {}",
                self.stats.writer_starved_decoded.load(Ordering::Relaxed)
            )),
        ]);
        frame.render_widget(health, chunks[4]);

        // Footer
        frame.render_widget(
            Paragraph::new(" q quit ")
                .alignment(Alignment::Right)
                .style(Style::default().fg(Color::DarkGray)),
            chunks[5],
        );
    }

    fn render_query(&mut self, frame: &mut Frame) {
        let area = frame.area();

        let outer = Block::default()
            .title(" SSP — Query Mode ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Length(1), // presets
            Constraint::Min(5),   // results table
            Constraint::Length(1), // input
            Constraint::Length(1), // status bar
        ])
        .split(inner);

        // Presets bar
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

        // Results table / error / empty state
        if let Some(err) = &self.error_msg {
            frame.render_widget(
                Paragraph::new(format!("  Error: {err}")).style(Style::default().fg(Color::Red)),
                chunks[1],
            );
        } else if !self.columns.is_empty() {
            let available = chunks[1].width.saturating_sub(4); // borders + highlight symbol
            let (col_widths, truncated) =
                prepare_table(&self.columns, &self.rows, available as usize);

            let header = Row::new(self.columns.iter().enumerate().map(|(i, n)| {
                let max = col_widths.get(i).copied().unwrap_or(10);
                Cell::from(truncate(n, max))
                    .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
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

        // Input bar
        let input_line = Line::from(vec![
            Span::styled("ssp> ", Style::default().fg(Color::Cyan)),
            Span::raw(&self.input),
        ]);
        frame.render_widget(Paragraph::new(input_line), chunks[2]);

        // Cursor
        let cursor_x =
            (chunks[2].x + 5 + self.cursor_pos as u16).min(chunks[2].right().saturating_sub(1));
        frame.set_cursor_position((cursor_x, chunks[2].y));

        // Status bar
        let tables_str = self
            .tables
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let status = format!(
            " {} rows │ {} │ Esc quit │ ? help",
            self.rows.len(),
            tables_str
        );
        frame.render_widget(
            Paragraph::new(status).style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }

    // ── Event loop ─────────────────────────────────────────────

    fn event_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
        pipeline_handle: JoinHandle<anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        let mut pipeline_handle = Some(pipeline_handle);

        loop {
            terminal.draw(|frame| match self.phase {
                Phase::Processing => self.render_processing(frame),
                Phase::Query => self.render_query(frame),
            })?;

            if self.should_quit {
                return Ok(());
            }

            // Check if pipeline finished
            if self.phase == Phase::Processing {
                if let Some(ref handle) = pipeline_handle {
                    if handle.is_finished() {
                        match pipeline_handle.take().unwrap().join() {
                            Ok(Ok(())) => self.transition_to_query(),
                            Ok(Err(e)) => {
                                self.phase = Phase::Query;
                                self.error_msg = Some(format!("Pipeline error: {e}"));
                            }
                            Err(_) => {
                                self.phase = Phase::Query;
                                self.error_msg = Some("Pipeline panicked".into());
                            }
                        }
                        continue;
                    }
                }
            }

            // Handle input events (poll timeout = refresh rate)
            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }

                    match self.phase {
                        Phase::Processing => {
                            if key.code == KeyCode::Char('q')
                                || (key.code == KeyCode::Char('c')
                                    && key.modifiers.contains(KeyModifiers::CONTROL))
                            {
                                return Ok(());
                            }
                        }
                        Phase::Query => match key.code {
                            KeyCode::Esc => return Ok(()),
                            KeyCode::Char('c')
                                if key.modifiers.contains(KeyModifiers::CONTROL) =>
                            {
                                return Ok(())
                            }
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
                            KeyCode::Left => {
                                self.cursor_pos = self.cursor_pos.saturating_sub(1);
                            }
                            KeyCode::Right => {
                                self.cursor_pos = (self.cursor_pos + 1).min(self.input.len());
                            }
                            KeyCode::Up => {
                                let sel = self.table_state.selected().unwrap_or(0);
                                self.table_state.select(Some(sel.saturating_sub(1)));
                            }
                            KeyCode::Down => {
                                let sel = self.table_state.selected().unwrap_or(0);
                                let max = self.rows.len().saturating_sub(1);
                                self.table_state.select(Some((sel + 1).min(max)));
                            }
                            KeyCode::PageUp => {
                                let sel = self.table_state.selected().unwrap_or(0);
                                self.table_state.select(Some(sel.saturating_sub(20)));
                            }
                            KeyCode::PageDown => {
                                let sel = self.table_state.selected().unwrap_or(0);
                                let max = self.rows.len().saturating_sub(1);
                                self.table_state.select(Some((sel + 20).min(max)));
                            }
                            KeyCode::F(n) if (1..=5).contains(&n) => {
                                let idx = (n - 1) as usize;
                                if idx < PRESETS.len() {
                                    self.execute_query(PRESETS[idx].2);
                                }
                            }
                            _ => {}
                        },
                    }
                }
            }
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────

fn resolve_quick_command(lower: &str) -> Option<String> {
    let parts: Vec<&str> = lower.split_whitespace().collect();
    match parts.as_slice() {
        ["top"] => Some("SELECT * FROM accounts ORDER BY lamports DESC LIMIT 10".into()),
        ["top", n] if n.parse::<u32>().is_ok() => Some(format!(
            "SELECT * FROM accounts ORDER BY lamports DESC LIMIT {n}"
        )),
        ["top", "mints"] => {
            Some("SELECT * FROM mints ORDER BY supply DESC LIMIT 10".into())
        }
        ["top", "mints", n] if n.parse::<u32>().is_ok() => Some(format!(
            "SELECT * FROM mints ORDER BY supply DESC LIMIT {n}"
        )),
        ["top", "tokens"] => Some(
            "SELECT * FROM token_accounts ORDER BY amount DESC LIMIT 10".into(),
        ),
        ["top", "tokens", n] if n.parse::<u32>().is_ok() => Some(format!(
            "SELECT * FROM token_accounts ORDER BY amount DESC LIMIT {n}"
        )),
        ["count"] => Some("SELECT COUNT(*) AS count FROM accounts".into()),
        ["count", table] => Some(format!("SELECT COUNT(*) AS count FROM {table}")),
        ["schema", table] => Some(format!("SELECT column_name, column_type FROM (DESCRIBE {table})")),
        _ => None,
    }
}

/// Compute per-column widths that fit `available` chars, then truncate cell values.
fn prepare_table(
    columns: &[String],
    rows: &[Vec<String>],
    available: usize,
) -> (Vec<usize>, Vec<Vec<String>>) {
    let ncols = columns.len().max(1);
    // Separator overhead: ~3 chars per column gap
    let usable = available.saturating_sub(ncols.saturating_sub(1) * 3);
    let max_col = (usable / ncols).max(6);

    // Natural widths (capped per column)
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len().min(max_col)).collect();
    for row in rows.iter().take(50) {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len()).min(max_col);
            }
        }
    }

    // Truncate cell values
    let truncated: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, cell)| {
                    let max = widths.get(i).copied().unwrap_or(max_col);
                    truncate(cell, max)
                })
                .collect()
        })
        .collect();

    (widths, truncated)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else if max <= 2 {
        s[..max].to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}

fn format_rows(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{n}")
    }
}

// ── Public entry point ─────────────────────────────────────────

pub fn run(
    stats: Arc<PipelineStats>,
    total_bytes: Option<u64>,
    pipeline_handle: JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(stats, total_bytes);
    let result = app.event_loop(&mut terminal, pipeline_handle);

    // Always restore terminal
    terminal::disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}
