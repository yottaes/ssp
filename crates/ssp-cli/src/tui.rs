mod helpers;
mod processing;
mod query;
mod setup;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::widgets::TableState;
use ratatui::{Frame, Terminal};

use crate::db::DuckDB;
use crate::pipeline::PipelineStats;
use ssp_core::filters::ResolvedFilters;

// ── Constants ──────────────────────────────────────────────────

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
    (
        "F5",
        "schema",
        "SELECT column_name, column_type FROM (DESCRIBE accounts)",
    ),
];

const SPINNER: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

// ── Types ──────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Setup,
    Discovering,
    Processing,
    Query,
}

#[derive(Clone, Copy, PartialEq)]
enum SetupScreen {
    Source,
    FilePicker,
    NetworkType,
    Filters,
}

struct FileEntry {
    name: String,
    path: PathBuf,
    is_dir: bool,
    size: u64,
}

struct DiscoverResult {
    reader: Box<dyn io::Read + Send>,
    total_size: Option<u64>,
}

// ── App ────────────────────────────────────────────────────────

struct App {
    phase: Phase,
    should_quit: bool,

    // Setup
    setup_screen: SetupScreen,
    source_choice: usize,
    net_choice: usize,
    include_dead: bool,
    include_spam: bool,
    filter_focus: usize,
    selected_path: Option<PathBuf>,

    // File picker
    cwd: PathBuf,
    dir_entries: Vec<FileEntry>,
    file_selected: usize,

    // Discover
    discover_handle: Option<JoinHandle<anyhow::Result<DiscoverResult>>>,
    discover_start: Instant,

    // Processing
    stats: Option<Arc<PipelineStats>>,
    total_bytes: Option<u64>,
    start_time: Instant,
    pipeline_handle: Option<JoinHandle<anyhow::Result<()>>>,

    // Query
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
    fn new_interactive() -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let mut app = Self {
            phase: Phase::Setup,
            should_quit: false,
            setup_screen: SetupScreen::Source,
            source_choice: 0,
            net_choice: 0,
            include_dead: false,
            include_spam: false,
            filter_focus: 0,
            selected_path: None,
            cwd: cwd.clone(),
            dir_entries: Vec::new(),
            file_selected: 0,
            discover_handle: None,
            discover_start: Instant::now(),
            stats: None,
            total_bytes: None,
            start_time: Instant::now(),
            pipeline_handle: None,
            db: None,
            input: String::new(),
            cursor_pos: 0,
            columns: Vec::new(),
            rows: Vec::new(),
            table_state: TableState::default(),
            error_msg: None,
            tables: Vec::new(),
        };
        app.refresh_dir_entries();
        app
    }

    fn start_pipeline_with_reader(
        &mut self,
        reader: Box<dyn io::Read + Send + 'static>,
        total_size: Option<u64>,
    ) {
        let filters = ResolvedFilters {
            owner: None,
            hash: None,
            pubkey: None,
            include_dead: self.include_dead,
            include_spam: self.include_spam,
        };

        let stats = Arc::new(PipelineStats::new());
        self.stats = Some(stats.clone());
        self.total_bytes = total_size;
        self.start_time = Instant::now();
        self.phase = Phase::Processing;

        self.pipeline_handle = Some(std::thread::spawn(move || {
            crate::pipeline::run(reader, filters, stats)
        }));
    }

    fn start_local(&mut self) {
        if let Some(path) = self.selected_path.clone() {
            match std::fs::File::open(&path) {
                Ok(file) => {
                    let size = file.metadata().ok().map(|m| m.len());
                    self.start_pipeline_with_reader(Box::new(file), size);
                }
                Err(e) => {
                    self.phase = Phase::Query;
                    self.error_msg = Some(format!("Failed to open file: {e}"));
                }
            }
        }
    }

    fn start_discover(&mut self) {
        let incremental = self.net_choice == 1;
        self.phase = Phase::Discovering;
        self.discover_start = Instant::now();

        self.discover_handle = Some(std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new()?;
            let source = rt.block_on(crate::rpc::find_fastest_snapshot(None, incremental))?;
            let resp = reqwest::blocking::Client::builder()
                .timeout(None)
                .build()?
                .get(&source.url)
                .send()?;
            Ok(DiscoverResult {
                reader: Box::new(resp),
                total_size: source.size,
            })
        }));
    }

    // ── Main loop ──────────────────────────────────────────────

    fn run_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> anyhow::Result<()> {
        loop {
            terminal.draw(|frame| self.render(frame))?;

            if self.should_quit {
                return Ok(());
            }

            self.tick()?;

            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }
                    if key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        return Ok(());
                    }
                    self.handle_key(key);
                }
            }
        }
    }

    fn tick(&mut self) -> anyhow::Result<()> {
        match self.phase {
            Phase::Discovering => {
                if let Some(ref handle) = self.discover_handle {
                    if handle.is_finished() {
                        match self.discover_handle.take().unwrap().join() {
                            Ok(Ok(result)) => {
                                self.start_pipeline_with_reader(result.reader, result.total_size);
                            }
                            Ok(Err(e)) => {
                                self.phase = Phase::Query;
                                self.error_msg = Some(format!("Discover failed: {e}"));
                            }
                            Err(_) => {
                                self.phase = Phase::Query;
                                self.error_msg = Some("Discover panicked".into());
                            }
                        }
                    }
                }
            }
            Phase::Processing => {
                if let Some(ref handle) = self.pipeline_handle {
                    if handle.is_finished() {
                        match self.pipeline_handle.take().unwrap().join() {
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
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    // ── Dispatch ────────────────────────────────────────────────

    fn render(&mut self, frame: &mut Frame) {
        match self.phase {
            Phase::Setup => match self.setup_screen {
                SetupScreen::Source => self.render_source(frame),
                SetupScreen::FilePicker => self.render_file_picker(frame),
                SetupScreen::NetworkType => self.render_network(frame),
                SetupScreen::Filters => self.render_filters(frame),
            },
            Phase::Discovering => self.render_discovering(frame),
            Phase::Processing => self.render_processing(frame),
            Phase::Query => self.render_query(frame),
        }
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) {
        match self.phase {
            Phase::Setup => match self.setup_screen {
                SetupScreen::Source => self.key_source(key),
                SetupScreen::FilePicker => self.key_file_picker(key),
                SetupScreen::NetworkType => self.key_network(key),
                SetupScreen::Filters => self.key_filters(key),
            },
            Phase::Discovering => {
                if key.code == KeyCode::Esc {
                    self.should_quit = true;
                }
            }
            Phase::Processing => {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                    self.should_quit = true;
                }
            }
            Phase::Query => self.key_query(key),
        }
    }
}

// ── Public entry point ─────────────────────────────────────────

pub fn run_interactive() -> anyhow::Result<()> {
    let mut app = App::new_interactive();

    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = app.run_loop(&mut terminal);

    terminal::disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}
