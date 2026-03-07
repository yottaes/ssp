use crossterm::event::KeyCode;
use ratatui::layout::{Alignment, Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use ratatui::Frame;

use super::helpers::{format_size, read_dir_entries};
use super::{App, SetupScreen};

impl App {
    pub(super) fn refresh_dir_entries(&mut self) {
        self.dir_entries = read_dir_entries(&self.cwd);
        self.file_selected = 0;
    }

    // ── Source selection ─────────────────────────────────────────

    pub(super) fn render_source(&self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Solana Snapshot Parser ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Min(3),
            Constraint::Length(5),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(inner);

        let options = ["Local file", "Network (auto-discover)"];
        let lines: Vec<Line> = options
            .iter()
            .enumerate()
            .map(|(i, label)| {
                let marker = if i == self.source_choice {
                    "  ▸ "
                } else {
                    "    "
                };
                let style = if i == self.source_choice {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };
                Line::from(Span::styled(format!("{marker}{label}"), style))
            })
            .collect();

        let select = Paragraph::new(lines).alignment(Alignment::Left);
        frame.render_widget(
            Paragraph::new("  Select snapshot source:")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[0],
        );
        frame.render_widget(select, chunks[1]);

        frame.render_widget(
            Paragraph::new(" ↑↓ navigate │ Enter select │ Esc quit")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }

    pub(super) fn key_source(&mut self, key: crossterm::event::KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.source_choice = self.source_choice.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.source_choice = (self.source_choice + 1).min(1);
            }
            KeyCode::Enter => {
                if self.source_choice == 0 {
                    self.setup_screen = SetupScreen::FilePicker;
                    self.refresh_dir_entries();
                } else {
                    self.setup_screen = SetupScreen::NetworkType;
                }
            }
            KeyCode::Esc => self.should_quit = true,
            _ => {}
        }
    }

    // ── File picker ─────────────────────────────────────────────

    pub(super) fn render_file_picker(&self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Select File ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(inner);

        let cwd_str = format!(" {}", self.cwd.display());
        frame.render_widget(
            Paragraph::new(cwd_str).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            chunks[0],
        );

        let visible_height = chunks[1].height as usize;
        let scroll = if self.file_selected >= visible_height {
            self.file_selected - visible_height + 1
        } else {
            0
        };

        let lines: Vec<Line> = self
            .dir_entries
            .iter()
            .enumerate()
            .skip(scroll)
            .take(visible_height)
            .map(|(i, entry)| {
                let marker = if i == self.file_selected {
                    " ▸ "
                } else {
                    "   "
                };
                let (icon, size_str) = if entry.is_dir {
                    ("📁 ", String::new())
                } else {
                    ("   ", format_size(entry.size))
                };
                let style = if i == self.file_selected {
                    if entry.is_dir {
                        Style::default().fg(Color::Cyan)
                    } else {
                        Style::default()
                            .fg(Color::Green)
                            .add_modifier(Modifier::BOLD)
                    }
                } else if entry.is_dir {
                    Style::default().fg(Color::Blue)
                } else {
                    Style::default().fg(Color::White)
                };

                Line::from(vec![
                    Span::raw(marker),
                    Span::raw(icon),
                    Span::styled(&entry.name, style),
                    Span::styled(
                        format!("  {size_str}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                ])
            })
            .collect();

        frame.render_widget(Paragraph::new(lines), chunks[1]);

        frame.render_widget(
            Paragraph::new(" ↑↓ navigate │ Enter select │ Backspace up │ Esc back")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[2],
        );
    }

    pub(super) fn key_file_picker(&mut self, key: crossterm::event::KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.file_selected = self.file_selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let max = self.dir_entries.len().saturating_sub(1);
                self.file_selected = (self.file_selected + 1).min(max);
            }
            KeyCode::Enter => {
                if let Some(entry) = self.dir_entries.get(self.file_selected) {
                    if entry.is_dir {
                        self.cwd = entry.path.clone();
                        self.refresh_dir_entries();
                    } else {
                        self.selected_path = Some(entry.path.clone());
                        self.setup_screen = SetupScreen::Filters;
                    }
                }
            }
            KeyCode::Backspace => {
                if let Some(parent) = self.cwd.parent().map(|p| p.to_path_buf()) {
                    self.cwd = parent;
                    self.refresh_dir_entries();
                }
            }
            KeyCode::Esc => {
                self.setup_screen = SetupScreen::Source;
            }
            _ => {}
        }
    }

    // ── Network type ────────────────────────────────────────────

    pub(super) fn render_network(&self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Network Source ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Min(3),
            Constraint::Length(5),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(inner);

        let options = ["Full snapshot", "Incremental snapshot"];
        let lines: Vec<Line> = options
            .iter()
            .enumerate()
            .map(|(i, label)| {
                let marker = if i == self.net_choice {
                    "  ▸ "
                } else {
                    "    "
                };
                let style = if i == self.net_choice {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };
                Line::from(Span::styled(format!("{marker}{label}"), style))
            })
            .collect();

        frame.render_widget(
            Paragraph::new("  Select snapshot type:")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[0],
        );
        frame.render_widget(Paragraph::new(lines), chunks[1]);

        frame.render_widget(
            Paragraph::new(" ↑↓ navigate │ Enter select │ Esc back")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }

    pub(super) fn key_network(&mut self, key: crossterm::event::KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.net_choice = self.net_choice.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.net_choice = (self.net_choice + 1).min(1);
            }
            KeyCode::Enter => {
                self.setup_screen = SetupScreen::Filters;
            }
            KeyCode::Esc => {
                self.setup_screen = SetupScreen::Source;
            }
            _ => {}
        }
    }

    // ── Filters ─────────────────────────────────────────────────

    pub(super) fn render_filters(&self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Filters ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Length(2),
            Constraint::Length(5),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(inner);

        frame.render_widget(
            Paragraph::new("  Filters (applied during parsing):")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[0],
        );

        let toggles = [
            (self.include_dead, "Include dead accounts (lamports = 0)"),
            (self.include_spam, "Include spam mints"),
        ];

        let lines: Vec<Line> = toggles
            .iter()
            .enumerate()
            .map(|(i, (on, label))| {
                let marker = if i == self.filter_focus {
                    "  ▸ "
                } else {
                    "    "
                };
                let checkbox = if *on { "[x]" } else { "[ ]" };
                let style = if i == self.filter_focus {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default().fg(Color::White)
                };
                Line::from(Span::styled(format!("{marker}{checkbox} {label}"), style))
            })
            .collect();

        frame.render_widget(Paragraph::new(lines), chunks[1]);

        let info = if self.source_choice == 0 {
            format!(
                "  Source: {}",
                self.selected_path
                    .as_ref()
                    .map_or("—".into(), |p| p.display().to_string())
            )
        } else {
            let kind = if self.net_choice == 0 {
                "full"
            } else {
                "incremental"
            };
            format!("  Source: network ({kind})")
        };
        frame.render_widget(
            Paragraph::new(info).style(Style::default().fg(Color::DarkGray)),
            chunks[2],
        );

        frame.render_widget(
            Paragraph::new(" ↑↓ navigate │ Space toggle │ Enter start │ Esc back")
                .style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }

    pub(super) fn key_filters(&mut self, key: crossterm::event::KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.filter_focus = self.filter_focus.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.filter_focus = (self.filter_focus + 1).min(1);
            }
            KeyCode::Char(' ') => match self.filter_focus {
                0 => self.include_dead = !self.include_dead,
                1 => self.include_spam = !self.include_spam,
                _ => {}
            },
            KeyCode::Enter => {
                if self.source_choice == 0 {
                    self.start_local();
                } else {
                    self.start_discover();
                }
            }
            KeyCode::Esc => {
                if self.source_choice == 0 {
                    self.setup_screen = SetupScreen::FilePicker;
                } else {
                    self.setup_screen = SetupScreen::NetworkType;
                }
            }
            _ => {}
        }
    }
}
