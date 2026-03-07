use std::sync::atomic::Ordering;

use ratatui::layout::{Alignment, Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Gauge, Paragraph};
use ratatui::Frame;

use super::helpers::format_rows;
use super::{App, SPINNER};

impl App {
    pub(super) fn render_discovering(&self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Discovering ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let elapsed = self.discover_start.elapsed();
        let spin = SPINNER[(elapsed.as_millis() / 80) as usize % SPINNER.len()];

        let chunks = Layout::vertical([
            Constraint::Min(3),
            Constraint::Length(3),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(inner);

        frame.render_widget(
            Paragraph::new(format!(
                "  {spin} Discovering fastest snapshot node... ({}s)",
                elapsed.as_secs()
            ))
            .style(Style::default().fg(Color::Cyan)),
            chunks[1],
        );

        frame.render_widget(
            Paragraph::new(" Esc cancel").style(Style::default().fg(Color::DarkGray)),
            chunks[3],
        );
    }

    pub(super) fn render_processing(&self, frame: &mut Frame) {
        let area = frame.area();
        let outer = Block::default()
            .title(" SSP — Processing Snapshot ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);
        let inner = outer.inner(area);
        frame.render_widget(outer, area);

        let chunks = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Length(4),
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(inner);

        let stats = match &self.stats {
            Some(s) => s,
            None => return,
        };

        let bytes = stats.bytes_read.load(Ordering::Relaxed);
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
            .block(Block::default().borders(Borders::ALL).title(" Progress "))
            .gauge_style(Style::default().fg(Color::Cyan).bg(Color::DarkGray))
            .ratio(ratio)
            .label(label);
        frame.render_widget(gauge, chunks[0]);

        let elapsed = self.start_time.elapsed();
        let secs = elapsed.as_secs_f64();
        let speed = if secs > 0.5 {
            bytes as f64 / secs / 1_048_576.0
        } else {
            0.0
        };
        let rows = stats.rows_parsed.load(Ordering::Relaxed);
        let eta = if ratio > 0.01 && ratio < 1.0 {
            let rem = secs / ratio * (1.0 - ratio);
            format!("{}m {:02}s", rem as u64 / 60, rem as u64 % 60)
        } else {
            "—".into()
        };

        let stat_text = Paragraph::new(vec![
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
        frame.render_widget(stat_text, chunks[2]);

        let health = Paragraph::new(vec![
            Line::from(Span::styled(
                "  Pipeline Health",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "  ├ parsers blocked (tx)       {}",
                stats.parser_blocked_tx.load(Ordering::Relaxed)
            )),
            Line::from(format!(
                "  ├ parsers blocked (decoded)  {}",
                stats.parser_blocked_decoded.load(Ordering::Relaxed)
            )),
            Line::from(format!(
                "  ├ writers starved (acct)     {}",
                stats.writer_starved_acct.load(Ordering::Relaxed)
            )),
            Line::from(format!(
                "  └ writers starved (decoded)  {}",
                stats.writer_starved_decoded.load(Ordering::Relaxed)
            )),
        ]);
        frame.render_widget(health, chunks[4]);

        frame.render_widget(
            Paragraph::new(" q quit ")
                .alignment(Alignment::Right)
                .style(Style::default().fg(Color::DarkGray)),
            chunks[5],
        );
    }
}
