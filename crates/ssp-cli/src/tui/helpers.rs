use std::path::Path;

use super::FileEntry;

pub fn read_dir_entries(dir: &Path) -> Vec<FileEntry> {
    let mut entries = Vec::new();

    if let Some(parent) = dir.parent() {
        entries.push(FileEntry {
            name: "../".into(),
            path: parent.to_path_buf(),
            is_dir: true,
            size: 0,
        });
    }

    let Ok(rd) = std::fs::read_dir(dir) else {
        return entries;
    };

    let mut dirs = Vec::new();
    let mut files = Vec::new();

    for entry in rd.flatten() {
        let meta = entry.metadata().ok();
        let is_dir = meta.as_ref().is_some_and(|m| m.is_dir());
        let size = meta.as_ref().map_or(0, |m| m.len());
        let name = entry.file_name().to_string_lossy().to_string();

        if name.starts_with('.') {
            continue;
        }

        if is_dir {
            dirs.push(FileEntry {
                name: format!("{name}/"),
                path: entry.path(),
                is_dir: true,
                size: 0,
            });
        } else if name.ends_with(".tar.zst") || name.ends_with(".tar.bz2") {
            files.push(FileEntry {
                name,
                path: entry.path(),
                is_dir: false,
                size,
            });
        }
    }

    dirs.sort_by(|a, b| a.name.cmp(&b.name));
    files.sort_by(|a, b| a.name.cmp(&b.name));
    entries.extend(dirs);
    entries.extend(files);
    entries
}

pub fn resolve_quick_command(lower: &str) -> Option<String> {
    let parts: Vec<&str> = lower.split_whitespace().collect();
    match parts.as_slice() {
        ["top"] => Some("SELECT * FROM accounts ORDER BY lamports DESC LIMIT 10".into()),
        ["top", n] if n.parse::<u32>().is_ok() => Some(format!(
            "SELECT * FROM accounts ORDER BY lamports DESC LIMIT {n}"
        )),
        ["top", "mints"] => Some("SELECT * FROM mints ORDER BY supply DESC LIMIT 10".into()),
        ["top", "mints", n] if n.parse::<u32>().is_ok() => Some(format!(
            "SELECT * FROM mints ORDER BY supply DESC LIMIT {n}"
        )),
        ["top", "tokens"] => {
            Some("SELECT * FROM token_accounts ORDER BY amount DESC LIMIT 10".into())
        }
        ["top", "tokens", n] if n.parse::<u32>().is_ok() => Some(format!(
            "SELECT * FROM token_accounts ORDER BY amount DESC LIMIT {n}"
        )),
        ["count"] => Some("SELECT COUNT(*) AS count FROM accounts".into()),
        ["count", table] => Some(format!("SELECT COUNT(*) AS count FROM {table}")),
        ["schema", table] => Some(format!(
            "SELECT column_name, column_type FROM (DESCRIBE {table})"
        )),
        _ => None,
    }
}

pub fn prepare_table(
    columns: &[String],
    rows: &[Vec<String>],
    available: usize,
) -> (Vec<usize>, Vec<Vec<String>>) {
    let ncols = columns.len().max(1);
    let usable = available.saturating_sub(ncols.saturating_sub(1) * 3);
    let max_col = (usable / ncols).max(6);

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len().min(max_col)).collect();
    for row in rows.iter().take(50) {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len()).min(max_col);
            }
        }
    }

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

pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else if max <= 2 {
        s[..max].to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}

pub fn format_rows(n: u64) -> String {
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

pub fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.0} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
