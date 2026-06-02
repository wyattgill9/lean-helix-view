//! The infoview viewer: the proxy↔viewer socket *client* + a ratatui app.
//!
//! `watch` dials the workspace socket (or `--socket`), subscribes, and renders
//! Goals / Expected type / Diagnostics / Progress in an adjacent pane. It is a
//! separate, disposable process: it survives the proxy being absent (connect
//! loop) or restarting (reconnect loop), and its own crash/exit never touches
//! the proxy or Helix.

use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use lhv_lsp::read_frame;
use lhv_wire::{Diagnostic, ServerMsg, Severity, Snapshot, workspace_socket_path};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Waiting,
    Connected,
    Disconnected,
}

struct ViewerState {
    snapshot: Snapshot,
    status: Status,
    scroll: u16,
}

type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Run the viewer until the user quits.
pub async fn run(socket: Option<PathBuf>) -> io::Result<()> {
    let path = socket.unwrap_or_else(workspace_socket_path);

    let state = Arc::new(Mutex::new(ViewerState {
        snapshot: Snapshot::default(),
        status: Status::Waiting,
        scroll: 0,
    }));

    // Network: connect/reconnect loop, decoupled from rendering.
    tokio::spawn(network_loop(path.clone(), state.clone()));

    // Input: a blocking reader thread funnels events into the async loop.
    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Event>();
    std::thread::spawn(move || input_thread(&input_tx));

    let mut terminal = setup_terminal()?;
    let _guard = TerminalGuard;

    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut tick: u64 = 0;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                tick = tick.wrapping_add(1);
                draw(&mut terminal, &state, &path, tick)?;
            }
            event = input_rx.recv() => {
                match event {
                    Some(ev) => {
                        if handle_event(ev, &state) == Flow::Quit {
                            break;
                        }
                        draw(&mut terminal, &state, &path, tick)?;
                    }
                    None => break, // input thread gone
                }
            }
        }
    }
    Ok(()) // TerminalGuard restores the terminal on the way out
}

async fn network_loop(path: PathBuf, state: Arc<Mutex<ViewerState>>) {
    let mut backoff: Option<Duration> = None;
    loop {
        match UnixStream::connect(&path).await {
            Ok(stream) => {
                backoff = None; // reset on a successful connect
                set_status(&state, Status::Connected);
                let mut reader = BufReader::new(stream);
                while let Ok(Some(frame)) = read_frame(&mut reader).await {
                    if let Ok(ServerMsg::Snapshot(snapshot)) = ServerMsg::from_json(frame.body()) {
                        state.lock().unwrap().snapshot = snapshot;
                    }
                }
                set_status(&state, Status::Disconnected);
            }
            Err(_) => set_status(&state, Status::Waiting),
        }
        let delay = next_backoff(backoff);
        backoff = Some(delay);
        tokio::time::sleep(delay).await; // bounded backoff, never a hot spin
    }
}

/// Bounded exponential reconnect backoff: 250ms → 500ms → 1s → 2s, capped at 3s.
fn next_backoff(current: Option<Duration>) -> Duration {
    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(3);
    match current {
        None => MIN,
        Some(d) => (d * 2).min(MAX),
    }
}

fn set_status(state: &Arc<Mutex<ViewerState>>, status: Status) {
    state.lock().unwrap().status = status;
}

fn input_thread(tx: &mpsc::UnboundedSender<Event>) {
    loop {
        match event::poll(Duration::from_millis(200)) {
            Ok(true) => match event::read() {
                Ok(ev) => {
                    if tx.send(ev).is_err() {
                        break; // the app has quit
                    }
                }
                Err(_) => break, // read error
            },
            Ok(false) => {}
            Err(_) => break,
        }
    }
}

#[derive(PartialEq, Eq)]
enum Flow {
    Continue,
    Quit,
}

fn handle_event(event: Event, state: &Arc<Mutex<ViewerState>>) -> Flow {
    if let Event::Key(key) = event {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return Flow::Continue;
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Flow::Quit,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Flow::Quit;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                let mut s = state.lock().unwrap();
                s.scroll = s.scroll.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                let mut s = state.lock().unwrap();
                s.scroll = s.scroll.saturating_sub(1);
            }
            KeyCode::Char('g') | KeyCode::Home => state.lock().unwrap().scroll = 0,
            _ => {}
        }
    }
    Flow::Continue
}

fn setup_terminal() -> io::Result<Tui> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

/// Restores the terminal on drop — including on early return or panic.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn draw(terminal: &mut Tui, state: &Arc<Mutex<ViewerState>>, path: &Path, tick: u64) -> io::Result<()> {
    let (snapshot, status, scroll) = {
        let s = state.lock().unwrap();
        (s.snapshot.clone(), s.status, s.scroll)
    };
    terminal.draw(|frame| ui(frame, &snapshot, status, scroll, path, tick))?;
    Ok(())
}

fn ui(frame: &mut ratatui::Frame, snap: &Snapshot, status: Status, scroll: u16, path: &Path, tick: u64) {
    let regions = Layout::vertical([
        Constraint::Length(1), // status
        Constraint::Min(6),    // goals (primary)
        Constraint::Length(5), // expected type
        Constraint::Length(7), // diagnostics
        Constraint::Length(1), // progress
    ])
    .split(frame.area());

    render_status(frame, regions[0], snap, status, path);
    render_goals(frame, regions[1], snap, scroll, status, path);
    render_expected(frame, regions[2], snap);
    render_diagnostics(frame, regions[3], snap);
    render_progress(frame, regions[4], snap, tick);
}

fn render_status(frame: &mut ratatui::Frame, area: Rect, snap: &Snapshot, status: Status, path: &Path) {
    let (label, color) = match status {
        Status::Connected => ("connected", Color::Green),
        Status::Waiting => ("waiting for proxy…", Color::Yellow),
        Status::Disconnected => ("disconnected, retrying…", Color::Red),
    };
    let mut spans = vec![
        Span::styled(" lean-helix-view ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(label, Style::default().fg(color)),
    ];
    if let (Some(doc), Some(pos)) = (&snap.doc, &snap.position) {
        let name = doc.uri.rsplit('/').next().unwrap_or(&doc.uri);
        spans.push(Span::styled(
            format!("  {name}:{}:{}", pos.line + 1, pos.character + 1),
            Style::default().fg(Color::DarkGray),
        ));
    } else {
        spans.push(Span::styled(
            format!("  {}", path.display()),
            Style::default().fg(Color::DarkGray),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_goals(
    frame: &mut ratatui::Frame,
    area: Rect,
    snap: &Snapshot,
    scroll: u16,
    status: Status,
    path: &Path,
) {
    // Not connected and nothing received yet → explain how to connect.
    if status != Status::Connected && snap.doc.is_none() {
        let help = format!(
            "No lean-helix-view proxy found for this workspace.\n\n\
             Is Helix running in this project?\n\n\
             Looking for socket:\n  {}\n\n\
             Override with:\n  lean-helix-view watch --socket <path>",
            path.display()
        );
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().borders(Borders::ALL).title(" Goals "))
                .wrap(Wrap { trim: false })
                .style(Style::default().fg(Color::DarkGray)),
            area,
        );
        return;
    }

    let body = if !snap.in_tactic {
        if snap.doc.is_some() {
            "not in tactic mode".to_string()
        } else {
            "waiting for goals…".to_string()
        }
    } else if snap.goals.is_empty() {
        "no goals — proof complete here".to_string()
    } else if let Some(rendered) = &snap.rendered {
        rendered.clone()
    } else {
        snap.goals.join("\n\n")
    };

    // Progress gating: if the focus is inside an elaborating region, the goals
    // may be stale — dim them and say so (milestone-6 refinement).
    let stale = focus_is_elaborating(snap);
    let title = if stale {
        " Goals (elaborating — may be stale) ".to_string()
    } else if snap.in_tactic && !snap.goals.is_empty() {
        format!(" Goals ({}) ", snap.goals.len())
    } else {
        " Goals ".to_string()
    };
    let style = if stale {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default()
    };
    frame.render_widget(
        Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false })
            .style(style)
            .scroll((scroll, 0)),
        area,
    );
}

/// Whether the focused position falls inside a region Lean is still
/// elaborating (so goals for it may be stale).
fn focus_is_elaborating(snap: &Snapshot) -> bool {
    match snap.position {
        Some(p) => snap
            .progress
            .iter()
            .any(|r| r.start.line <= p.line && p.line <= r.end.line),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_caps_and_never_hot_spins() {
        assert_eq!(next_backoff(None), Duration::from_millis(250));
        let mut d = next_backoff(None);
        let mut seen = vec![d];
        for _ in 0..10 {
            d = next_backoff(Some(d));
            seen.push(d);
        }
        assert!(seen.windows(2).all(|w| w[1] >= w[0]), "non-decreasing");
        assert_eq!(*seen.last().unwrap(), Duration::from_secs(3), "capped");
        assert!(
            seen.iter().all(|d| *d >= Duration::from_millis(250)),
            "never a tight retry"
        );
    }

    #[test]
    fn focus_inside_elaborating_region_is_stale() {
        use lhv_wire::{Position, Range};
        let mut snap = Snapshot {
            position: Some(Position { line: 4, character: 0 }),
            ..Snapshot::default()
        };
        assert!(!focus_is_elaborating(&snap));
        snap.progress = vec![Range {
            start: Position { line: 2, character: 0 },
            end: Position { line: 6, character: 0 },
        }];
        assert!(focus_is_elaborating(&snap));
    }
}

fn render_expected(frame: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let body = snap.term_goal.clone().unwrap_or_else(|| "—".to_string());
    frame.render_widget(
        Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL).title(" Expected type "))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_diagnostics(frame: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let diags = snap
        .doc
        .as_ref()
        .and_then(|doc| snap.diagnostics.get(&doc.uri));
    let lines: Vec<Line> = match diags {
        Some(ds) if !ds.is_empty() => ds.iter().map(diagnostic_line).collect(),
        _ => vec![Line::from(Span::styled("no diagnostics", Style::default().fg(Color::DarkGray)))],
    };
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(" Diagnostics "))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn diagnostic_line(d: &Diagnostic) -> Line<'static> {
    let (tag, color) = match d.severity {
        Severity::Error => ("E", Color::Red),
        Severity::Warning => ("W", Color::Yellow),
        Severity::Information => ("I", Color::Cyan),
        Severity::Hint => ("H", Color::Gray),
    };
    Line::from(vec![
        Span::styled(format!("{tag} "), Style::default().fg(color).add_modifier(Modifier::BOLD)),
        Span::styled(
            format!("{}:{} ", d.range.start.line + 1, d.range.start.character + 1),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(d.message.replace('\n', " ")),
    ])
}

fn render_progress(frame: &mut ratatui::Frame, area: Rect, snap: &Snapshot, tick: u64) {
    const FRAMES: [&str; 8] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠇"];
    let (text, color) = if snap.elaborating {
        let spinner = FRAMES[(tick as usize) % FRAMES.len()];
        (format!(" {spinner} elaborating…"), Color::Yellow)
    } else {
        (" idle".to_string(), Color::DarkGray)
    };
    frame.render_widget(Paragraph::new(text).style(Style::default().fg(color)), area);
}
