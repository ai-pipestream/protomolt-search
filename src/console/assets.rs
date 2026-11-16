//! The UI's static files. They are compiled in; with `--ui-dir` a file
//! present in that directory is read from disk on every request instead,
//! so an edit shows on reload without rebuilding.

use super::*;

/// Every UI file, by the name its URL uses.
pub(super) const EMBEDDED: &[(&str, &str)] = &[
    ("index.html", include_str!("../bin/console-ui/index.html")),
    (
        "dashboard.html",
        include_str!("../bin/console-ui/dashboard.html"),
    ),
    (
        "explorer.html",
        include_str!("../bin/console-ui/explorer.html"),
    ),
    ("app.css", include_str!("../bin/console-ui/app.css")),
    ("common.js", include_str!("../bin/console-ui/common.js")),
    (
        "query-state.js",
        include_str!("../bin/console-ui/query-state.js"),
    ),
    (
        "sparkline.js",
        include_str!("../bin/console-ui/sparkline.js"),
    ),
    ("explorer.js", include_str!("../bin/console-ui/explorer.js")),
    ("search.js", include_str!("../bin/console-ui/search.js")),
    (
        "search-state.js",
        include_str!("../bin/console-ui/search-state.js"),
    ),
    (
        "search-form.js",
        include_str!("../bin/console-ui/search-form.js"),
    ),
    (
        "search-request.js",
        include_str!("../bin/console-ui/search-request.js"),
    ),
    (
        "search-run.js",
        include_str!("../bin/console-ui/search-run.js"),
    ),
    (
        "search-results.js",
        include_str!("../bin/console-ui/search-results.js"),
    ),
    (
        "search-hits.js",
        include_str!("../bin/console-ui/search-hits.js"),
    ),
    (
        "search-live.js",
        include_str!("../bin/console-ui/search-live.js"),
    ),
    (
        "dashboard.js",
        include_str!("../bin/console-ui/dashboard.js"),
    ),
    (
        "dashboard-state.js",
        include_str!("../bin/console-ui/dashboard-state.js"),
    ),
    (
        "dashboard-poll.js",
        include_str!("../bin/console-ui/dashboard-poll.js"),
    ),
    (
        "dashboard-metrics.js",
        include_str!("../bin/console-ui/dashboard-metrics.js"),
    ),
    (
        "dashboard-cluster.js",
        include_str!("../bin/console-ui/dashboard-cluster.js"),
    ),
];

/// The file a request path names: `/` and the page aliases, or one path
/// segment of ASCII letters, digits, `-`, `_` and `.` that does not start
/// with `.` and ends in `.html`, `.js` or `.css`. Anything else, `..`,
/// `/`, `\` and percent escapes included, names no file.
pub(super) fn asset_name(path: &str) -> Option<&str> {
    let name = match path {
        "/" => "index.html",
        "/dashboard" => "dashboard.html",
        "/explorer" => "explorer.html",
        _ => path.strip_prefix('/')?,
    };
    let allowed = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.');
    let typed = [".html", ".js", ".css"]
        .iter()
        .any(|ext| name.ends_with(ext));
    (typed && !name.starts_with('.') && name.bytes().all(allowed)).then_some(name)
}

fn content_type(name: &str) -> &'static str {
    if name.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if name.ends_with(".css") {
        "text/css; charset=utf-8"
    } else {
        "text/javascript; charset=utf-8"
    }
}

/// The reply for a UI file: read from `ui_dir` when the file is there,
/// else the compiled-in copy, else `None`. A file whose resolved path
/// leaves `ui_dir` (a symbolic link out of it) is refused.
pub(super) fn asset_reply(ui_dir: Option<&Path>, path: &str) -> Option<Reply> {
    let name = asset_name(path)?;
    if let Some(dir) = ui_dir {
        if let Some(reply) = disk_asset(dir, name) {
            return Some(reply);
        }
    }
    let (_, body) = EMBEDDED.iter().find(|(n, _)| *n == name)?;
    Some(Reply::text(200, content_type(name), body))
}

/// `name` under `dir`, or `None` when the directory has no such file.
/// `dir` is canonical (see `Console::with_ui_dir`).
fn disk_asset(dir: &Path, name: &str) -> Option<Reply> {
    let file = match dir.join(name).canonicalize() {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            return Some(Reply::error(
                500,
                "INTERNAL",
                format!("--ui-dir {name}: {e}"),
            ))
        }
    };
    if !file.starts_with(dir) {
        return Some(Reply::error(
            403,
            "PERMISSION_DENIED",
            format!("{name} resolves outside --ui-dir"),
        ));
    }
    match std::fs::read(&file) {
        Ok(body) => Some(Reply {
            status: 200,
            content_type: content_type(name),
            body,
        }),
        Err(e) => Some(Reply::error(
            500,
            "INTERNAL",
            format!("--ui-dir {name}: {e}"),
        )),
    }
}
