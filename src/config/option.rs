//! Option resolution: one flag name per option, read from the command line,
//! then `PIPESTREAM_SEARCH_<NAME>`, then the config file.

use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;

/// The product's one environment prefix.
const ENV_PREFIX: &str = "PIPESTREAM_SEARCH_";

/// Flags whose environment name is not the flag's own.
const ENV_NAMES: &[(&str, &str)] = &[
    ("turbovec-coordinator", "COORDINATOR"),
    ("turbovec-cluster-nodes", "CLUSTER_NODES"),
    ("turbovec-cluster-state", "CLUSTER_STATE"),
    (
        "allow-ephemeral-turbovec-cluster",
        "ALLOW_EPHEMERAL_CLUSTER",
    ),
];

/// The environment variable of `flag`: the prefix, then the flag in upper
/// case with `_` for `-`, unless [`ENV_NAMES`] gives the name.
pub(super) fn env_name(flag: &str) -> String {
    let name = ENV_NAMES
        .iter()
        .find(|(known, _)| *known == flag)
        .map_or_else(
            || flag.to_ascii_uppercase().replace('-', "_"),
            |(_, name)| (*name).to_string(),
        );
    format!("{ENV_PREFIX}{name}")
}

/// The one message shape for a value that does not parse.
pub(super) fn invalid(flag: &str, raw: &str, error: impl Display) -> String {
    format!("invalid --{flag} {raw:?}: {error}")
}

pub(super) fn parse_bool(text: &str) -> bool {
    matches!(text, "1" | "true" | "on" | "yes")
}

/// A comma-separated list: entries trimmed, empty entries dropped.
pub(super) fn split_list(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

/// The command line an option is resolved against.
#[derive(Clone, Copy)]
pub(super) struct Options<'a> {
    args: &'a [String],
}

impl<'a> Options<'a> {
    pub(super) fn new(args: &'a [String]) -> Self {
        Self { args }
    }

    /// `--flag=value` or `--flag value`.
    fn argument(&self, flag: &str) -> Option<String> {
        let prefix = format!("--{flag}=");
        if let Some(value) = self
            .args
            .iter()
            .find_map(|arg| arg.strip_prefix(&prefix).map(str::to_string))
        {
            return Some(value);
        }
        let bare = format!("--{flag}");
        self.args
            .windows(2)
            .find(|pair| pair[0] == bare && !pair[1].starts_with("--"))
            .map(|pair| pair[1].clone())
    }

    /// Every value of a repeatable flag, in both spellings.
    pub(super) fn arguments(&self, flag: &str) -> Vec<String> {
        let prefix = format!("--{flag}=");
        let bare = format!("--{flag}");
        let mut values: Vec<String> = self
            .args
            .iter()
            .filter_map(|arg| arg.strip_prefix(&prefix).map(str::to_string))
            .collect();
        for pair in self.args.windows(2) {
            if pair[0] == bare && !pair[1].starts_with("--") {
                values.push(pair[1].clone());
            }
        }
        values
    }

    /// Whether the bare `--flag` is on the command line.
    pub(super) fn bare(&self, flag: &str) -> bool {
        let bare = format!("--{flag}");
        self.args.contains(&bare)
    }

    /// The flag's value, or its environment variable's.
    pub(super) fn given(&self, flag: &str) -> Option<String> {
        self.argument(flag)
            .or_else(|| std::env::var(env_name(flag)).ok())
    }

    /// A text option.
    pub(super) fn text(&self, flag: &str, file: Option<&str>) -> Option<String> {
        self.given(flag).or_else(|| file.map(str::to_string))
    }

    /// A text option where blank means absent.
    pub(super) fn nonblank(&self, flag: &str, file: Option<&str>) -> Option<String> {
        self.text(flag, file)
            .filter(|value| !value.trim().is_empty())
    }

    /// A text option trimmed, where blank means absent.
    pub(super) fn trimmed(&self, flag: &str, file: Option<&str>) -> Option<String> {
        self.text(flag, file)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    /// A path option where blank means absent.
    pub(super) fn path(&self, flag: &str, file: Option<&str>) -> Option<PathBuf> {
        self.nonblank(flag, file).map(PathBuf::from)
    }

    /// A typed option: the flag or environment text parsed, else the file's
    /// value as the file typed it.
    pub(super) fn value<T>(&self, flag: &str, file: Option<T>) -> Result<Option<T>, String>
    where
        T: FromStr,
        T::Err: Display,
    {
        match self.given(flag) {
            Some(raw) => raw
                .parse::<T>()
                .map(Some)
                .map_err(|error| invalid(flag, &raw, error)),
            None => Ok(file),
        }
    }

    /// A typed option the file holds as text: every source is parsed.
    pub(super) fn parsed<T>(&self, flag: &str, file: Option<&str>) -> Result<Option<T>, String>
    where
        T: FromStr,
        T::Err: Display,
    {
        self.text(flag, file)
            .map(|raw| raw.parse::<T>().map_err(|error| invalid(flag, &raw, error)))
            .transpose()
    }

    /// A switch with a value: `--flag=true`, the environment, the file,
    /// then `default`.
    pub(super) fn switch(&self, flag: &str, file: Option<bool>, default: bool) -> bool {
        match self.given(flag) {
            Some(raw) => parse_bool(&raw),
            None => file.unwrap_or(default),
        }
    }

    /// [`Self::switch`] that the bare `--flag` also turns on.
    pub(super) fn switch_or_bare(&self, flag: &str, file: Option<bool>, default: bool) -> bool {
        self.bare(flag) || self.switch(flag, file, default)
    }

    /// A switch any source can turn on and none can turn off: the bare
    /// flag, a true environment value, or a true file value.
    pub(super) fn raised(&self, flag: &str, file: Option<bool>) -> bool {
        self.bare(flag)
            || std::env::var(env_name(flag)).is_ok_and(|raw| parse_bool(&raw))
            || file.unwrap_or(false)
    }

    /// A comma-separated list from the flag or environment, else the file's.
    pub(super) fn list(&self, flag: &str, file: Option<&Vec<String>>) -> Vec<String> {
        match self.given(flag) {
            Some(raw) => split_list(&raw),
            None => file.cloned().unwrap_or_default(),
        }
    }
}
