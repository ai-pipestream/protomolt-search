//! Option resolution. An option is named by its flag; the command line wins,
//! then the environment variable `PIPESTREAM_SEARCH_<FLAG>` (the flag in upper
//! case with `_` for `-`), then the config file, then the caller's default.

use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;

/// The product's one environment prefix.
pub(super) const ENV_PREFIX: &str = "PIPESTREAM_SEARCH_";

/// The environment variable that carries `flag`.
pub(super) fn env_name(flag: &str) -> String {
    format!(
        "{ENV_PREFIX}{}",
        flag.to_ascii_uppercase().replace('-', "_")
    )
}

/// The one message shape for a value that does not parse.
pub(super) fn invalid(flag: &str, raw: &str, error: impl Display) -> String {
    format!("invalid --{flag} {raw:?}: {error}")
}

/// The truthy spellings a switch accepts; anything else is off.
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

/// The command line every option is resolved against.
#[derive(Clone, Copy)]
pub(super) struct Options<'a> {
    args: &'a [String],
}

impl<'a> Options<'a> {
    pub(super) fn new(args: &'a [String]) -> Self {
        Self { args }
    }

    /// `--flag=value`, else `--flag value`.
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

    /// The flag's value, else its environment variable's.
    pub(super) fn given(&self, flag: &str) -> Option<String> {
        self.argument(flag)
            .or_else(|| std::env::var(env_name(flag)).ok())
    }

    /// A text option.
    pub(super) fn text(&self, flag: &str, file: Option<&str>) -> Option<String> {
        self.given(flag).or_else(|| file.map(str::to_string))
    }

    /// A text option where a blank value means absent.
    pub(super) fn nonblank(&self, flag: &str, file: Option<&str>) -> Option<String> {
        self.text(flag, file)
            .filter(|value| !value.trim().is_empty())
    }

    /// A text option, trimmed, where a blank value means absent.
    pub(super) fn trimmed(&self, flag: &str, file: Option<&str>) -> Option<String> {
        self.text(flag, file)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    /// A path option where a blank value means absent.
    pub(super) fn path(&self, flag: &str, file: Option<&str>) -> Option<PathBuf> {
        self.nonblank(flag, file).map(PathBuf::from)
    }

    /// A typed option: flag or environment text parsed, else the file's
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

    /// [`Self::value`] with a default for the absent case.
    pub(super) fn value_or<T>(&self, flag: &str, file: Option<T>, default: T) -> Result<T, String>
    where
        T: FromStr,
        T::Err: Display,
    {
        Ok(self.value(flag, file)?.unwrap_or(default))
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

    /// A comma-separated list from the flag or environment, else the file's.
    pub(super) fn list(&self, flag: &str, file: Option<&Vec<String>>) -> Vec<String> {
        match self.given(flag) {
            Some(raw) => split_list(&raw),
            None => file.cloned().unwrap_or_default(),
        }
    }
}
