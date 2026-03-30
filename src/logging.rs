//! Configurable log rotation and compression for hc-hue.
//!
//! Drop-in replacement for the hard-coded `tracing_appender::rolling::daily()`.
//! Supports time-based rotation (hourly/daily/weekly/never), optional size-based
//! rotation, and gzip compression of rotated files.
//!
//! **Config:**
//! ```toml
//! [logging]
//! level       = "info"    # stderr filter; RUST_LOG overrides this
//! rotation    = "daily"   # daily | hourly | weekly | never
//! max_size_mb = 100       # 0 = time-only rotation (no size limit)
//! compress    = true      # gzip rotated files in a background thread
//! ```

use flate2::{write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

// ── Config ───────────────────────────────────────────────────────────────────

fn default_level() -> String {
    "info".into()
}
fn default_max_size_mb() -> u64 {
    100
}
fn default_compress() -> bool {
    true
}

/// Rotation strategy for the log file.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum RotationStrategy {
    #[default]
    Daily,
    Hourly,
    Weekly,
    Never,
}

/// `[logging]` section in `config/config.toml`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    /// Stderr log level when `RUST_LOG` is not set.
    /// Accepts any `tracing` filter directive, e.g. `"debug"` or `"hc_hue=debug,rumqttc=warn"`.
    #[serde(default = "default_level")]
    pub level: String,
    /// Time-based rotation strategy: `daily` (default), `hourly`, `weekly`, or `never`.
    #[serde(default)]
    pub rotation: RotationStrategy,
    /// Rotate the active log file when it exceeds this many megabytes.
    /// Combined with `rotation` as "whichever comes first".
    /// Set to `0` to disable size-based rotation.
    #[serde(default = "default_max_size_mb")]
    pub max_size_mb: u64,
    /// Gzip-compress rotated files in a background thread.
    #[serde(default = "default_compress")]
    pub compress: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_level(),
            rotation: RotationStrategy::Daily,
            max_size_mb: default_max_size_mb(),
            compress: default_compress(),
        }
    }
}

// ── RotatingWriter ────────────────────────────────────────────────────────────

/// File writer that rotates on a time schedule and/or when a size limit is hit.
/// Implements `std::io::Write`; pass to `tracing_appender::non_blocking`.
pub struct RotatingWriter {
    file: File,
    bytes_written: u64,
    max_bytes: u64,
    rotation: RotationStrategy,
    current_period: String,
    dir: PathBuf,
    prefix: String,
    compress: bool,
    period_counter: u32,
}

impl RotatingWriter {
    pub fn new(
        dir: PathBuf,
        prefix: String,
        rotation: RotationStrategy,
        max_bytes: u64,
        compress: bool,
    ) -> io::Result<Self> {
        let current_period = period_str(&rotation);
        let active = active_path(&dir, &prefix);
        let file = open_append(&active)?;
        let bytes_written = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            file,
            bytes_written,
            max_bytes,
            rotation,
            current_period,
            dir,
            prefix,
            compress,
            period_counter: 0,
        })
    }

    fn maybe_rotate(&mut self) -> io::Result<()> {
        let new_period = period_str(&self.rotation);
        let period_changed = !new_period.is_empty() && new_period != self.current_period;
        let size_exceeded = self.max_bytes > 0 && self.bytes_written >= self.max_bytes;

        if !period_changed && !size_exceeded {
            return Ok(());
        }

        self.file.flush()?;

        if period_changed {
            self.period_counter = 0;
            self.current_period = new_period;
        }

        let rotated = self.next_rotated_path();
        let active = active_path(&self.dir, &self.prefix);

        std::fs::rename(&active, &rotated)?;

        if self.compress {
            compress_in_background(rotated);
        }

        self.file = open_append(&active)?;
        self.bytes_written = 0;
        self.period_counter += 1;

        Ok(())
    }

    fn next_rotated_path(&self) -> PathBuf {
        let period = if self.current_period.is_empty() {
            chrono::Local::now().format("%Y-%m-%dT%H%M%S").to_string()
        } else {
            self.current_period.clone()
        };

        if self.period_counter == 0 {
            let candidate = self.dir.join(format!("{}.{}.log", self.prefix, period));
            if !candidate.exists() {
                return candidate;
            }
        }

        let start = if self.period_counter == 0 {
            1
        } else {
            self.period_counter
        };
        let mut n = start;
        loop {
            let candidate = self
                .dir
                .join(format!("{}.{}.{}.log", self.prefix, period, n));
            if !candidate.exists() {
                return candidate;
            }
            n += 1;
        }
    }
}

impl Write for RotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.maybe_rotate()?;
        let n = self.file.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn active_path(dir: &Path, prefix: &str) -> PathBuf {
    dir.join(format!("{}.log", prefix))
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn period_str(rotation: &RotationStrategy) -> String {
    let now = chrono::Local::now();
    match rotation {
        RotationStrategy::Hourly => now.format("%Y-%m-%d_%H").to_string(),
        RotationStrategy::Daily => now.format("%Y-%m-%d").to_string(),
        RotationStrategy::Weekly => now.format("%Y-W%V").to_string(),
        RotationStrategy::Never => String::new(),
    }
}

fn compress_in_background(src: PathBuf) {
    std::thread::spawn(move || {
        let mut gz_os = src.as_os_str().to_owned();
        gz_os.push(".gz");
        let gz_path = PathBuf::from(gz_os);

        let result: io::Result<()> = (|| {
            let mut input = File::open(&src)?;
            let output = File::create(&gz_path)?;
            let mut encoder = GzEncoder::new(output, Compression::default());
            io::copy(&mut input, &mut encoder)?;
            encoder.finish()?;
            drop(input);
            std::fs::remove_file(&src)?;
            Ok(())
        })();

        if let Err(e) = result {
            eprintln!("log compression failed for {:?}: {e}", src);
        }
    });
}

// ── Public init ───────────────────────────────────────────────────────────────

/// Initialise tracing: stderr layer + rotating compressed file layer.
///
/// - `config_path`: path to `config/config.toml`; derives the `logs/` dir.
/// - `prefix`: log file name prefix, e.g. `"hc-hue"` → `logs/hc-hue.log`.
/// - `stderr_default`: filter used for stderr when `RUST_LOG` is unset and
///   `cfg.level` is the default `"info"`, e.g. `"hc_hue=info"`.
/// - `cfg`: `[logging]` config from the plugin config file.
///
/// **The returned `WorkerGuard` must be kept alive for the process lifetime.**
pub fn init_logging(
    config_path: &str,
    prefix: &str,
    stderr_default: &str,
    cfg: &LoggingConfig,
) -> tracing_appender::non_blocking::WorkerGuard {
    let log_dir = Path::new(config_path)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("logs"))
        .unwrap_or_else(|| PathBuf::from("logs"));
    std::fs::create_dir_all(&log_dir).ok();

    let max_bytes = cfg.max_size_mb.saturating_mul(1024 * 1024);
    let writer = RotatingWriter::new(
        log_dir,
        prefix.to_string(),
        cfg.rotation.clone(),
        max_bytes,
        cfg.compress,
    )
    .expect("failed to open log file");

    let (non_blocking, guard) = tracing_appender::non_blocking(writer);

    let effective_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
        if cfg.level == "info" {
            stderr_default.to_string()
        } else {
            cfg.level.clone()
        }
    });

    let stderr_filter = EnvFilter::new(&effective_filter);

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(stderr_filter);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_filter(EnvFilter::new(&effective_filter));

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();

    guard
}
