use indicatif::ProgressBar;

use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

pub struct Result {
    pub path: String,
    pub duration: Duration,
    pub files: usize,
    pub directories: usize,
    pub empty_file: usize,
    pub less_than_4_k: usize,
    pub between_4_k_8_k: usize,
    pub between_8_k_16_k: usize,
    pub between_16_k_32_k: usize,
    pub between_32_k_64_k: usize,
    pub between_64_k_128_k: usize,
    pub between_128_k_256_k: usize,
    pub between_256_k_512_k: usize,
    pub between_512_k_1_m: usize,
    pub between_1_m_10_m: usize,
    pub between_10_m_100_m: usize,
    pub between_100_m_1_g: usize,
    pub more_than_1_g: usize,
}
pub fn build_result(path: &str) -> Result {
    Result {
        path: path.to_string(),

        duration: Duration::new(0, 0),

        files: 0,
        directories: 0,

        empty_file: 0,
        less_than_4_k: 0,
        between_4_k_8_k: 0,
        between_8_k_16_k: 0,
        between_16_k_32_k: 0,
        between_32_k_64_k: 0,
        between_64_k_128_k: 0,
        between_128_k_256_k: 0,
        between_256_k_512_k: 0,
        between_512_k_1_m: 0,
        between_1_m_10_m: 0,
        between_10_m_100_m: 0,
        between_100_m_1_g: 0,
        more_than_1_g: 0,
    }
}

impl Result {
    pub fn csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            &self.path,
            &self.duration.as_millis(),
            &self.files,
            &self.directories,
            &self.empty_file,
            &self.less_than_4_k,
            &self.between_4_k_8_k,
            &self.between_8_k_16_k,
            &self.between_16_k_32_k,
            &self.between_32_k_64_k,
            &self.between_64_k_128_k,
            &self.between_128_k_256_k,
            &self.between_256_k_512_k,
            &self.between_512_k_1_m,
            &self.between_1_m_10_m,
            &self.between_10_m_100_m,
            &self.between_100_m_1_g,
            &self.more_than_1_g,
        )
    }
}

pub enum ResponseType {
    File,
    Dir,
    DoneDir,
}
pub struct ChanResponse {
    pub t: ResponseType,
    pub path: PathBuf,
    pub len: u64,
}
pub fn build_dir_chan(path: PathBuf) -> ChanResponse {
    ChanResponse {
        t: ResponseType::Dir,
        path,
        len: 0,
    }
}
pub fn build_dir_chan_done() -> ChanResponse {
    ChanResponse {
        t: ResponseType::DoneDir,
        path: PathBuf::new(),
        len: 0,
    }
}
pub fn build_file_chan(size: u64) -> ChanResponse {
    ChanResponse {
        t: ResponseType::File,
        path: PathBuf::new(),
        len: size,
    }
}

use clap::Parser;

/// Scan recursively the given directory and generate a report of the scanned files based on their relative size.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    pub path: String,
    /// Maximum number of parallel threads. If not configured, 4 times the number of detected logical CPU.
    #[arg(short = 't', long, default_value_t = 0)]
    pub max_threads: usize,
    /// If specified a CSV log file is generated. Multiple run can be done from the same directory to collect outputs from multiple directories in a single file.
    #[arg(short, long)]
    pub save_csv: bool,
    /// If specified use statx size for Lustre LSoM. No effect on Windows target
    #[arg(short, long)]
    pub lustre_lsom: bool,
}

impl Config {
    pub fn handle_dir(&self, path: PathBuf, ch: Sender<ChanResponse>, bar: &ProgressBar) {
        use rustix::fs::{cwd, openat, statx, AtFlags, Mode, OFlags, StatxFlags};
        use std::ffi::{CString, OsStr};
        use std::os::unix::ffi::OsStrExt;
        use std::path::Path;

        let dir_c_str = CString::new(path.to_str().unwrap()).unwrap();
        let dir = openat(
            cwd(),
            &dir_c_str,
            OFlags::RDONLY | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .unwrap();

        match fs::read_dir(&path) {
            Ok(entries) => {
                let bar = bar.clone();
                let lustre_lsom = self.lustre_lsom;
                thread::spawn(move || {
                    for entry in entries {
                        match entry {
                            Ok(entry) => {
                                if lustre_lsom {
                                    match entry.file_type() {
                                        Ok(t) => {
                                            if t.is_dir() {
                                                ch.send(build_dir_chan(entry.path())).unwrap();
                                                continue;
                                            }
                                        }
                                        Err(e) => {
                                            bar.println(format!("Can't get type of file {e:?}"));
                                            continue;
                                        }
                                    }

                                    let file_c_str = CString::new(
                                        entry.file_name().to_str().expect("path existed"),
                                    )
                                    .unwrap();
                                    let dir_c_str = CString::new(
                                        entry.file_name().to_str().expect("path existed"),
                                    )
                                    .unwrap();

                                    let stat = statx(
                                        &dir,
                                        &file_c_str,
                                        AtFlags::SYMLINK_NOFOLLOW | AtFlags::STATX_DONT_SYNC,
                                        StatxFlags::SIZE | StatxFlags::TYPE,
                                    )
                                    .unwrap_or_else(|_| {
                                        panic!(
                                            "Failed to stat file: {:?}",
                                            Path::new(OsStr::from_bytes(dir_c_str.as_bytes()))
                                                .join(Path::new(OsStr::from_bytes(
                                                    file_c_str.to_bytes()
                                                )))
                                        )
                                    });
                                    ch.send(build_file_chan(stat.stx_size)).unwrap();
                                } else {
                                    match entry.metadata() {
                                        Ok(metadata) => {
                                            let ch = ch.clone();
                                            if metadata.is_dir() {
                                                ch.send(build_dir_chan(entry.path())).unwrap();
                                            } else if metadata.is_file() {
                                                ch.send(build_file_chan(metadata.len())).unwrap();
                                            }
                                        }
                                        Err(err) => {
                                            bar.println(format!(
                                                "Couldn't get file metadata for {:?}: {}",
                                                entry.path(),
                                                err
                                            ));
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                bar.println(format!("warning 1 {err}"));
                            }
                        }
                    }
                    // Notify the end of the thread
                    ch.send(build_dir_chan_done()).unwrap();
                });
            }
            Err(err) => {
                bar.println(format!("warning 0 {} {:?}", err, &path));
                // Notify the end of the thread
                ch.send(build_dir_chan_done()).unwrap();
            }
        }
    }
}
