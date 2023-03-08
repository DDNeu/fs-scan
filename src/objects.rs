use indicatif::ProgressBar;

use std::fs::{self, ReadDir};
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

    /// If specified some additional information are provided.
    #[arg(short, long)]
    pub verbose: bool,

    /// If specified statx on linux won't be used.
    #[arg(long)]
    #[cfg(target_os = "linux")]
    pub prevent_statx: bool,
}

fn display_error_and_stop_thread_before_return(
    bar: &ProgressBar,
    ch: &Sender<ChanResponse>,
    message: String,
    path: String,
) {
    bar.println(message);
    // Notify the end of the thread
    match ch.send(build_dir_chan_done()) {
        Ok(_) => {}
        Err(e) => {
            bar.println(format!(
                "Expect channel to be able to send ERR: 5766\n{:}\n{e}",
                path,
            ));
        }
    }
}

impl Config {
    pub fn handle_dir(
        &self,
        path: &PathBuf,
        ch: Sender<ChanResponse>,
        bar: &ProgressBar,
        statx_capable: bool,
    ) {
        let path_as_str = match path.to_str() {
            Some(s) => s,
            None => {
                return display_error_and_stop_thread_before_return(
                    bar,
                    &ch,
                    format!("Expect path to be real string but got error for {path:?}"),
                    "can't get path".to_string(),
                );
            }
        };

        match fs::read_dir(path) {
            Ok(entries) => {
                let bar = bar.clone();
                let statx_capable = statx_capable;
                let path_as_str = path_as_str.to_string();
                let path = path.clone();

                thread::spawn(move || {
                    if statx_capable {
                        #[cfg(target_os = "linux")]
                        statx_scroller(entries, &ch, &bar, path);
                    } else {
                        regular_scroller(entries, &ch, &bar, path);
                    }
                    // Notify the end of the thread
                    match ch.send(build_dir_chan_done()) {
                        Ok(_) => {}
                        Err(e) => {
                            bar.println(format!(
                                "Expect channel to be able to send the signal to tell the directory \"{:}\" is done successfully but got error:\n{e}",
                                path_as_str,
                            ));
                        }
                    }
                });
            }
            Err(err) => {
                bar.println(format!(
                    "Can't read the directory content of \"{:?}\": {}",
                    path_as_str, err
                ));
                // Notify the end of the thread
                match ch.send(build_dir_chan_done()) {
                    Ok(_) => {}
                    Err(e) => {
                        bar.println(format!(
                            "Expect channel to be able to send ERR: 4252\n{:}\n{e}",
                            path_as_str,
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn statx_scroller(entries: ReadDir, ch: &Sender<ChanResponse>, bar: &ProgressBar, path: PathBuf) {
    use rustix::fs::{cwd, openat, statx, AtFlags, Mode, OFlags, StatxFlags};
    use std::ffi::{CString, OsStr};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    let path_as_str = match path.to_str() {
        Some(s) => s,
        None => {
            return display_error_and_stop_thread_before_return(
                &bar,
                &ch,
                format!("Expect path to be real string but got error for {path:?}"),
                "can't get path".to_string(),
            );
        }
    };

    let dir_c_str = match CString::new(path_as_str) {
        Ok(cs) => cs,
        Err(e) => {
            return display_error_and_stop_thread_before_return(
                &bar,
                &ch,
                format!("Expect to be able to convert path into CString for {path:?}: {e:?}"),
                path_as_str.to_string(),
            );
        }
    };

    let dir = match openat(
        cwd(),
        &dir_c_str,
        OFlags::RDONLY | OFlags::DIRECTORY,
        Mode::empty(),
    ) {
        Ok(d) => d,
        Err(e) => {
            return display_error_and_stop_thread_before_return(
                &bar,
                &ch,
                format!(
                    "Error opening directory \"{:?}\" with error {e:?}",
                    &path_as_str
                ),
                path_as_str.to_string(),
            );
        }
    };

    for entry in entries {
        match entry {
            Ok(entry) => {
                match entry.file_type() {
                    Ok(t) => {
                        if t.is_dir() {
                            match ch.send(build_dir_chan(entry.path())) {
                                Ok(_) => {}
                                Err(e) => {
                                    bar.println(
                            format!(
                                "Expect channel to be able to send ERR: 6151\n{path_as_str:}/{:?}\n{e:?}", entry.path(),
                            )
                        );
                                }
                            }
                            continue;
                        }
                    }
                    Err(e) => {
                        bar.println(format!(
                            "Can't get type of file \"{path_as_str:}/{:?}\"\n{e:?}",
                            entry.path()
                        ));
                        continue;
                    }
                }

                let file_c_str = match CString::new(match entry.file_name().to_str() {
                    Some(s) => s,
                    None => {
                        bar.println(
                            format!("Expected file name or {path_as_str:}/{entry:?}").as_str(),
                        );
                        return display_error_and_stop_thread_before_return(
                            &bar,
                            &ch,
                            format!("Expected file name or {path_as_str:}/{entry:?}"),
                            path_as_str.to_string(),
                        );
                    }
                }) {
                    Ok(cs) => cs,
                    Err(e) => {
                        return display_error_and_stop_thread_before_return(
                            &bar,
                            &ch,
                            format!("Expected file name or \"{path_as_str:?}/{entry:?}\" {e:?}"),
                            path_as_str.to_string(),
                        );
                    }
                };

                let stat = match statx(
                    &dir,
                    &file_c_str,
                    AtFlags::SYMLINK_NOFOLLOW | AtFlags::STATX_DONT_SYNC,
                    StatxFlags::SIZE | StatxFlags::TYPE,
                ) {
                    Ok(stat) => stat,
                    Err(err) => {
                        bar.println(format!(
                            "Failed to stat file \"{:?}\" with error {err:?}",
                            Path::new(OsStr::from_bytes(dir_c_str.as_bytes()))
                                .join(Path::new(OsStr::from_bytes(file_c_str.to_bytes())))
                        ));
                        continue;
                    }
                };
                match ch.send(build_file_chan(stat.stx_size)) {
                    Ok(_) => {}
                    Err(e) => {
                        bar.println(format!(
                            "Expect channel to be able to send ERR: 8096\n{:?}\n{e:?}",
                            entry.path(),
                        ));
                        continue;
                    }
                }
            }
            Err(err) => {
                bar.println(format!(
                    "Can't display the entry of directory \"{:?}\" {err}",
                    path_as_str
                ));
            }
        }
    }
}

fn regular_scroller(entries: ReadDir, ch: &Sender<ChanResponse>, bar: &ProgressBar, path: PathBuf) {
    for entry in entries {
        match entry {
            Ok(entry) => match entry.metadata() {
                Ok(metadata) => {
                    if metadata.is_dir() {
                        match ch.send(build_dir_chan(entry.path())) {
                            Ok(_) => {}
                            Err(e) => {
                                bar.println(format!(
                                    "Expect channel to be able to send ERR: 585\n{:?}\n{e:?}",
                                    entry.path(),
                                ));
                            }
                        };
                    } else if metadata.is_file() {
                        match ch.send(build_file_chan(metadata.len())) {
                            Ok(_) => {}
                            Err(e) => {
                                bar.println(format!(
                                    "Expect channel to be able to send ERR: 9656\n{:?}\n{e:?}",
                                    entry.path(),
                                ));
                            }
                        };
                    }
                }
                Err(err) => {
                    bar.println(format!(
                        "Couldn't get file metadata for {:?}: {}",
                        entry.path(),
                        err
                    ));
                }
            },
            Err(err) => {
                bar.println(format!(
                    "Can't display the entry of directory \"{:?}\" {err}",
                    path
                ));
            }
        }
    }
}
