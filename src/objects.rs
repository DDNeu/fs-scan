use indicatif::ProgressBar;

use std::fs;
use std::path::PathBuf;
use std::process::Command;
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
pub fn build_result(path: &String) -> Result {
    Result {
        path: path.clone(),

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
    /// Maximum number of parallel threads
    #[arg(short = 't', long, default_value_t = 0)]
    pub max_threads: usize,
    /// If specified a log file is generated
    #[arg(short, long)]
    pub save_csv: bool,

    /// If specified try to use statx size for Lustre LSoM
    #[arg(short, long)]
    lustre_lsom: bool,
}

impl Config {
    pub fn handle_dir(&self, path: PathBuf, ch: Sender<ChanResponse>, bar: &ProgressBar) {
        match fs::read_dir(&path) {
            Ok(entries) => {
                let bar = bar.clone();
                let lsom = self.lustre_lsom;
                thread::spawn(move || {
                    for entry in entries {
                        match entry {
                            Ok(entry) => match entry.metadata() {
                                Ok(metadata) => {
                                    let ch = ch.clone();
                                    if metadata.is_dir() {
                                        ch.send(build_dir_chan(entry.path())).unwrap();
                                    } else if metadata.is_file() {
                                        if lsom {
                                            let out = Command::new("/bin/lfs")
                                                .arg("getsom")
                                                .arg("-s")
                                                .arg(entry.path())
                                                .output();
                                            match out {
                                                Ok(o) => {
                                                    if o.status.success() {
                                                        let mut cleaned_size = o.stdout.clone();
                                                        cleaned_size.truncate(o.stdout.len() - 1);

                                                        let size =
                                                            match String::from_utf8(cleaned_size) {
                                                                Ok(s_as_str) => {
                                                                    match u64::from_str_radix(
                                                                        &s_as_str, 10,
                                                                    ) {
                                                                        Ok(s) => s,
                                                                        Err(e) => {
                                                                            println!(
                                                                                "ERROR 1: {} '{}'",
                                                                                e, s_as_str
                                                                            );
                                                                            continue;
                                                                        }
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    println!("ERROR 2: {}", e);
                                                                    continue;
                                                                }
                                                            };

                                                        // Save the size of the given file
                                                        ch.send(build_file_chan(size)).unwrap();
                                                    } else {
                                                        println!(
                                                            "get LSoM failed: {}",
                                                            String::from_utf8(o.stderr).unwrap()
                                                        )
                                                    }
                                                }
                                                Err(e) => {
                                                    println!("lfs getsom not working: {e}");
                                                    println!("Failover to regular scan");
                                                }
                                            }
                                        } else {
                                            ch.send(build_file_chan(metadata.len())).unwrap();
                                        }
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
                                bar.println(format!("warning 1 {}", err));
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

    pub fn lsom_not_ok(&mut self) {
        self.lustre_lsom = false;
    }
    pub fn is_lsom(&self) -> bool {
        self.lustre_lsom
    }
}
