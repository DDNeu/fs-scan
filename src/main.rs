mod csv;
mod objects;

use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time;

use clap::Parser;
use indicatif::{HumanDuration, ProgressBar, ProgressStyle};

use colored::Colorize;

fn main() {
    let mut conf = objects::Config::parse();

    if conf.max_threads == 0 {
        conf.max_threads = num_cpus::get() * 4;
    }

    let statx_capable = statx_supported(&conf.path, conf.verbose);

    let mut res = objects::build_result(&conf.path);

    // build channel
    let (sender, receiver) = channel();

    let bar = ProgressBar::new(conf.max_threads as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{elapsed} {bar:.cyan/blue} {pos:>3}/{len:3} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    // Start scanning at the given path
    conf.handle_dir(
        &PathBuf::from(&conf.path),
        sender.clone(),
        &bar,
        statx_capable,
    );

    let cloned_sender_again = sender;
    let mut running_thread = 0;
    let mut dir_queue = Vec::new();

    let starting_point = time::Instant::now();

    let display_refresh_time = time::Duration::from_millis(250);
    let mut last_message = time::Instant::now()
        .checked_sub(display_refresh_time)
        .expect("to remove some time");

    // Handle responses
    for received in receiver {
        //  Limit the display refresh
        let dur = time::Instant::now().duration_since(last_message);
        if dur > display_refresh_time {
            bar.set_message(format!(
                "files scanned {} and dirs in queue {}",
                &res.files,
                &dir_queue.len()
            ));
            bar.set_position(running_thread as u64);

            last_message = time::Instant::now();
        }

        // Check the type of the given element
        match received.t {
            // If Dir
            objects::ResponseType::Dir => {
                res.directories += 1;
                // Check if the number of running thread is not too height
                if running_thread >= conf.max_threads {
                    // If it's over four times the number of CPU than the folder is saved into a queue
                    dir_queue.push(received);
                } else {
                    // No problem with too much concurrency, so let's run the scan right away
                    running_thread += 1;

                    // // Add latency to debug the display
                    // thread::sleep(time::Duration::from_millis(5));

                    conf.handle_dir(
                        &received.path,
                        cloned_sender_again.clone(),
                        &bar,
                        statx_capable,
                    );
                }
            }
            // If this signal a directory scan terminated
            objects::ResponseType::DoneDir => {
                // The process is done
                // Break the loop to display the results
                if running_thread == 0 {
                    bar.set_message(format!("Total file scanned {}", &res.files));
                    break;
                }
                match dir_queue.pop() {
                    Some(dir) => {
                        // // Add latency to debug the display
                        // thread::sleep(time::Duration::from_millis(5));

                        conf.handle_dir(
                            &dir.path,
                            cloned_sender_again.clone(),
                            &bar,
                            statx_capable,
                        );
                    }
                    None => {
                        running_thread -= 1;
                    }
                };
            }
            // If File
            objects::ResponseType::File => {
                handle_file(received.len, &mut res);
            }
        }
    }
    bar.finish();

    // Save the time spend
    res.duration = starting_point.elapsed();

    if conf.save_csv {
        csv::save(&res);
    }

    let ms_dur = res.duration.as_millis();
    let mut duration_to_display = ms_dur.to_string() + "ms";
    if ms_dur > 1000 {
        duration_to_display = HumanDuration(res.duration).to_string();
    }
    println!("Scan took {}", duration_to_display.bold());

    println!("Files -> {}", nice_number(res.files));
    println!("Directories -> {}", nice_number(res.directories));
    println!("Empty files -> {}", nice_number(res.empty_file));
    println!("Less than 4K -> {}", nice_number(res.less_than_4_k));
    println!(
        "Between 4KB and 8KB -> {}",
        nice_number(res.between_4_k_8_k)
    );
    println!(
        "Between 8KB and 16KB -> {}",
        nice_number(res.between_8_k_16_k)
    );
    println!(
        "Between 16KB and 32KB -> {}",
        nice_number(res.between_16_k_32_k)
    );
    println!(
        "Between 32KB and 64KB -> {}",
        nice_number(res.between_32_k_64_k)
    );
    println!(
        "Between 64KB and 128KB -> {}",
        nice_number(res.between_64_k_128_k)
    );
    println!(
        "Between 128KB and 256KB -> {}",
        nice_number(res.between_128_k_256_k)
    );
    println!(
        "Between 256KB and 512KB -> {}",
        nice_number(res.between_256_k_512_k)
    );
    println!(
        "Between 512KB and 1MB -> {}",
        nice_number(res.between_512_k_1_m)
    );
    println!(
        "Between 1MB and 10MB -> {}",
        nice_number(res.between_1_m_10_m)
    );
    println!(
        "Between 10MB and 100MB -> {}",
        nice_number(res.between_10_m_100_m)
    );
    println!(
        "Between 100MB and 1GB -> {}",
        nice_number(res.between_100_m_1_g)
    );
    println!("More than 1GB -> {}", nice_number(res.more_than_1_g));
}

fn nice_number(input: usize) -> colored::ColoredString {
    if input < 1_000 {
        format!("{input:?}").bold()
    } else if input < 1_000_000 {
        format!("{:?}K ({:?})", input / 1_000, input).bold()
    } else {
        format!("{:?}M ({:?})", input / 1_000_000, input).bold()
    }
}

fn handle_file(len: u64, res: &mut objects::Result) {
    if len == 0 {
        res.empty_file += 1;
    } else if len < 4_000 {
        res.less_than_4_k += 1;
    } else if len < 8_000 {
        res.between_4_k_8_k += 1;
    } else if len < 16_000 {
        res.between_8_k_16_k += 1;
    } else if len < 32_000 {
        res.between_16_k_32_k += 1;
    } else if len < 64_000 {
        res.between_32_k_64_k += 1;
    } else if len < 128_000 {
        res.between_64_k_128_k += 1;
    } else if len < 256_000 {
        res.between_128_k_256_k += 1;
    } else if len < 512_000 {
        res.between_256_k_512_k += 1;
    } else if len < 1_000_000 {
        res.between_512_k_1_m += 1;
    } else if len < 10_000_000 {
        res.between_1_m_10_m += 1;
    } else if len < 100_000_000 {
        res.between_10_m_100_m += 1;
    } else if len < 1_000_000_000 {
        res.between_100_m_1_g += 1;
    } else {
        res.more_than_1_g += 1;
    }
    res.files += 1;
}

fn statx_supported(path: &String, verbose: bool) -> bool {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) => {
            println!(
                "the path can't be read as a directory: {:}",
                e.to_string().red()
            );
            return false;
        }
    };

    // Get the fist file from the directory
    for entry in entries {
        match entry {
            Ok(entry) => match entry.file_type() {
                Ok(t) => {
                    if t.is_dir() {
                        continue;
                    } else if t.is_file() {
                        return test_statx_on_file(path, entry, verbose);
                    }
                }
                Err(e) => {
                    println!(
                        "can't get type of file {:?} with error: {:}",
                        entry.file_name().as_os_str(),
                        e.to_string().red()
                    );
                    continue;
                }
            },
            Err(e) => {
                println!(
                    "can't get the content from the directory: {:}",
                    e.to_string().red()
                );
            }
        }
    }

    false
}

fn test_statx_on_file(path_as_str: &String, entry: fs::DirEntry, verbose: bool) -> bool {
    use rustix::fs::{cwd, openat, statx, AtFlags, Mode, OFlags, StatxFlags};
    use std::ffi::CString;

    let return_false = |message: String, verbose| -> bool {
        // Print the given message
        println!("{message:}");
        // If verbose it mention the fact that Statx is not supported on this system
        if verbose {
            println!("statx is {:} supported on this system", "NOT".red());
        }

        // Return false
        false
    };

    // Generate a CString for the directory
    let dir_c_str = match CString::new(path_as_str.as_str()) {
        Ok(cs) => cs,
        Err(e) => {
            return return_false(
                format!(
                    "can't make the directory C string: {:}",
                    e.to_string().red()
                ),
                verbose,
            );
        }
    };

    // Open the directory from the CString directory
    let dir = match openat(
        cwd(),
        &dir_c_str,
        OFlags::RDONLY | OFlags::DIRECTORY,
        Mode::empty(),
    ) {
        Ok(d) => d,
        Err(e) => {
            return return_false(
                format!("can't open file with statx lib: {:}", e.to_string().red()),
                verbose,
            );
        }
    };

    // Generate a CString for the file
    let file_c_str = match CString::new(match entry.file_name().to_str() {
        Some(s) => s,
        None => {
            return return_false(format!("can't get entry file name"), verbose);
        }
    }) {
        Ok(cs) => cs,
        Err(e) => {
            return return_false(
                format!(
                    "can't make CString from file name: {:}",
                    e.to_string().red()
                ),
                verbose,
            );
        }
    };

    // Extract statx based on the previously generated dir and file path via CString
    match statx(
        &dir,
        &file_c_str,
        AtFlags::SYMLINK_NOFOLLOW | AtFlags::STATX_DONT_SYNC,
        StatxFlags::SIZE | StatxFlags::TYPE,
    ) {
        Ok(stat) => stat,
        Err(e) => {
            return return_false(
                format!("can't get stat with statx: {:}", e.to_string().red()),
                verbose,
            );
        }
    };

    //Display to user that statx is supported on the system
    if verbose {
        println!("statx is {:} on this system", "supported".green());
    }

    true
}
