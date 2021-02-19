mod csv;
mod objects;

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time;

use indicatif::{HumanDuration, ProgressBar, ProgressStyle};
use num_cpus;

fn main() {
    let mut path = PathBuf::from(".");
    let args: Vec<_> = env::args().collect();
    if args.len() > 1 {
        println!("The path is {}", args[1]);
        path = PathBuf::from(&args[1]);
    };

    let mut res;
    match &path.to_str() {
        Some(path_as_string) => res = objects::build_result(path_as_string),
        None => res = objects::build_result(""),
    }

    // build channel
    let (sender, receiver) = channel();
    let saved_num_cpu = num_cpus::get() * 4;

    let bar = ProgressBar::new(saved_num_cpu as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{elapsed} {bar:.cyan/blue} {pos:>3}/{len:3} {msg}")
            .progress_chars("##-"),
    );

    // Start scanning at the given path
    handle_dir(path, sender.clone(), &bar);

    let cloned_sender_again = sender.clone();
    let mut running_thread = 0;
    let mut dir_queue = Vec::new();

    let starting_point = time::Instant::now();

    let display_refresh_time = time::Duration::from_millis(250);
    let mut last_message = time::Instant::now()
        .checked_sub(display_refresh_time.clone())
        .expect("to remove some time");

    // Handle responses
    for received in receiver {
        //  Limit the display refresh
        let dur = time::Instant::now().duration_since(last_message);
        if dur > display_refresh_time {
            bar.set_message(&format!(
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
                if running_thread >= saved_num_cpu {
                    // If it's over four times the number of CPU than the folder is saved into a queue
                    dir_queue.push(received);
                } else {
                    // No problem with too much concurrency, so let's run the scan right away
                    running_thread += 1;

                    // // Add latency to debug the display
                    // thread::sleep(time::Duration::from_millis(5));

                    handle_dir(received.path, cloned_sender_again.clone(), &bar);
                }
            }
            // If this signal a directory scan terminated
            objects::ResponseType::DoneDir => {
                // The process is done
                // Break the loop to display the results
                if running_thread == 0 {
                    bar.set_message(&format!("Total file scanned {}", &res.files));
                    break;
                }
                match dir_queue.pop() {
                    Some(dir) => {
                        // // Add latency to debug the display
                        // thread::sleep(time::Duration::from_millis(5));

                        handle_dir(dir.path, cloned_sender_again.clone(), &bar);
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

    csv::save(&res);

    println!("Scan took {}", HumanDuration(res.duration));
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

fn nice_number(input: usize) -> String {
    if input < 1_000 {
        return format!("{:?}", input);
    } else if input < 1_000_000 {
        return format!("{:?}K ({:?})", input / 1_000, input);
    } else {
        return format!("{:?}M ({:?})", input / 1_000_000, input);
    }
}

fn handle_dir(path: PathBuf, ch: Sender<objects::ChanResponse>, bar: &ProgressBar) {
    match fs::read_dir(&path) {
        Ok(entries) => {
            let ch = ch.clone();
            let bar = bar.clone();
            thread::spawn(move || {
                for entry in entries {
                    match entry {
                        Ok(entry) => match entry.metadata() {
                            Ok(metadata) => {
                                if metadata.is_dir() {
                                    let ch = ch.clone();
                                    ch.send(objects::build_dir_chan(entry.path())).unwrap();
                                } else if metadata.is_file() {
                                    ch.send(objects::build_file_chan(metadata.len())).unwrap();
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
                ch.send(objects::build_dir_chan_done()).unwrap();
            });
        }
        Err(err) => {
            bar.println(format!("warning 0 {} {:?}", err, &path));
            // Notify the end of the thread
            ch.send(objects::build_dir_chan_done()).unwrap();
        }
    }
}

fn handle_file(len: u64, res: &mut objects::Result) {
    if len == 0 {
        res.empty_file = res.empty_file + 1;
    } else if len < 4_000 {
        res.less_than_4_k = res.less_than_4_k + 1;
    } else if len < 8_000 {
        res.between_4_k_8_k = res.between_4_k_8_k + 1;
    } else if len < 16_000 {
        res.between_8_k_16_k = res.between_8_k_16_k + 1;
    } else if len < 32_000 {
        res.between_16_k_32_k = res.between_16_k_32_k + 1;
    } else if len < 64_000 {
        res.between_32_k_64_k = res.between_32_k_64_k + 1;
    } else if len < 128_000 {
        res.between_64_k_128_k = res.between_64_k_128_k + 1;
    } else if len < 256_000 {
        res.between_128_k_256_k = res.between_128_k_256_k + 1;
    } else if len < 512_000 {
        res.between_256_k_512_k = res.between_256_k_512_k + 1;
    } else if len < 1_000_000 {
        res.between_512_k_1_m = res.between_512_k_1_m + 1;
    } else if len < 10_000_000 {
        res.between_1_m_10_m = res.between_1_m_10_m + 1;
    } else if len < 100_000_000 {
        res.between_10_m_100_m = res.between_10_m_100_m + 1;
    } else if len < 1_000_000_000 {
        res.between_100_m_1_g = res.between_100_m_1_g + 1;
    } else {
        res.more_than_1_g = res.more_than_1_g + 1;
    }
    res.files = res.files + 1;
}
