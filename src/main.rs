mod csv;
mod objects;

use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time;

use clap::Parser;
use indicatif::{HumanDuration, ProgressBar, ProgressStyle};
use num_cpus;

fn main() {
    let mut conf = objects::Config::parse();

    if conf.max_threads == 0 {
        conf.max_threads = num_cpus::get() * 4;
    }

    let mut res = objects::build_result(&conf.path);

    // build channel
    let (sender, receiver) = channel();

    let bar = ProgressBar::new(conf.max_threads as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{elapsed} {bar:.cyan/blue} {pos:>3}/{len:3} {msg}")
            .progress_chars("##-"),
    );

    // Start scanning at the given path
    conf.handle_dir(PathBuf::from(&conf.path), sender.clone(), &bar);

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
                if running_thread >= conf.max_threads {
                    // If it's over four times the number of CPU than the folder is saved into a queue
                    dir_queue.push(received);
                } else {
                    // No problem with too much concurrency, so let's run the scan right away
                    running_thread += 1;

                    // // Add latency to debug the display
                    // thread::sleep(time::Duration::from_millis(5));

                    conf.handle_dir(received.path, cloned_sender_again.clone(), &bar);
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

                        conf.handle_dir(dir.path, cloned_sender_again.clone(), &bar);
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

    let duration_to_display;
    let ms_dur = res.duration.as_millis();
    if ms_dur < 1000 {
        duration_to_display = ms_dur.to_string() + "ms";
    } else {
        duration_to_display = HumanDuration(res.duration).to_string();
    }
    println!("Scan took {duration_to_display}");

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
