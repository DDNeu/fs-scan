use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;

use crate::objects;

static OUTPUT_FILE: &str = "fs-scan_output.csv";
static FILE_FIRST_LINE: &str = "Path,Duration_ms,Files,Directories,Empty_files,Less_than_4K,4K_8K,8K_16K,16K_32K,32K_64K,64K_128K,128K_256K,256K_512K,512K_1M,1M_10M,10M_100M,100M_1G,1G";

pub fn save(res: &objects::Result) {
    match check_file() {
        Err(s) => {
            println!("ERROR on check: {}", s);
            return;
        }
        Ok(s) => println!("SUCCESS on check: {}", s),
    }

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(OUTPUT_FILE)
        .unwrap();

    let _ = writeln!(file, "{}", res.csv_line());
}

fn check_file() -> Result<String, String> {
    // Open the file
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(OUTPUT_FILE)
    {
        Err(_) => {
            // File not opened
            // Try to create it
            let file = match OpenOptions::new()
                .write(true)
                .read(true)
                .append(true)
                .create(true)
                .open(OUTPUT_FILE)
            {
                Err(_) => return Err("can't open/create new file".to_string()),
                Ok(file) => file,
            };
            file
        }
        Ok(file) => file,
    };

    match file.metadata() {
        Ok(m) => {
            if m.len() == 0 {
                if writeln!(&mut file, "{}", &String::from(FILE_FIRST_LINE)).is_err() {
                    return Err("can't write first line".to_string());
                }
                return Ok("new file created and first line added successfully".to_string());
            }
        }
        Err(_) => return Err("Can't get meta".to_string()),
    }

    // Check the first line is valid
    //
    // Read the content
    let file_content = BufReader::new(&file);
    if let Some(line) = file_content.lines().next() {
        let l = match line {
            Ok(l) => l,
            Err(e) => return Err(format!("can't read line: {}", e)),
        };
        if l != FILE_FIRST_LINE {
            return Err(format!("Not the same line: content {}", l));
        }
    }

    Ok("first line valid, can add the new report".to_string())
}
