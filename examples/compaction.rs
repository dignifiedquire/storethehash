use std::convert::TryFrom;
use std::env;
use std::fs::OpenOptions;
use std::io::{self, SeekFrom};

use log::{debug, info};

use storethehash::index::{self, Index, IndexFileStorage};
use storethehash_primary_inmemory::InMemory;
use system_interface::fs::FileIoExt;

const BUCKETS_BITS: u8 = 24;

/// Returns the lowest offset of the index that is referred to in the buckets.
fn get_lowest_file_offset(index_path: &str) -> u64 {
    let primary_storage = InMemory::new(&[]);
    let index =
        Index::<_, IndexFileStorage<BUCKETS_BITS>, BUCKETS_BITS>::open(index_path, primary_storage)
            .unwrap();
    let offsets = index.offsets();
    let lowest_file_offset = offsets.iter().min().unwrap();
    info!(
        "Lowest file offset of the index that is referred to in the buckets is: {}",
        lowest_file_offset
    );
    *lowest_file_offset
}

fn compaction(index_path: &str) {
    let lowest_file_offset = get_lowest_file_offset(index_path);

    let index_file = OpenOptions::new().read(true).open(index_path).unwrap();
    let index_file_writer = io_streams::StreamWriter::file(index_file.try_clone().unwrap());
    let index_file_reader = io_streams::StreamReader::file(index_file.try_clone().unwrap());

    let compacted_path = format!("{}{}", index_path, ".compacted");
    info!("Compacted file path: {}", compacted_path);
    // Overwrite any existing compacted file.
    let mut compacted_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(compacted_path)
        .unwrap();

    // Copy the header.
    let (_header, bytes_read_usize) = index::read_header(&index_file_writer).unwrap();
    let bytes_read = u64::try_from(bytes_read_usize).expect("64-bit platform needed");
    index_file_reader.seek(SeekFrom::Start(0)).unwrap();
    let mut header_bytes = vec![0u8; bytes_read as usize];
    index_file_reader.read_exact(&mut header_bytes).unwrap();

    debug!("Copy {} header bytes.", bytes_read);
    compacted_file.write_all(&header_bytes).unwrap();

    // Copy the actual contents.
    index_file_reader
        .seek(SeekFrom::Start(lowest_file_offset))
        .unwrap();
    let mut index_file_buffer = std::io::BufReader::new(index_file_reader);
    debug!("Copy contents.");
    io::copy(&mut index_file_buffer, &mut compacted_file).unwrap();
    debug!("Compation done.");
}

fn main() {
    fil_logger::init();
    let mut args = env::args().skip(1);
    let index_path_arg = args.next();
    match index_path_arg {
        Some(index_path) => {
            compaction(&index_path);
        }
        _ => println!("usage: compaction <index-file>"),
    }
}
