use std::collections::BTreeMap;
use std::convert::TryInto;
use std::env;
use std::fs::File;

use storethehash::index::{self, IndexIter};
use storethehash::recordlist::{RecordList, BUCKET_PREFIX_SIZE};

fn index_stats(index_path: &str) -> BTreeMap<u32, Vec<usize>> {
    let mut stats = BTreeMap::new();

    let index_file = File::open(&index_path).unwrap();
    let index_file = io_streams::StreamWriter::file(index_file);
    // Skip the header
    let (_header, bytes_read) = index::read_header(&index_file).unwrap();

    for entry in IndexIter::new(&index_file, bytes_read) {
        match entry {
            Ok((data, _pos)) => {
                let bucket = u32::from_le_bytes(data[..BUCKET_PREFIX_SIZE].try_into().unwrap());

                let recordlist = RecordList::new(&data);
                let keys_length: Vec<usize> = recordlist
                    .into_iter()
                    .map(|record| record.key.len())
                    .collect();

                stats.insert(bucket, keys_length);
            }
            Err(error) => panic!("{}", error),
        }
    }
    stats
}

fn main() {
    fil_logger::init();
    let mut args = env::args().skip(1);
    let index_path_arg = args.next();
    match index_path_arg {
        Some(index_path) => {
            let stats = index_stats(&index_path);
            println!("{}", serde_json::to_string(&stats).unwrap());
        }
        _ => println!("usage: fromcarfile <path-to-car-file> <index-file>"),
    }
}
