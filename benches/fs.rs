use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use rand::RngCore;
use storethehash::{db::Db, index::IndexMemoryStorage};
use storethehash_primary_inmemory::InMemory;

const BUCKETS_BITS: u8 = 24;

pub fn criterion_benchmark(c: &mut Criterion) {
    let primary_storage = InMemory::new(&[]);
    let db = Db::<_, IndexMemoryStorage<BUCKETS_BITS>, BUCKETS_BITS>::new(
        primary_storage,
        IndexMemoryStorage::new(),
    )
    .unwrap();

    let mut group = c.benchmark_group("inmemory");
    for (key_size, value_size) in [(32, 128), (32, 512)] {
        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("put", format!("{}-{}", key_size, value_size)),
            &(key_size, value_size),
            |b, &(key_size, value_size)| {
                let mut rng = rand::thread_rng();

                b.iter_custom(|iters| {
                    let data = (0..iters)
                        .map(|_| {
                            let mut key = vec![0u8; key_size];
                            let mut value = vec![0u8; value_size];
                            rng.fill_bytes(&mut key);
                            rng.fill_bytes(&mut value);
                            (key, value)
                        })
                        .collect::<Vec<_>>();

                    let start = Instant::now();
                    for i in 0..iters {
                        black_box(db.put(&data[i as usize].0, &data[i as usize].1).unwrap());
                    }
                    start.elapsed()
                });
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
