use std::{
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use rand::distributions::Alphanumeric;
use rand::prelude::Distribution;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, ReadOptions, WriteOptions, DB};

static mut COUNT: AtomicU64 = AtomicU64::new(0);
static mut TOTAL: AtomicU64 = AtomicU64::new(0);
static mut TIME: AtomicU64 = AtomicU64::new(0);

fn new_db() -> DB {
    let mut db_opts = Options::default();
    db_opts.set_atomic_flush(true);
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.enable_statistics();
    db_opts.set_stats_dump_period_sec(10);
    db_opts.set_stats_persist_period_sec(10);
    // Threshold of all memtable across column families added in size
    db_opts.set_db_write_buffer_size(1024 * 1024 * 1024);
    db_opts.set_max_background_jobs(2);

    db_opts.set_enable_blob_files(true);
    db_opts.set_min_blob_size(1024);
    db_opts.set_enable_blob_gc(false);

    let index_cf = ColumnFamilyDescriptor::new("index", db_opts.clone());

    DB::open_cf_descriptors(&db_opts, "/data/store", vec![index_cf]).unwrap()
}

fn random_bytes(len: usize) -> Box<[u8]> {
    Alphanumeric
        .sample_iter(&mut rand::thread_rng())
        .take(len)
        .collect()
}

fn write_message(db: Arc<DB>, queue_id: u32) {
    let tick: Instant = Instant::now();
    let mut count = 0;
    let index_cf = db.cf_handle("index").unwrap();

    let mut write_opts = WriteOptions::default();
    // write_opts.disable_wal(true);
    // write_opts.set_sync(false);
    loop {
        if tick.elapsed() >= Duration::from_secs(60) {
            break;
        }
        let queue_index = format!(
            "/%RETRY%longlonglonglongtopicname_longlonglonglonggroup_name/{}/{}",
            queue_id, count
        );
        count += 1;
        let message = random_bytes(4 * 1024);

        let instant = Instant::now();
        // write message
        db.put_opt(queue_index.clone(), message, &write_opts)
            .unwrap();
        // write message key for index
        db.put_cf_opt(
            index_cf,
            random_bytes(256),
            queue_index.clone(),
            &write_opts,
        )
        .unwrap();
        db.put_cf_opt(
            index_cf,
            random_bytes(256),
            queue_index.clone(),
            &write_opts,
        )
        .unwrap();
        db.put_cf_opt(
            index_cf,
            random_bytes(256),
            queue_index.clone(),
            &write_opts,
        )
        .unwrap();
        db.put_cf_opt(
            index_cf,
            random_bytes(256),
            queue_index.clone(),
            &write_opts,
        )
        .unwrap();
        // db.flush().unwrap();
        unsafe {
            COUNT.fetch_add(1, Ordering::Relaxed);
            TIME.fetch_add(instant.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
    }
}

fn read_message(db: Arc<DB>) {
    let mut instant = Instant::now();
    let mut options = ReadOptions::default();
    options
        .set_iterate_lower_bound("/%RETRY%longlonglonglongtopicname_longlonglonglonggroup_name/0/");
    options
        .set_iterate_upper_bound("/%RETRY%longlonglonglongtopicname_longlonglonglonggroup_name/1");
    db.iterator_opt(IteratorMode::Start, options)
        .for_each(|result| {
            let (_, value) = result.unwrap();
            // assert_eq!(4 * 1024, value.len());
            unsafe {
                COUNT.fetch_add(1, Ordering::Relaxed);
                TIME.fetch_add(instant.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
            instant = Instant::now();
        })
}

fn main() {
    let db = Arc::new(new_db());
    let mut hanles = vec![];

    thread::spawn(|| {
        let mut instant = Instant::now();

        loop {
            unsafe {
                if instant.elapsed() >= Duration::from_secs(1) {
                    let count = COUNT.load(Ordering::Relaxed) as f64;
                    let time = TIME.load(Ordering::Relaxed) as f64;
                    COUNT.store(0, Ordering::Relaxed);
                    TIME.store(0, Ordering::Relaxed);
                    TOTAL.fetch_add(count as u64, Ordering::Relaxed);

                    println!("read/wite {}MB/s", 4.0 * count / 1024.0);
                    println!("read/wite latency {}us", time / count);
                    instant = Instant::now();
                }
            }
        }
    });

    for i in 0..4 {
        let db = db.clone();
        hanles.push(thread::spawn(move || write_message(db, i)));
    }

    for handle in hanles {
        handle.join().unwrap();
    }

    unsafe {
        let count = COUNT.load(Ordering::Relaxed);
        TOTAL.fetch_add(count, Ordering::Relaxed);
        COUNT.store(0, Ordering::Relaxed);
        TIME.store(0, Ordering::Relaxed);

        println!(
            "wite complete, totally write {}GB",
            TOTAL.load(Ordering::Relaxed) * 4 / 1024 / 1024
        );
    }

    db.flush().unwrap();

    Command::new("sh")
        .arg("-c")
        .arg("echo 3 > /proc/sys/vm/drop_caches")
        .output()
        .expect("failed to execute process");

    read_message(db.clone());
    println!("read complete");
    // sleep 1s for stats dump
    thread::sleep(Duration::from_secs(1));
}
