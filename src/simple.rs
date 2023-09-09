
fn main() {
    let db = new_db();
    let mut rng = rand::thread_rng();

    let mut instant = Instant::now();
    let mut count = 0.0;
    loop {
        if instant.elapsed() >= Duration::from_secs(1) {
            println!("write {}MB/s", 32.0 * count / 1024.0);
            count = 0.0;
            instant = Instant::now();
        }
        let key: Box<[u8]> = Alphanumeric
            .sample_iter(&mut rng)
            .take(7)
            .collect();
        let value: Box<[u8]> = Alphanumeric
            .sample_iter(&mut rng)
            .take(1024 * 32)
            .collect();
        db.put(key.clone(), value.clone()).unwrap();
        count += 1.0;
    }
}