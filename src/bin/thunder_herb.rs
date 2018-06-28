extern crate futures_cpupool;
extern crate futures_timer;
extern crate futures;

use futures_cpupool::CpuPool;
use std::time::{Duration, Instant};
use std::sync::mpsc::{self, Sender};

use futures::Future;

fn schedule_delay(p: CpuPool, tx: Sender<()>, t: u64) {
    let p2 = p.clone();
    p.spawn(futures_timer::Delay::new(Duration::from_secs(t)).map(move |_| {
        tx.send(()).unwrap();
        schedule_delay(p2, tx, t)
    })).forget()
}

fn main() {
    let pool = futures_cpupool::Builder::new().name_prefix("poll").pool_size(2).create();
    let (tx, rx) = mpsc::channel();
    let t = Instant::now();
    for _ in 0..100000 {
        let p = pool.clone();
        let t = tx.clone();
        schedule_delay(p, t, 1);
    }
    println!("schedule takes {:?}", t.elapsed());
    for _ in 0..6000000 {
        rx.recv().unwrap();
    }
}
