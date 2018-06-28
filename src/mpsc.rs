use std::result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel;
use futures::prelude::*;
use futures::task::AtomicTask;

pub enum Error<T> {
    Full(T),
    Disconnected(T),
}

type Result<T, M> = result::Result<T, Error<M>>;

struct State {
    task: AtomicTask,
    closed: AtomicBool,
}

pub struct Sender<T> {
    sender: crossbeam_channel::Sender<T>,
    state: Arc<State>,
    try_point: usize,
    limit: usize,
}

impl<T> Sender<T> {
    pub fn send(&mut self, msg: T) -> Result<(), T> {
        if !self.state.closed.load(Ordering::Acquire) {
            if self.try_point < self.limit {
            } else {
                let len = self.sender.len();
                if len < self.limit {
                    self.try_point = 0;
                } else {
                    return Err(Error::Full(msg));
                }
            }
            self.try_point += 1;
            self.sender.send(msg);
            self.state.task.notify();
            Ok(())
        } else {
            Err(Error::Disconnected(msg))
        }
    }

    pub fn force_send(&mut self, msg: T) {
        self.try_point += 1;
        self.sender.send(msg);
        self.state.task.notify();
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        Sender {
            sender: self.sender.clone(),
            try_point: 0,
            state: self.state.clone(),
            limit: self.limit.clone(),
        }
    }
}

pub struct Receiver<T> {
    receiver: crossbeam_channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Receiver<T> {
    /// Retrive an element. If `None` is returned, the current future
    /// is guranteed to be notified when there are new messages enqueue.
    /// 
    /// ### Panic
    /// 
    /// This function will panic if no current future is being polled.
    pub fn recv(&self) -> Option<T> {
        let msg = self.receiver.try_recv();
        if msg.is_some() {
            return msg;
        }

        self.state.task.register();

        // In case there is new message enqueue after setting task.
        self.receiver.try_recv()
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    type Error = Error<T>;

    fn poll(&mut self) -> Poll<Option<T>, Error<T>> {
        match self.recv() {
            Some(i) => Ok(Async::Ready(Some(i))),
            None => Ok(Async::NotReady),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.state.closed.store(true, Ordering::Release);
    }
}

/// A special channel used only for mpsc cases.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State {
        task: AtomicTask::default(),
        closed: AtomicBool::new(false),
    });
    let (sender, receiver) = crossbeam_channel::unbounded();
    (Sender {
        sender,
        try_point: 0,
        state: state.clone(),
        limit: capacity,
    }, Receiver {
        receiver,
        state,
    })
}

#[cfg(feature = "unstable")]
mod bench_channel {
    use test::*;

    use futures::prelude::*;
    use futures::future;
    use crossbeam_channel::*;
    use futures::sync::mpsc;
    use std::thread;

    #[bench]
    fn bench_wrapped_future(b: &mut Bencher) {
        let (mut tx, rx) = super::channel(102400);
        let t = thread::spawn(move || {
            let mut count = 0;
            future::poll_fn(|| {
                loop {
                    match rx.recv() {
                        None => {}
                        Some(t) => {
                            if t != 0 {
                                count += 1;
                            } else {
                                break;
                            }
                        }
                    }
                }
                Ok(Async::Ready(count)) as Result<_, ()>
            }).wait().unwrap()
        });
        let mut i = 0;
        b.iter(|| {
            i += 1;
            tx.send(i);
        });
        tx.send(0);
        let ts = t.join().unwrap();
        assert_eq!(ts, i);
    }

    #[bench]
    fn bench_future_unbounded(b: &mut Bencher) {
        let (tx, rx) = mpsc::unbounded();
        let t = thread::spawn(move || {
            rx.take_while(|i| Ok(*i != 0)).fold(0, |count, _| Ok(count + 1)).wait().unwrap()
        });
        let mut i = 0;
        b.iter(|| {
            i += 1;
            tx.unbounded_send(i).unwrap();
        });
        tx.unbounded_send(0).unwrap();
        let ts = t.join().unwrap();
        assert_eq!(ts, i);
    }

    #[bench]
    fn bench_bounded(b: &mut Bencher) {
        let (tx, rx) = bounded(102400);

        let t = thread::spawn(move || {
            let mut n2: usize = 0;
            loop {
                let n = rx.recv().unwrap();
                if n != 0 {
                    n2 += 1
                } else {
                    return n2;
                }
            }
        });

        let mut n1 = 0;
        b.iter(|| {
            n1 += 1;
            tx.send(1)
        });

        tx.send(0);
        let n2 = t.join().unwrap();
        assert_eq!(n1, n2);
    }

    #[bench]
    fn bench_unbounded(b: &mut Bencher) {
        let (tx, rx) = unbounded();

        let t = thread::spawn(move || {
            let mut n2: usize = 0;
            loop {
                let n = rx.recv().unwrap();
                if n != 0 {
                    n2 += 1
                } else {
                    return n2;
                }
            }
        });

        let mut n1 = 0;
        b.iter(|| {
            n1 += 1;
            tx.send(1)
        });

        tx.send(0);
        let n2 = t.join().unwrap();
        assert_eq!(n1, n2);
    }
}
