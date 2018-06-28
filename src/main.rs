#![cfg_attr(feature = "unstable", feature(test))]

#[cfg(feature = "unstable")]
extern crate test;
extern crate crossbeam;
extern crate crossbeam_channel;
extern crate futures;
extern crate raft;
extern crate spin;
extern crate futures_timer;
extern crate futures_cpupool;
extern crate protobuf;

mod mpsc;

use mpsc::{Sender, Receiver};

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use raft::{Config, RawNode};
use raft::storage::MemStorage;
use raft::eraftpb::{Message, EntryType, ConfChange, ConfChangeType};
use spin::Mutex;
use futures::prelude::*;
use futures_timer::Delay;
use futures_cpupool::CpuPool;

#[derive(Debug)]
pub enum Error {
    Channel,
    NotLeader(u64),
}

pub type Result<T> = std::result::Result<T, Error>;
type ProposeCallback = Box<Fn(Result<()>) + Send>;

enum Msg {
    Propose {
        id: u8,
        cb: ProposeCallback,
    },
    AddPeer {
        id: u64,
        cb: ProposeCallback,
    },
    Tick,
    Raft(Message),
}


#[derive(Clone)]
struct MessageRouter {
    // store id -> channel
    // net_route: Arc<Mutex<HashMap<u64, Sender<Msg>>>>,
    // region id -> channel
    ch_route: Arc<Mutex<HashMap<u64, Sender<Msg>>>>,
    // TODO: this can be optimized as thread local variables.
    // More particularly, as a per poll pool thread cache.
    // net_cache: HashMap<u64, Sender<Msg>>,
    ch_cache: HashMap<u64, Sender<Msg>>,
    pool: CpuPool,
    phantom: PhantomData<::std::sync::mpsc::Sender<()>>,
}

impl MessageRouter {
    pub fn new(pool: CpuPool) -> MessageRouter {
        MessageRouter {
            ch_route: Arc::new(Mutex::new(HashMap::default())),
            ch_cache: HashMap::default(),
            pool,
            phantom: PhantomData,
        }
    }

    fn get_or_create_ch(&mut self, id: u64) -> Option<Sender<Msg>> {
        if let Some(ch) = self.ch_cache.get(&id) {
            return Some(ch.clone());
        }

        let peer = {
            let mut router = self.ch_route.lock();
            let v = match router.entry(id) {
                Entry::Occupied(e) => {
                    self.ch_cache.insert(id, e.get().clone());
                    return Some(e.get().clone())
                },
                Entry::Vacant(v) => v,
            };
            // TODO: check range, tombstone.
            let mut peer = Peer::new(id, vec![1, 2, 3], Duration::from_millis(200), self.pool.clone(), self.clone());
            v.insert(peer.sender.clone());
            peer
        };
        let sender = peer.sender.clone();
        self.ch_cache.insert(id, sender.clone());
        peer.start();
        self.pool.spawn(peer).forget();
        Some(sender)
    }

    fn send(&mut self, msg: Message) -> Result<()> {
        let to = msg.get_to();
        self.send_command(to, Msg::Raft(msg))
    }

    fn send_command(&mut self, to: u64, mut msg: Msg) -> Result<()> {
        if let Some(h) = self.ch_cache.get_mut(&to) {
            match h.send(msg) {
                Ok(()) => return Ok(()),
                Err(mpsc::Error::Disconnected(m)) => msg = m,
                _ => return Err(Error::Channel),
            }
        }
        self.ch_cache.remove(&to);
        if let Some(mut s) = self.get_or_create_ch(to) {
            s.send(msg).map_err(|_| Error::Channel)
        } else {
            Err(Error::Channel)
        }
    }
}

struct Peer {
    node: RawNode<MemStorage>,
    sender: Sender<Msg>,
    receiver: Receiver<Msg>,
    base_tick: Duration,
    pool: CpuPool,
    cbs: HashMap<u8, ProposeCallback>,
    cc_cb: Option<ProposeCallback>,
    router: MessageRouter,
}

impl Peer {
    fn new(id: u64, nodes: Vec<u64>, base_tick: Duration, pool: CpuPool, router: MessageRouter) -> Peer {
        let storage = MemStorage::new();
        let cfg = Config {
            // The unique ID for the Raft node.
            id,
            // The Raft node list.
            // Mostly, the peers need to be saved in the storage
            // and we can get them from the Storage::initial_state function, so here
            // you need to set it empty.
            peers: nodes,
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            // Just for log
            tag: format!("[{}]", id),
            ..Default::default()
        };
        let node = RawNode::new(&cfg, storage, vec![]).unwrap();
        let (sender, receiver) = mpsc::channel(100);
        Peer { node, sender, receiver, base_tick, pool, router, cbs: HashMap::new(), cc_cb: None }
    }
}

impl Peer {
    fn schedule_base_tick(&self) {
        let mut sender = self.sender.clone();
        self.pool.spawn(Delay::new(self.base_tick.clone()).map(move |_| {
            sender.force_send(Msg::Tick)
        })).forget()
    }

    fn start(&self) {
        self.schedule_base_tick()
    }


    fn handle_msgs<I: IntoIterator<Item=Msg>>(&mut self, ms: I) -> bool {
        let mut has_ready = false;
        for m in ms {
            match m {
                Msg::Tick => {
                    has_ready = self.node.tick() || has_ready;
                    self.schedule_base_tick();
                },
                Msg::Propose { id, cb } => {
                    if self.node.raft.leader_id == self.node.raft.id {
                        self.node.propose(vec![], vec![id]).unwrap();
                        has_ready = true;
                        self.cbs.insert(id, cb);
                        continue;
                    }

                    cb(Err(Error::NotLeader(self.node.raft.leader_id)));
                },
                Msg::AddPeer { id, cb } => {
                    if self.node.raft.leader_id == self.node.raft.id {
                        assert!(self.cc_cb.is_none());
                        let mut cc = ConfChange::new();
                        cc.set_change_type(ConfChangeType::AddNode);
                        cc.set_node_id(id);
                        self.node.propose_conf_change(vec![], cc).unwrap();
                        has_ready = true;
                        self.cc_cb = Some(cb);
                        continue;
                    }

                    cb(Err(Error::NotLeader(self.node.raft.leader_id)));
                }
                Msg::Raft(m) => {
                    self.node.step(m).unwrap();
                    has_ready = true;
                },
            }
        }
        has_ready
    }

    fn handle_ready(&mut self) {
        if !self.node.has_ready() {
            return;
        }

        // The Raft is ready, we can do something now.
        let mut ready = self.node.ready();

        let is_leader = self.node.raft.leader_id == 1;
        if is_leader {
            // If the peer is leader, the leader can send messages to other followers ASAP.
            let msgs = ready.messages.drain(..);
            for msg in msgs {
                self.router.send(msg);
            }
        }

        if !raft::is_empty_snap(&ready.snapshot) {
            // This is a snapshot, we need to apply the snapshot at first.
            self.node.mut_store()
                .wl()
                .apply_snapshot(ready.snapshot.clone())
                .unwrap();
        }

        if !ready.entries.is_empty() {
            // Append entries to the Raft log
            self.node.mut_store().wl().append(&ready.entries).unwrap();
        }

        if let Some(ref hs) = ready.hs {
            // Raft HardState changed, and we need to persist it.
            self.node.mut_store().wl().set_hardstate(hs.clone());
        }

        if !is_leader {
            // If not leader, the follower needs to reply the messages to
            // the leader after appending Raft entries.
            let msgs = ready.messages.drain(..);
            for msg in msgs {
                self.router.send(msg);
            }
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            let mut _last_apply_index = 0;
            for entry in committed_entries {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                _last_apply_index = entry.get_index();

                if entry.get_data().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                if entry.get_entry_type() == EntryType::EntryNormal {
                    if let Some(cb) = self.cbs.remove(entry.get_data().get(0).unwrap()) {
                        cb(Ok(()));
                    }
                } else {
                    let cc: ConfChange = protobuf::parse_from_bytes(entry.get_data()).unwrap();
                    match cc.get_change_type() {
                        ConfChangeType::AddNode => {
                            self.node.apply_conf_change(&cc);
                            if let Some(cb) = self.cc_cb.take() {
                                cb(Ok(()))
                            }
                        }
                        ConfChangeType::RemoveNode => {
                            if cc.get_node_id() == self.node.raft.id {
                                // self.destroy();
                            }
                        }
                        ConfChangeType::AddLearnerNode => {}
                    }
                }

                // TODO: handle EntryConfChange
            }
        }

        // Advance the Raft
        self.node.advance(ready);
    }
}

impl Future for Peer {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        let mut msgs = Vec::with_capacity(1024);
        let mut has_ready = false;
        // Potential hungry problem.
        loop {
            for _ in 0..1024 {
                msgs.push(match self.receiver.recv() {
                    Some(m) => m,
                    None => break,
                });
            }
            // TODO: Handle ready for every loop, but write to rocksdb at the end.
            if !msgs.is_empty() {
                has_ready = self.handle_msgs(msgs.drain(..)) || has_ready;
            } else {
                if has_ready {
                    self.handle_ready();
                }
                return Ok(Async::NotReady)
            }
        }
    }
}

fn main() {
    let pool = CpuPool::new(2);
    let mut router = MessageRouter::new(pool.clone());
    let peer = Peer::new(1, vec![1], Duration::from_millis(200), pool.clone(), router.clone());
    peer.start();
    pool.spawn(peer).forget();
    
    use std::thread;
    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut leader = 1;
    loop {
        let tx = tx.clone();
        router.send_command(leader, Msg::Propose { id: 1, cb: Box::new(move |res| tx.send(res.map(|_| "Log is committed")))});
        match rx.try_recv() {
            Some(Err(Error::NotLeader(id))) => {
                if id != 0 {
                    println!("Leader is elected as {}", id);
                    leader = id;
                }
            }
            Some(Err(e)) => panic!("unexpected error: {:?}", e),
            Some(Ok(s)) => {
                println!("{}", s);
                break;
            },
            None => {},
        }
        thread::sleep(Duration::from_millis(100));
    }
    for id in 2..4 {
        loop {
            let tx = tx.clone();
            router.send_command(leader, Msg::AddPeer { id, cb: Box::new(move |res| tx.send(res.map(|_| ""))) });
            match rx.recv() {
                Some(Err(Error::NotLeader(id))) => {
                    if id != 0 {
                        println!("Leader is elected as {}", id);
                        leader = id;
                    }
                }
                Some(Err(e)) => panic!("unexpected error: {:?}", e),
                Some(Ok(s)) => {
                    println!("node {} is added.", id);
                    break;
                },
                None => panic!("receiver is dropped silently"),
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    router.send_command(3, Msg::Propose { id: 3, cb: Box::new(move |res| tx.send(res.map(|_| "should failed")))});
    match rx.recv() {
        Some(Err(Error::NotLeader(_))) => println!("node 3 is working."),
        res => panic!("unexpected result: {:?}", res),
    }
}
