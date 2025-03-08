use std::cmp::min;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use crossbeam::epoch;
use crossbeam::epoch::{Atomic, Collector, Owned};
use crossbeam::utils::{Backoff, CachePadded};
use rand::distributions::Alphanumeric;
use rand::Rng;

fn main() {
    let map = Arc::new(ConcurrentMap::new(8));

    let collector = Collector::new();
    collector.register();

    let start = Instant::now();
    // Writers
    let mut workers = Vec::new();
    (0..32).for_each(|idx| {
        let worker = thread::spawn({
            let map = Arc::clone(&map);
            move || {
                (0..1000000).for_each(|idx| {
                    let rng = rand::thread_rng();
                    let value: String = rng
                        .sample_iter(&Alphanumeric)
                        .take(15)
                        .map(char::from)
                        .collect();
                    map.insert(value.clone(), value.clone());
                });
            }
        });

        workers.push(worker);
    });


    loop {
        match workers.pop() {
            None => { break; }
            Some(worker) => {
                worker.join();
            }
        }
    }

    let end = start.elapsed();
    println!("{}", end.as_millis());
}


#[derive(Debug)]
struct ConcurrentMap<K, V>
where
    K: Hash + Clone + PartialEq,
    V: Clone,
{
    head: Bucket<K, V>,
    bucket_capacity: u64,
}


impl<K: Hash + Clone + PartialEq, V: Clone> ConcurrentMap<K, V> {
    pub fn new(bucket_capacity: u64) -> Self {
        assert_eq!(bucket_capacity % 2, 0);
        Self {
            head: Bucket::new(bucket_capacity),
            bucket_capacity: min(bucket_capacity, 64),
        }
    }

    // Add deep look
    pub fn get<F>(&self, key: &K, f: F)
    where
        F: Fn(&V) -> (),
    {
        let slot = self.head.get_slot_unchecked(key);
        let guard = epoch::pin();
        let value = slot.value.load(Ordering::Relaxed, &guard);
        if value.is_null() {
            return;
        }

        let value_ref = &unsafe { value.as_ref().unwrap_unchecked() }.1;
        f(value_ref);
    }


    pub fn insert(&self, key: K, value: V) {
        let guard = epoch::pin();
        let mut current_bucket = &self.head;
        let mut slot_ref: &Slot<K, V> = current_bucket.get_slot_unchecked(&key);
        loop {
            let mut value_ref = slot_ref.value.load(Ordering::Acquire, &guard);
            if value_ref.is_null() {
                let new_value = Owned::new((key.clone(), value.clone()));
                let result = slot_ref.value.compare_exchange(value_ref, new_value, Ordering::Relaxed
                                                             , Ordering::Acquire, &guard);
                if let Err(val) = result {
                    value_ref = val.current;
                } else {
                    break;
                }
            }

            let current_value = unsafe { value_ref.as_ref().unwrap_unchecked() };
            if current_value.0.eq(&key) {
                let new_value = Owned::new((key.clone(), value.clone()));
                slot_ref.value.store(new_value, Ordering::Release);
                break;
            }

            let mut bucket_ref = slot_ref.bucket.load(Ordering::Acquire, &guard);
            if bucket_ref.is_null() {
                let current_slot_id = 1 << slot_ref.id;
                let previous_mask = current_bucket.bucket_init_mask.fetch_or(current_slot_id, Ordering::SeqCst);
                if previous_mask & current_slot_id != 0 {
                    let backoff = Backoff::new();
                    loop {
                        let new_bucket_ref = slot_ref.bucket.load(Ordering::Acquire, &guard);
                        if !new_bucket_ref.is_null() {
                            bucket_ref = new_bucket_ref;
                            break;
                        }
                        backoff.snooze();
                    }
                } else {
                    let new_bucket = Owned::new(Bucket::new(self.bucket_capacity));
                    let _ = slot_ref.bucket.store(new_bucket, Ordering::Release);
                    bucket_ref = slot_ref.bucket.load(Ordering::Acquire, &guard);
                }
            }

            current_bucket = unsafe { bucket_ref.as_ref().unwrap_unchecked() };
            slot_ref = current_bucket.get_slot_unchecked(&key);
        }
    }
}


#[derive(Debug)]
struct Bucket<K, V>
where
    K: Hash + Clone + PartialEq,
    V: Clone,
{
    slots: Vec<CachePadded<Slot<K, V>>>,
    capacity: u64,
    index_mask: u64,
    bucket_init_mask: AtomicU64,
    hasher: RandomState,
}

impl<K: Hash + Clone + PartialEq, V: Clone> Bucket<K, V> {
    pub fn new(capacity: u64) -> Self {
        let slots = (1..=capacity)
            .map(|idx| CachePadded::new(Slot::new(idx)))
            .collect();
        let hasher = RandomState::default();
        Self {
            slots,
            capacity,
            index_mask: capacity - 1,
            bucket_init_mask: Default::default(),
            hasher,
        }
    }


    pub fn get_slot_unchecked(&self, key: &K) -> &Slot<K, V> {
        let bucket_index = (self.hasher.hash_one(key) & self.index_mask) as usize;
        unsafe { self.slots.get_unchecked(bucket_index) }
    }
}

#[derive(Debug)]
struct Slot<K, V>
where
    K: Hash + Clone + PartialEq,
    V: Clone,
{
    id: u64,
    value: Atomic<(K, V)>,
    bucket: Atomic<Bucket<K, V>>,
}

impl<K, V> Slot<K, V>
where
    K: Hash + Clone + PartialEq,
    V: Clone,
{
    pub fn new(id: u64) -> Self {
        Self {
            id,
            value: Atomic::null(),
            bucket: Atomic::null(),
        }
    }
}


