use core_affinity::*;
use flume::Sender;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use rayon::prelude::*;
use std::cmp::min;
use std::mem::{drop, swap};
use std::thread;
use std::thread::JoinHandle;

const DEFAULT_THREADS: usize = 4;

struct WorkerThread<T: Send> {
    taskqueue: Sender<(usize, T)>,
    handle: JoinHandle<()>
}

fn split_work_flume<T, R, F>(mut vec: Vec<T>, function: F, threshold: usize) -> Vec<R>
where T: Send + 'static, R: Send + 'static, F: Fn(T) -> R + Clone + Send + 'static {
    let len = vec.len();

    if len < threshold {
        vec.into_iter().map(function).collect()
    }
    else {
        let cores = get_core_ids();
        let nthread = min(
            len,
            if let Some(ref c) = cores { c.len() } else { DEFAULT_THREADS }
        );

        let (reply, mailbox) = flume::unbounded();
        let mut threads = Vec::with_capacity(nthread);

        for thread in 0..nthread {
            let function = function.clone();
            let reply = reply.clone();
            let (taskqueue, tasks) = flume::unbounded();
            let core = cores.as_ref().map(|c| c[thread]);

            let handle = thread::spawn(move || {
                if let Some(c) = core {
                    set_for_current(c);
                }

                for (index, item) in tasks.iter() {
                    reply.send((index, function(item))).unwrap();
                }
            });

            threads.push(WorkerThread { taskqueue, handle });
        }
        drop(reply);

        for index in (0..len).rev() {
            threads[index % nthread].taskqueue.send(
                (index, vec.pop().unwrap())
            ).unwrap();
        }

        for thread in threads {
            drop(thread.taskqueue);
            thread.handle.join().unwrap();
        }

        // Flume Iter currently lacks size_hint information.
        let mut mail = Vec::with_capacity(len);
        for tuple in mailbox {
            mail.push(tuple);
        }
        mail.sort_by_key(|t| t.0);

        mail.into_iter().map(|t| t.1).collect()
    }
}

fn split_work_rayon<T, R, F>(vec: Vec<T>, function: F, threshold: usize) -> Vec<R>
where T: Send, R: Send, F: Fn(T) -> R + Sync + Send {
    if vec.len() < threshold {
        vec.into_iter().map(function).collect()
    }
    else {
        let mut result = Vec::new();
        vec.into_par_iter().map(function).collect_into_vec(&mut result);

        result
    }
}

fn fib(num: usize) -> BigUint {
    let mut values = (BigUint::zero(), BigUint::one());

    for _ in 0..num {
        swap(&mut values.0, &mut values.1);
        values.0 += &values.1;
    }

    values.0
}

fn main() {
    println!("Initializing...");
    let v = vec![1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900];
    let demo = (v.clone(), v.clone(), v.clone(), v);
    println!("Starting demo...");

    let flume8 = split_work_flume(demo.0, fib, 8);
    println!("\nSplit work with flume on threshold 8 yields:\n{:?}", flume8);

    let flume16 = split_work_flume(demo.1, fib, 16);
    println!("\nSplit work with flume on threshold 16 yields:\n{:?}", flume16);

    let rayon8 = split_work_rayon(demo.2, fib, 8);
    println!("\nSplit work with rayon on threshold 8 yields:\n{:?}", rayon8);

    let rayon16 = split_work_rayon(demo.3, fib, 16);
    println!("\nSplit work with rayon on threshold 16 yields:\n{:?}", rayon16);
}
