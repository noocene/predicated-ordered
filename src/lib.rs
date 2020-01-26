#![cfg_attr(feature = "no_std", no_std)]

extern crate alloc;

use alloc::collections::binary_heap::{BinaryHeap, PeekMut};
use core::{
    cmp::Ordering,
    fmt::{self, Debug},
    pin::Pin,
    task::{Context, Poll},
};
use futures::{
    ready,
    stream::{FuturesUnordered, StreamExt},
    Future, Stream,
};
#[cfg(feature = "no_std")]
use hashbrown::HashSet;
use pin_utils::unsafe_pinned;
#[cfg(not(feature = "no_std"))]
use std::collections::HashSet;

struct OrderWrapper<T> {
    data: T,
    index: usize,
}

impl<T> PartialEq for OrderWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl<T> Eq for OrderWrapper<T> {}

impl<T> PartialOrd for OrderWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for OrderWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.index.cmp(&self.index)
    }
}

impl<T> OrderWrapper<T> {
    unsafe_pinned!(data: T);
}

impl<T> Future for OrderWrapper<T>
where
    T: Future,
{
    type Output = OrderWrapper<T::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut()
            .data()
            .as_mut()
            .poll(cx)
            .map(|output| OrderWrapper {
                data: output,
                index: self.index,
            })
    }
}

pub struct PredicatedOrdered<T: Future, P: FnMut(&T::Output) -> bool + Sync + Send> {
    in_progress_queue: FuturesUnordered<OrderWrapper<T>>,
    queued_outputs: BinaryHeap<OrderWrapper<T::Output>>,
    skipped_idxs: HashSet<usize>,
    next_incoming_index: usize,
    next_outgoing_index: usize,
    predicate: P,
}

impl<T: Future, P: FnMut(&T::Output) -> bool + Sync + Send> Unpin for PredicatedOrdered<T, P> {}

impl<Fut: Future, P: FnMut(&Fut::Output) -> bool + Sync + Send> PredicatedOrdered<Fut, P> {
    pub fn new(predicate: P) -> PredicatedOrdered<Fut, P> {
        PredicatedOrdered {
            in_progress_queue: FuturesUnordered::new(),
            queued_outputs: BinaryHeap::new(),
            next_incoming_index: 0,
            skipped_idxs: HashSet::new(),
            predicate,
            next_outgoing_index: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.in_progress_queue.len() + self.queued_outputs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.in_progress_queue.is_empty() && self.queued_outputs.is_empty()
    }
    pub fn push(&mut self, future: Fut) {
        let wrapped = OrderWrapper {
            data: future,
            index: self.next_incoming_index,
        };
        self.next_incoming_index += 1;
        self.in_progress_queue.push(wrapped);
    }
}

impl<Fut: Future, P: FnMut(&Fut::Output) -> bool + Sync + Send> Stream
    for PredicatedOrdered<Fut, P>
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.skipped_idxs.remove(&this.next_outgoing_index) {
            this.next_outgoing_index += 1;
        }
        if let Some(next_output) = this.queued_outputs.peek_mut() {
            if next_output.index == this.next_outgoing_index {
                this.next_outgoing_index += 1;
                return Poll::Ready(Some(PeekMut::pop(next_output).data));
            }
        }
        loop {
            match ready!(this.in_progress_queue.poll_next_unpin(cx)) {
                Some(output) => {
                    if (this.predicate)(&output.data) {
                        this.skipped_idxs.insert(output.index);
                        return Poll::Ready(Some(output.data));
                    } else if output.index == this.next_outgoing_index {
                        this.next_outgoing_index += 1;
                        return Poll::Ready(Some(output.data));
                    } else {
                        this.queued_outputs.push(output)
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<Fut: Future, P: FnMut(&Fut::Output) -> bool + Sync + Send> Debug
    for PredicatedOrdered<Fut, P>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredicatedOrdered {{ ... }}")
    }
}
