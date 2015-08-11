use std::ops::{Deref, DerefMut};

use mio::buf::RingBuf;

/// This is not a very good Guard, so you must take care to not call mark().
pub struct ResetGuard<'a> {
    ringbuf: &'a mut RingBuf,
    should_reset: bool,
}

impl<'a> Deref for ResetGuard<'a> {
    type Target = RingBuf;

    fn deref(&self) -> &Self::Target {
        &self.ringbuf
    }
}

impl<'a> DerefMut for ResetGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ringbuf
    }
}

impl<'a> Drop for ResetGuard<'a> {
    fn drop(&mut self) {
        if self.should_reset {
            self.ringbuf.reset();
            self.should_reset = false;
        }
    }
}

impl<'a> ResetGuard<'a> {
    pub fn new(ringbuf: &'a mut RingBuf) -> ResetGuard<'a> {
        ringbuf.mark();
        ResetGuard {
            ringbuf: ringbuf,
            should_reset: true,
        }
    }

    pub fn commit(&mut self) {
        self.should_reset = false;
    } 
}