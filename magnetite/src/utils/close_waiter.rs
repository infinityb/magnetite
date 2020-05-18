use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::select;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::watch;

pub fn channel() -> (Holder, Done) {
    let (tx, rx) = watch::channel(());
    (Holder(tx), Done(rx))
}

pub struct Holder(watch::Sender<()>);

#[derive(Clone)]
pub struct Done(watch::Receiver<()>);

impl Done {
    pub async fn await_with<F>(self: Pin<&mut Self>, other: F) -> Result<F::Output, ()>
    where
        F: Future + Unpin,
    {
        self.await_with_err((), other).await
    }

    pub async fn await_with_err<F, E>(
        mut self: Pin<&mut Self>,
        err: E,
        mut other: F,
    ) -> Result<F::Output, E>
    where
        F: Future + Unpin,
    {
        loop {
            let sself: &mut Self = &mut *self;
            match select(Pin::new(&mut sself.0.next()), other).await {
                futures::future::Either::Left((Some(()), f)) => {
                    other = f;
                    continue;
                }
                futures::future::Either::Left((None, _)) => return Err(err),
                futures::future::Either::Right((other_value, _)) => return Ok(other_value),
            }
        }
    }
}

impl From<watch::Receiver<()>> for Done {
    fn from(w: watch::Receiver<()>) -> Done {
        Done(w)
    }
}

impl Future for Done {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        let sself: &mut Self = Pin::into_inner(self);
        loop {
            match Stream::poll_next(Pin::new(&mut sself.0), cx) {
                Poll::Ready(Some(())) => continue, // repoll
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
