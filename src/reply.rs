use std::{
    error::Error,
    fmt::{Debug, Display},
    sync::mpsc::{self, SendError},
};

#[derive(Debug)]
pub enum SenderSideError<Q> {
    SendError(Q),
    GetReplyErrorNotSentYet,
    GetReplyErrorNotRepliedYet,
    GetReplyErrorDisconnected,
}
impl<Q> From<oneshot::TryRecvError> for SenderSideError<Q> {
    fn from(value: oneshot::TryRecvError) -> Self {
        match value {
            oneshot::TryRecvError::Empty => SenderSideError::GetReplyErrorNotRepliedYet,
            oneshot::TryRecvError::Disconnected => SenderSideError::GetReplyErrorDisconnected,
        }
    }
}
impl<Q> Display for SenderSideError<Q>
where
    Q: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}
impl<Q> Error for SenderSideError<Q> where Q: Debug {}
pub struct Sender<Q, A> {
    question: mpsc::Sender<(Q, oneshot::Sender<A>)>,
    answer: Option<oneshot::Receiver<A>>,
}
impl<Q, A> Sender<Q, A> {
    pub fn send(&mut self, t: Q) -> Result<(), SenderSideError<Q>> {
        let (s, r) = oneshot::channel();
        if let Err(SendError((a, _))) = self.question.send((t, s)) {
            Err(SenderSideError::SendError(a))
        } else {
            self.answer = Some(r);
            Ok(())
        }
    }

    pub fn try_get_reply(&mut self) -> Result<A, SenderSideError<Q>> {
        let a = self
            .answer
            .as_ref()
            .ok_or(SenderSideError::GetReplyErrorNotSentYet)?;
        let ret = a.try_recv();
        if let Err(oneshot::TryRecvError::Empty) = ret {
            ();
        } else {
            self.answer = None;
        };
        ret.map_err(|e| e.into())
    }
}

#[derive(Debug)]
pub enum ReceiverSideError<A> {
    TryRecvErrorNotSentYet,
    TryRecvErrorDisconnected,
    NotRecvedYet,
    ReplyError(A),
}
impl<A> From<mpsc::TryRecvError> for ReceiverSideError<A> {
    fn from(value: mpsc::TryRecvError) -> Self {
        match value {
            mpsc::TryRecvError::Empty => ReceiverSideError::TryRecvErrorNotSentYet,
            mpsc::TryRecvError::Disconnected => ReceiverSideError::TryRecvErrorDisconnected,
        }
    }
}
pub struct Receiver<Q, A> {
    question: mpsc::Receiver<(Q, oneshot::Sender<A>)>,
    answer: Option<oneshot::Sender<A>>,
}
impl<Q, A> Receiver<Q, A> {
    pub fn try_recv(&mut self) -> Result<Q, ReceiverSideError<A>> {
        match self.question.try_recv() {
            Ok((q, a)) => {
                self.answer = Some(a);
                Ok(q)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn reply(&mut self, t: A) -> Result<(), ReceiverSideError<A>> {
        let a = self.answer.take().ok_or(ReceiverSideError::NotRecvedYet)?;
        a.send(t)
            .map_err(|e| ReceiverSideError::ReplyError(e.into_inner()))?;
        // No need to restore self.answer to Some(a).
        // If it succeeded, `a` is useless.
        // If it failed, which (per doc) means receiver has been dropped, `a` is also useless.
        Ok(())
    }
}

pub fn channel<Q, A>() -> (Sender<Q, A>, Receiver<Q, A>) {
    let (s, r) = mpsc::channel();
    (
        Sender {
            question: s,
            answer: None,
        },
        Receiver {
            question: r,
            answer: None,
        },
    )
}
