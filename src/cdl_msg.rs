use std::{
    path::PathBuf,
    sync::{mpsc, mpsc as oneshot},
};

use eyre::Result;
use tracing::instrument;

use crate::types::*;

#[tarpc::service]
pub trait CdlMsg {
    async fn add(src: PathBuf, dst: PathBuf, me: UID, group: GID) -> Result<(), String>;
    async fn list(me: UID) -> Result<Vec<FromTo<PathBuf>>, String>;
    async fn del(
        src: Option<PathBuf>,
        dst: Option<PathBuf>,
        me: UID,
    ) -> Result<Vec<SuccOrFail>, String>;
}

#[derive(Clone, Debug)]
pub struct Server {
    pub inotify_request: mpsc::Sender<(InotifyActions, oneshot::Sender<InotifyResults>)>,
}
impl CdlMsg for Server {
    #[instrument(level = "debug")]
    async fn add(
        self,
        _context: ::tarpc::context::Context,
        src: PathBuf,
        dst: PathBuf,
        me: UID,
        group: GID,
    ) -> Result<(), String> {
        let x: Result<()> = try {
            let (tx, rx) = oneshot::channel();
            self.inotify_request.send((
                InotifyActions::Add(Record {
                    src_dst: FromTo {
                        from: src.clone(),
                        to: dst.clone(),
                    },
                    user: me,
                    group: group,
                }),
                tx,
            ))?;
            if let InotifyResults::Add(ir) = rx.recv()? {
                ir?;
            };
        };
        x.map_err(|e| format!("{e:?}"))
    }

    #[instrument(level = "debug")]
    async fn list(
        self,
        _context: ::tarpc::context::Context,
        me: UID,
    ) -> Result<Vec<FromTo<PathBuf>>, String> {
        let x: Result<_> = try {
            let (tx, rx) = oneshot::channel();
            self.inotify_request.send((InotifyActions::List, tx))?;
            if let InotifyResults::List(ir) = rx.recv()? {
                ir.into_iter().map(|r| r.src_dst).collect()
            } else {
                vec![]
            }
        };
        x.map_err(|e| format!("{e:?}"))
    }

    #[instrument(level = "debug")]
    async fn del(
        self,
        _context: ::tarpc::context::Context,
        src: Option<PathBuf>,
        dst: Option<PathBuf>,
        me: UID,
    ) -> Result<Vec<SuccOrFail>, String> {
        let ret: Result<Vec<SuccOrFail>> = try {
            let (tx, rx) = oneshot::channel();
            self.inotify_request
                .send((InotifyActions::Del(FromTo { from: src, to: dst }, me), tx))?;
            if let InotifyResults::Del(ir) = rx.recv()? {
                ir?
            } else {
                vec![]
            }
        };
        ret.map_err(|e| format!("{e:?}"))
    }
}
