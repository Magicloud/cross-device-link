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
    pub db: PathBuf,
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
            // self.records
            //     .get_cloned()
            //     .await
            //     .iter()
            //     .any(|(r, _)| r.src_dst.to == dst)
            //     .not()
            //     .to_result(
            //         (),
            //         anyhow!("The target {} is already in sync", dst.to_string_lossy()),
            //     )?;

            // tracing::info!("Adding to inotify");
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

            // tracing::info!("Persisting in-mem DB");
            // let tmp = self.records.get_cloned().await;
            // let v: Vec<_> = tmp.keys().collect();
            // let v = serde_json::to_vec_pretty(&v)?;
            // tokio::fs::write(&self.db, &v).await?;
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
            // let tmp = self.records.get_cloned().await;
            // let to_dels: Vec<_> = tmp
            //     .iter()
            //     .filter(|(r, _)| r.user == me)
            //     .optional_filter(src.map(|s| {
            //         let s = s.clone();
            //         move |&(r, _): &(&Record, &FromTo<WatchDescriptor>)| r.src_dst.from == s
            //     }))
            //     .optional_filter(dst.map(|d| {
            //         let d = d.clone();
            //         move |&(r, _): &(&Record, &FromTo<WatchDescriptor>)| r.src_dst.to == d
            //     }))
            //     .map(|(a, b)| (a.clone(), b.clone()))
            //     .collect();
            // drop(tmp); // Avoid Arc being held too long to block `get_mut` below.
            // tracing::info!("Found {} records to delete.", to_dels.len());

            // let ret = to_dels
            //     .into_iter()
            //     .map(|(r, _)| {
            //         let records = self.records.clone();
            //         let i = self.inotify_request.clone();
            //         async move {
            //             let ret: Result<()> = try {
            //                 tracing::info!("Handling {r:?}");

            //                 let inotify_result: Result<()> = try {
            //                     tracing::info!("Deleting from Inotify");
            //                     let (tx, rx) = oneshot::channel();
            //                     i.send((InotifyActions::Del(r.clone()), tx)).await?;
            //                     if let InotifyResults::Del(ir) = rx.await? {
            //                         ir?;
            //                     };
            //                 };

            //                 tracing::debug!("Getting write lock");
            //                 let mut lock = records.write().await;
            //                 tracing::debug!("Got write lock");
            //                 tracing::info!("Deleting from in-mem DB");
            //                 if let Some(rs) = Arc::get_mut(&mut lock) {
            //                     rs.remove(&r);
            //                     inotify_result?;
            //                 } else {
            //                     if let Err(e) = inotify_result {
            //                         Err(anyhow!("{e:?} and records are occupied"))?;
            //                     } else {
            //                         Err(anyhow!("Records are occupied"))?;
            //                     };
            //                 }

            //                 tracing::debug!("Dropped write lock");
            //             };
            //             match ret {
            //                 Ok(()) => SuccOrFail::Succ(r.src_dst.clone()),
            //                 Err(e) => SuccOrFail::Fail {
            //                     filenames: r.src_dst.clone(),
            //                     error: format!("{e:?}"),
            //                 },
            //             }
            //         }
            //     })
            //     .collect::<JoinSet<_>>()
            //     .join_all()
            //     .await;

            // tracing::info!("Persisting in-mem DB");
            // let tmp = self.records.get_cloned().await;
            // let v: Vec<_> = tmp.keys().collect();
            // let v = serde_json::to_vec_pretty(&v)?;
            // tokio::fs::write(&self.db, &v).await?;
            // ret
        };
        ret.map_err(|e| format!("{e:?}"))
    }
}
