use std::{collections::HashMap, path::PathBuf, sync::Arc};

use eyre::{Result, anyhow};
use inotify::WatchDescriptor;
use iter_opt_filter::IteratorOptionalFilterExt;
use tokio::{
    process::Command,
    sync::{RwLock, mpsc, oneshot},
    task::JoinSet,
};
use tracing::instrument;

use crate::{traits::*, types::*};

#[tarpc::service]
pub trait CdlMsg {
    async fn add(src: PathBuf, dst: PathBuf, me: u32, group: u32) -> Result<(), String>;
    async fn list(me: u32) -> Result<Vec<FromTo<PathBuf>>, String>;
    async fn del(
        src: Option<PathBuf>,
        dst: Option<PathBuf>,
        me: u32,
    ) -> Result<Vec<SuccOrFail>, String>;
}

#[derive(Clone, Debug)]
pub struct Server {
    pub db: PathBuf,
    pub inotify_request: mpsc::Sender<(InotifyActions, oneshot::Sender<InotifyResults>)>,
    // Arcs
    // 1st is because RwLock is not clonable while Server should be, required by tarpc
    // 2nd is for get_cloned to have fast read access
    pub records: Arc<RwLock<Arc<HashMap<Record, FromTo<WatchDescriptor>>>>>,
}
impl CdlMsg for Server {
    #[instrument(level = "debug")]
    async fn add(
        self,
        _context: ::tarpc::context::Context,
        src: PathBuf,
        dst: PathBuf,
        me: u32,
        group: u32,
    ) -> Result<(), String> {
        let x: Result<()> = try {
            if self
                .records
                .get_cloned()
                .await
                .iter()
                .any(|(r, _)| r.src_dst.to == dst)
            {
                Err(anyhow!(
                    "The target {} is already in sync",
                    dst.to_string_lossy()
                ))?
            }

            tracing::info!("Syncing file");
            Command::new("cp")
                .uid(me)
                .gid(group)
                .args(["-f", &src.to_string_lossy(), &dst.to_string_lossy()])
                .status()
                .await?
                .success()
                .to_result((), anyhow!("Failed to sync the file"))?;

            tracing::debug!("Getting write lock");
            let mut lock = self.records.write().await;
            tracing::debug!("Got write lock");
            let r = Arc::get_mut(&mut *lock).ok_or(anyhow!("Records are occupied"))?;
            let (tx, rx) = oneshot::channel();

            tracing::info!("Adding to inotify");
            self.inotify_request
                .send((
                    InotifyActions::Add(FromTo {
                        from: src.clone(),
                        to: dst.clone(),
                    }),
                    tx,
                ))
                .await?;
            let x = rx.await?;
            if let InotifyResults::Add(ir) = x {
                let FromTo { from, to } = ir?;
                tracing::info!("Adding to in-mem DB");
                r.insert(
                    Record {
                        src_dst: FromTo { from: src, to: dst },
                        user: me,
                        group,
                    },
                    FromTo { from, to },
                );
            };
            drop(lock);
            tracing::debug!("Dropped write lock");

            tracing::info!("Persisting in-mem DB");
            let mut csv = csv::Writer::from_path(self.db)?;
            self.records
                .get_cloned()
                .await
                .keys()
                .map(|r| csv.serialize(r).map_err(|e| anyhow!("{e:?}")))
                .collect::<Result<Vec<_>>>()?;
        };
        x.map_err(|e| format!("{e:?}"))
    }

    #[instrument(level = "debug")]
    async fn list(
        self,
        _context: ::tarpc::context::Context,
        me: u32,
    ) -> Result<Vec<FromTo<PathBuf>>, String> {
        let x: Result<Vec<FromTo<PathBuf>>> = try {
            self.records
                .get_cloned()
                .await
                .iter()
                .filter_map(|(r, _)| {
                    if r.user == me {
                        Some(r.src_dst.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        x.map_err(|e| format!("{e:?}"))
    }

    #[instrument(level = "debug")]
    async fn del(
        self,
        _context: ::tarpc::context::Context,
        src: Option<PathBuf>,
        dst: Option<PathBuf>,
        me: u32,
    ) -> Result<Vec<SuccOrFail>, String> {
        let ret: Result<Vec<SuccOrFail>> = try {
            let tmp = self.records.get_cloned().await;
            let to_dels: Vec<_> = tmp
                .iter()
                .filter(|(r, _)| r.user == me)
                .optional_filter(src.map(|s| {
                    let s = s.clone();
                    move |&(r, _): &(&Record, &FromTo<WatchDescriptor>)| r.src_dst.from == s
                }))
                .optional_filter(dst.map(|d| {
                    let d = d.clone();
                    move |&(r, _): &(&Record, &FromTo<WatchDescriptor>)| r.src_dst.to == d
                }))
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            drop(tmp); // Avoid Arc being held too long to block `get_mut` below.
            tracing::info!("Found {} records to delete.", to_dels.len());

            let ret = to_dels
                .into_iter()
                .map(|(r, _)| {
                    let records = self.records.clone();
                    let i = self.inotify_request.clone();
                    async move {
                        let ret: Result<()> = try {
                            tracing::info!("Handling {r:?}");
                            tracing::info!("Deleting from Inotify");
                            let (tx, rx) = oneshot::channel();
                            i.send((InotifyActions::Del(r.clone()), tx)).await?;
                            if let InotifyResults::Del(ir) = rx.await? {
                                ir?;
                            };

                            // tokio::fs::remove_file(y.dst.clone()).await?;
                            tracing::info!("Deleting from in-mem DB");
                            tracing::debug!("Getting write lock");
                            let mut lock = records.write().await;
                            tracing::debug!("Got write lock");
                            Arc::get_mut(&mut lock)
                                .ok_or(anyhow!("Records are occupied"))?
                                .remove(&r);
                            drop(lock);
                            tracing::debug!("Dropped write lock");
                        };
                        match ret {
                            Ok(()) => SuccOrFail::Succ(r.src_dst.clone()),
                            Err(e) => SuccOrFail::Fail {
                                filenames: r.src_dst.clone(),
                                error: format!("{e:?}"),
                            },
                        }
                    }
                })
                .collect::<JoinSet<_>>()
                .join_all()
                .await;

            tracing::info!("Persisting in-mem DB");
            let mut csv = csv::Writer::from_path(self.db)?;
            self.records
                .get_cloned()
                .await
                .keys()
                .map(|r| csv.serialize(r).map_err(|e| anyhow!("{e:?}")))
                .collect::<Result<Vec<_>>>()?;
            ret
        };
        ret.map_err(|e| format!("{e:?}"))
    }
}
