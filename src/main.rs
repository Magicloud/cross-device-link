#![feature(try_blocks)]

use clap::*;
use eyre::{Result, anyhow};
use futures::{
    StreamExt,
    future::{ready, try_join_all},
};
use inotify::{Inotify, WatchDescriptor, WatchMask};
use iter_opt_filter::IteratorOptionalFilterExt;
use std::{
    collections::HashSet,
    io::ErrorKind,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tarpc::{
    client, context,
    server::{self, Channel},
};
use tokio::{
    fs,
    process::Command,
    signal,
    sync::{OnceCell, RwLock, mpsc, oneshot},
    task::JoinSet,
    time::sleep,
};
use tokio_serde::formats::Bincode;
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};

static SRC_MASK: OnceCell<WatchMask> = OnceCell::const_new();
static DST_MASK: OnceCell<WatchMask> = OnceCell::const_new();

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::FULL))
        .with(ErrorLayer::default())
        .try_init()?;
    color_eyre::install()?;

    tracing::debug!("Init app");
    SRC_MASK
        .set(WatchMask::MODIFY | WatchMask::CREATE | WatchMask::DELETE_SELF | WatchMask::MOVE_SELF)
        .map_err(|e| anyhow!("{e:?}"))?;
    DST_MASK
        .set(WatchMask::MODIFY | WatchMask::DELETE_SELF | WatchMask::MOVE_SELF)
        .map_err(|e| anyhow!("{e:?}"))?;
    let cli = Cli::parse();

    match cli.cmd {
        SubCmd::Serv => {
            tracing::debug!("Server starts");
            match fs::try_exists(&cli.db).await {
                Ok(true) => (),                              // file exists
                Ok(false) => fs::write(&cli.db, b"").await?, // file not exist, create empty
                Err(e) => Err(e)?,                           // cannot check
            }

            let mut tasks = Vec::new();

            let mut inotify = Inotify::init()?;

            tracing::debug!("Load records");
            let records = csv::Reader::from_path(&cli.db)?
                .deserialize()
                .collect::<Result<HashSet<Record>, csv::Error>>()?
                .into_iter()
                .map(
                    async |Record {
                               src,
                               dst,
                               user,
                               group,
                           }| try {
                        Arc::new(RecordWithWatchDescriptor {
                            src: src.clone(),
                            src_watcher: inotify.watches().add(&src, *SRC_MASK.get().unwrap())?, // Oncecell is set at the beginning
                            dst: dst.clone(),
                            dst_watcher: inotify.watches().add(&dst, *DST_MASK.get().unwrap())?, // Oncecell is set at the beginning
                            user,
                            group,
                        })
                    },
                );
            let records: Result<_> = try_join_all(records).await;
            let records = Arc::new(RwLock::new(Arc::new(HashSet::from_iter(records?))));

            // Communication between Cli and Inotify
            let (comm_tx, mut comm_rx) =
                mpsc::channel::<(InotifyActions, oneshot::Sender<InotifyResults>)>(8);

            // CLI interface
            let r = records.clone();
            let u = cli.uds.clone();
            let h = tokio::spawn(async move {
                serv_cli(
                    &u,
                    &Server {
                        // Maybe move this out of the spawn and just borrow? I wonder borrowed one still `serv`.
                        db: cli.db,
                        records: r,
                        inotify_request: comm_tx,
                    },
                )
                .await
            });
            tasks.push(h);

            // Initial sync to have all dsts in place. This cannot be parallel with inotify watching as it would trigger it.
            records
                .get_cloned()
                .await
                .iter()
                .map(|r| {
                    let r = r.clone();
                    async move {
                        let x: Result<_> = try {
                            if !tokio::fs::try_exists(&r.dst).await?
                                && tokio::fs::try_exists(&r.src).await?
                                && !Command::new("cp")
                                    .uid(r.user)
                                    .gid(r.group)
                                    .args([
                                        "-f",
                                        &r.src.to_string_lossy(),
                                        &r.dst.to_string_lossy(),
                                    ])
                                    .status()
                                    .await?
                                    .success()
                            {
                                // SRC exists, DST does not exist, copying over failed
                                Err(anyhow!(
                                    "Failed sync from {} to {} as {}",
                                    &r.src.to_string_lossy(),
                                    &r.dst.to_string_lossy(),
                                    &r.user
                                ))?
                            }
                        };
                        x
                    }
                })
                .collect::<JoinSet<_>>()
                .join_all()
                .await
                .iter()
                .map(|sync_result| {
                    if let Err(e) = sync_result {
                        tracing::warn!("Initial syncing error: {e:?}");
                    }
                })
                .for_each(drop);

            // Watcher to Syncing
            let (w2s_tx, mut w2s_rx) = tokio::sync::mpsc::channel::<WatchDescriptor>(256);
            // Syncing
            let h = tokio::spawn(async move {
                loop {
                    if let Err(e) = serv_syncing(&mut w2s_rx, records.clone()).await {
                        tracing::warn!("Syncing error: {e:?}");
                    }
                }
            });
            tasks.push(h);

            // Watching
            let h = tokio::spawn(async move {
                loop {
                    if let Err(e) = serv_watching(&mut inotify, &w2s_tx, &mut comm_rx).await {
                        tracing::warn!("Watchiang error: {e:?}");
                    }
                }
            });
            tasks.push(h);

            tracing::debug!("All tasks running");
            signal::ctrl_c().await?; // Block until SIGINT
            tracing::debug!("Server ending");

            for t in tasks {
                t.abort();
            }

            if !Command::new("rm")
                .args(["-f", &cli.uds.to_string_lossy()])
                .status()
                .await?
                .success()
            {
                tracing::warn!(
                    "Cannot remove Unix Domain Socket ({}) for this service.",
                    cli.uds.to_string_lossy()
                );
            };

            tracing::debug!("Server ends");
        }
        _ => {
            tracing::debug!("Init client");
            let me: u32 = String::from_utf8(Command::new("id").arg("-u").output().await?.stdout)?
                .trim()
                .parse()?;
            let grp: u32 = String::from_utf8(Command::new("id").arg("-g").output().await?.stdout)?
                .trim()
                .parse()?;
            let uds = tarpc::serde_transport::unix::connect(&cli.uds, Bincode::default)
                .await
                .expect("Could not connect to the socket, maybe server is not running.");
            let client = CdlMsgClient::new(client::Config::default(), uds).spawn();

            match cli.cmd {
                SubCmd::Add { src, dst } => {
                    client
                        .add(context::current(), src, dst, me, grp)
                        .await?
                        .map_err(|s| anyhow!(s))?;
                }
                SubCmd::List => {
                    for FromTo { from, to } in client
                        .list(context::current(), me)
                        .await?
                        .map_err(|s| anyhow!(s))?
                    {
                        println!("{} => {}", from.to_string_lossy(), to.to_string_lossy());
                    }
                }
                SubCmd::Del { src, dst } => {
                    for sf in client
                        .del(context::current(), src, dst, me)
                        .await?
                        .map_err(|s| anyhow!(s))?
                    {
                        println!("{sf:?}");
                    }
                }
                _ => todo!(),
            }
        }
    }

    Ok(())
}

#[derive(Subcommand, Debug)]
#[command(rename_all = "lower")]
enum SubCmd {
    Serv,
    List,
    Add {
        src: PathBuf,
        dst: PathBuf,
    },
    Del {
        src: Option<PathBuf>,
        dst: Option<PathBuf>,
    },
}
#[derive(Parser)]
struct Cli {
    #[arg(long, global = true, default_value = "/etc/cross-device-link.csv")]
    db: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = "/var/run/cross-device-link.socket"
    )]
    uds: PathBuf,

    #[command(subcommand)]
    cmd: SubCmd,
}

#[tarpc::service]
trait CdlMsg {
    async fn add(src: PathBuf, dst: PathBuf, me: u32, group: u32) -> Result<(), String>;
    async fn list(me: u32) -> Result<Vec<FromTo<PathBuf>>, String>;
    async fn del(
        src: Option<PathBuf>,
        dst: Option<PathBuf>,
        me: u32,
    ) -> Result<Vec<SuccOrFail>, String>;
}

#[derive(Clone, Debug)]
struct Server {
    db: PathBuf,
    inotify_request: mpsc::Sender<(InotifyActions, oneshot::Sender<InotifyResults>)>,
    // Arcs
    // 1st is because RwLock is not clonable while Server should be, required by tarpc
    // 2nd is for get_cloned to have fast read access
    // 3rd is for reducing memory usage, maybe not necessary
    // This should be a common pattern and become a rust type
    records: Arc<RwLock<Arc<HashSet<Arc<RecordWithWatchDescriptor>>>>>,
}
impl CdlMsg for Server {
    async fn add(
        self,
        _context: ::tarpc::context::Context,
        src: PathBuf,
        dst: PathBuf,
        me: u32,
        group: u32,
    ) -> Result<(), String> {
        let x: Result<()> = try {
            if self.records.get_cloned().await.iter().any(|r| r.dst == dst) {
                Err(anyhow!(
                    "The target {} is already in sync",
                    dst.to_string_lossy()
                ))?
            }
            if Command::new("cp")
                .uid(me)
                .gid(group)
                .args(["-f", &src.to_string_lossy(), &dst.to_string_lossy()])
                .status()
                .await?
                .success()
            {
                tracing::debug!("Getting write lock");
                let mut lock = self.records.write().await;
                tracing::debug!("Got write lock");
                let r = Arc::get_mut(&mut *lock).ok_or(anyhow!("Records are occupied"))?;
                let (tx, rx) = oneshot::channel();
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
                    r.insert(Arc::new(RecordWithWatchDescriptor {
                        src: src.clone(),
                        src_watcher: from,
                        dst: dst.clone(),
                        dst_watcher: to,
                        user: me,
                        group,
                    }));
                };
                drop(lock);
                tracing::debug!("Dropped write lock");

                let mut csv = csv::Writer::from_path(self.db)?;
                self.records
                    .get_cloned()
                    .await
                    .iter()
                    .map(|r| {
                        csv.serialize(Record {
                            src: r.src.clone(),
                            dst: r.dst.clone(),
                            user: r.user,
                            group: r.group,
                        })
                        .map_err(|e| anyhow!("{e:?}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
            } else {
                Err(anyhow!("Failed to sync the file"))?
            }
        };
        x.map_err(|e| format!("{e:?}"))
    }

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
                .filter_map(|r| {
                    if r.user == me {
                        Some(FromTo {
                            from: r.src.clone(),
                            to: r.dst.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        };
        x.map_err(|e| format!("{e:?}"))
    }

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
                .filter(|r| r.user == me)
                .optional_filter(src.map(|s| {
                    let s = s.clone();
                    move |r: &&Arc<RecordWithWatchDescriptor>| r.src == s
                }))
                .optional_filter(dst.map(|d| {
                    let d = d.clone();
                    move |r: &&Arc<RecordWithWatchDescriptor>| r.dst == d
                }))
                .map(|x| x.clone())
                .collect();
            drop(tmp); // Avoid Arc being held too long to block `get_mut` below.

            let ret = to_dels
                .into_iter()
                .map(|x| {
                    let records = self.records.clone();
                    let y = x.clone();
                    let i = self.inotify_request.clone();
                    async move {
                        let ret: Result<()> = try {
                            let (tx, rx) = oneshot::channel();
                            i.send((InotifyActions::Del(y.dst_watcher.clone()), tx))
                                .await?;
                            if let InotifyResults::Del(ir) = rx.await? {
                                ir?;
                            };

                            let (tx, rx) = oneshot::channel();
                            i.send((InotifyActions::Del(y.src_watcher.clone()), tx))
                                .await?;
                            if let InotifyResults::Del(ir) = rx.await? {
                                ir?;
                            };

                            // tokio::fs::remove_file(y.dst.clone()).await?;

                            tracing::debug!("Getting write lock");
                            let mut lock = records.write().await;
                            tracing::debug!("Got write lock");
                            Arc::get_mut(&mut lock)
                                .ok_or(anyhow!("Records are occupied"))?
                                .remove(&y);
                            drop(lock);
                            tracing::debug!("Dropped write lock");
                        };
                        match ret {
                            Ok(()) => SuccOrFail::Succ(FromTo {
                                from: y.src.clone(),
                                to: y.dst.clone(),
                            }),
                            Err(e) => SuccOrFail::Fail {
                                filenames: FromTo {
                                    from: y.src.clone(),
                                    to: y.dst.clone(),
                                },
                                error: format!("{e:?}"),
                            },
                        }
                    }
                })
                .collect::<JoinSet<_>>()
                .join_all()
                .await;

            let mut csv = csv::Writer::from_path(self.db)?;
            self.records
                .get_cloned()
                .await
                .iter()
                .map(|r| {
                    csv.serialize(Record {
                        src: r.src.clone(),
                        dst: r.dst.clone(),
                        user: r.user,
                        group: r.group,
                    })
                    .map_err(|e| anyhow!("{e:?}"))
                })
                .collect::<Result<Vec<_>>>()?;
            ret
        };
        ret.map_err(|e| format!("{e:?}"))
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash)]
struct FromTo<T> {
    from: T,
    to: T,
}

#[derive(Debug)]
enum InotifyActions {
    Add(FromTo<PathBuf>),
    Del(WatchDescriptor),
}

#[derive(Debug)]
enum InotifyResults {
    Add(std::io::Result<FromTo<WatchDescriptor>>),
    Del(std::io::Result<()>),
}

#[derive(serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash)]
struct Record {
    src: PathBuf,
    dst: PathBuf,
    user: u32,
    group: u32,
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct RecordWithWatchDescriptor {
    src: PathBuf,
    src_watcher: WatchDescriptor,
    dst: PathBuf,
    dst_watcher: WatchDescriptor,
    user: u32,
    group: u32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
enum SuccOrFail {
    Succ(FromTo<PathBuf>),
    Fail {
        filenames: FromTo<PathBuf>,
        error: String,
    },
}

#[async_trait::async_trait]
trait GetCloned<T>
where
    T: Clone,
{
    async fn get_cloned(&self) -> T;
}
#[async_trait::async_trait]
impl<T> GetCloned<T> for RwLock<T>
where
    T: Clone + Send + Sync + std::fmt::Debug,
{
    async fn get_cloned(&self) -> T {
        tracing::debug!("Getting read lock");
        let read_lock = self.read().await;
        tracing::debug!("Got read lock");
        let ret = read_lock.clone();
        drop(read_lock);
        tracing::debug!("Dropped read lock");
        ret
    }
}

async fn serv_cli(uds: &Path, server: &Server) -> Result<()> {
    let listener = tarpc::serde_transport::unix::listen(uds, Bincode::default).await?;
    let mut perm = fs::metadata(uds).await?.permissions();
    perm.set_mode(perm.mode() | 0o222); // chmod a+w
    fs::set_permissions(uds, perm).await?;

    listener
        .filter_map(|x| ready(x.ok())) // Filter bad connection ahead, since `buffered` takes no `Result<Futures>` anyway.
        .map(|trans| {
            tracing::debug!("Accept request");
            server::BaseChannel::with_defaults(trans)
                .execute(server.clone().serve())
                .for_each(|x| async {
                    tokio::spawn(async {
                        tracing::debug!("Handling request");
                        x.await
                    });
                })
        })
        .buffered(8)
        .for_each(|_| async {})
        .await;
    Ok(())
}

async fn serv_syncing(
    w2s_rx: &mut mpsc::Receiver<WatchDescriptor>,
    records: Arc<RwLock<Arc<HashSet<Arc<RecordWithWatchDescriptor>>>>>,
) -> Result<()> {
    if let Some(wd) = w2s_rx.recv().await {
        tracing::debug!("From watcher: {wd:?}");
        if let Some(r) = records
            .get_cloned()
            .await
            .iter()
            .find(|r| r.dst_watcher == wd.clone() || r.src_watcher == wd.clone())
        {
            tracing::debug!("Found record for WD: {wd:?}");
            if !Command::new("cp")
                .uid(r.user)
                .gid(r.group)
                .args(["-f", &r.src.to_string_lossy(), &r.dst.to_string_lossy()])
                .status()
                .await?
                .success()
            {
                Err(anyhow!(
                    "Failed sync from {} to {} as {}",
                    &r.src.to_string_lossy(),
                    &r.dst.to_string_lossy(),
                    &r.user
                ))?;
            }
        }
    }
    // if let Some(wd) = w2s_rx.recv().await
    //     && let Some(r) = records
    //         .get_cloned()
    //         .await
    //         .iter()
    //         .find(|r| r.dst_watcher == wd.clone() || r.src_watcher == wd.clone())
    //     && !Command::new("cp")
    //         .uid(r.user)
    //         .gid(r.group)
    //         .args(["-f", &r.src.to_string_lossy(), &r.dst.to_string_lossy()])
    //         .status()
    //         .await?
    //         .success()
    // {
    //     Err(anyhow!(
    //         "Failed sync from {} to {} as {}",
    //         &r.src.to_string_lossy(),
    //         &r.dst.to_string_lossy(),
    //         &r.user
    //     ))?;
    // }
    Ok(())
}

async fn serv_watching(
    inotify: &mut Inotify,
    w2s_tx: &mpsc::Sender<WatchDescriptor>,
    comm_rx: &mut mpsc::Receiver<(InotifyActions, oneshot::Sender<InotifyResults>)>,
) -> Result<()> {
    let mut event_buf = [0; 1024];
    match inotify.read_events(&mut event_buf) {
        Ok(events) => {
            tracing::debug!("Inotify events: {events:?}");
            for event in events {
                let x = format!("{:?}", event.wd);
                w2s_tx.send(event.wd).await?;
                tracing::debug!("To watcher: {x}");
            }
        }
        Err(e) if e.kind() == ErrorKind::WouldBlock => sleep(Duration::from_millis(1)).await,
        x => {
            x?;
        }
    };
    if let Ok((ia, res)) = comm_rx.try_recv() {
        tracing::debug!("From cli: {ia:?}");
        let ir = match ia {
            InotifyActions::Add(FromTo { ref from, ref to }) => InotifyResults::Add(
                inotify
                    .watches()
                    .add(from, *SRC_MASK.get().unwrap())
                    .and_then(|s_wd| {
                        inotify
                            .watches()
                            .add(to, *DST_MASK.get().unwrap())
                            .map(|d_wd| FromTo {
                                from: s_wd,
                                to: d_wd,
                            })
                    }),
            ),
            InotifyActions::Del(ref wd) => {
                InotifyResults::Del(inotify.watches().remove((*wd).clone()))
            }
        };
        let x = format!("{ir:?}");
        res.send(ir)
            .map_err(|x| anyhow!("Failed to send response {x:?} for {ia:?}"))?;
        tracing::debug!("To cli: {x}");
    } else {
        sleep(Duration::from_millis(1)).await
    }

    Ok(())
}
