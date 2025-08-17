#![feature(try_blocks)]
#![feature(iterator_try_collect)]

mod cdl_msg;
mod cli;
mod traits;
mod types;

use clap::Parser;
use eyre::{Result, anyhow};
use futures::{StreamExt, future::ready};
use inotify::{Inotify, WatchDescriptor, WatchMask};
use std::{
    collections::HashMap,
    fs::File,
    io::ErrorKind,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::exit,
    sync::{mpsc, mpsc as oneshot},
    thread,
    time::Duration,
};
use tarpc::{
    client, context,
    server::{self, Channel},
};
use tokio::{fs, process::Command, signal, sync::OnceCell};
use tokio_serde::formats::Bincode;
use tracing::instrument;
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};

use crate::cdl_msg::*;
use crate::cli::*;
use crate::traits::*;
use crate::types::*;

static SRC_MASK: OnceCell<WatchMask> = OnceCell::const_new();
static DST_MASK: OnceCell<WatchMask> = OnceCell::const_new();

#[tokio::main]
async fn main() -> Result<()> {
    let x: Result<_> = try {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::FULL))
            .with(ErrorLayer::default())
            .try_init()?;
        color_eyre::install()?;

        SRC_MASK
            .set(
                WatchMask::MODIFY
                    | WatchMask::CREATE
                    | WatchMask::DELETE_SELF
                    | WatchMask::MOVE_SELF,
            )
            .map_err(|e| anyhow!("{e:?}"))?;
        DST_MASK
            .set(WatchMask::MODIFY | WatchMask::DELETE_SELF | WatchMask::MOVE_SELF)
            .map_err(|e| anyhow!("{e:?}"))?;
    };
    x.unwrap_or_else(|e| {
        tracing::error!("Initializing error: {e:?}");
        exit(1);
    });
    let cli = Cli::parse();

    match cli.cmd {
        SubCmd::Serv => {
            serv(cli).await?; // Move ownership to `serv` since it is the rest of the program.
        }
        _ => {
            tracing::debug!("Init client");
            let me: UID = String::from_utf8(Command::new("id").arg("-u").output().await?.stdout)?
                .trim()
                .parse()?;
            let grp: GID = String::from_utf8(Command::new("id").arg("-g").output().await?.stdout)?
                .trim()
                .parse()?;
            let uds = tarpc::serde_transport::unix::connect(&cli.uds, Bincode::default)
                .await
                .expect("Could not connect to the socket, maybe server is not running.");
            let client = CdlMsgClient::new(client::Config::default(), uds).spawn();

            match cli.cmd {
                SubCmd::Add { src, dst } => {
                    match fs::try_exists(&dst).await {
                        Ok(true) => println!(
                            "Target file {} exists, will be overwritten.",
                            dst.to_string_lossy()
                        ),
                        Ok(false) => (),
                        Err(e) => println!(
                            "Cannot check target file {} existence for reason {e:?}, will proceed.",
                            dst.to_string_lossy()
                        ),
                    }
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
                    println!(
                        "Note: Target files won't be removed. This command just stops the syncing."
                    );
                    for sf in client
                        .del(context::current(), src, dst, me)
                        .await?
                        .map_err(|s| anyhow!(s))?
                    {
                        println!("{sf:?}");
                    }
                }
                _ => (),
            }
        }
    }

    Ok(())
}

#[instrument(level = "debug")]
async fn serv_cli(uds: &Path, server: &Server) -> Result<()> {
    let listener = tarpc::serde_transport::unix::listen(uds, Bincode::default).await?;
    let mut perm = fs::metadata(uds).await?.permissions();
    perm.set_mode(perm.mode() | 0o222); // chmod a+w
    fs::set_permissions(uds, perm).await?;

    listener
        .filter_map(|x| ready(x.ok())) // Filter bad connection ahead, since `buffered` takes no `Result<Futures>` anyway.
        .map(|trans| {
            tracing::info!("Accept request");
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

#[instrument(level = "debug")]
async fn serv_syncing(w2s_rx: &mut mpsc::Receiver<Record>) -> Result<()> {
    if let Ok(r) = w2s_rx.try_recv() {
        tracing::debug!("From watcher: {r:?}");
        Command::new("cp")
            .uid(r.user)
            .gid(r.group)
            .args([
                "-f",
                &r.src_dst.from.to_string_lossy(),
                &r.src_dst.to.to_string_lossy(),
            ])
            .status()
            .await?
            .success()
            .to_result(
                (),
                anyhow!(
                    "Failed sync from {} to {} as {}",
                    &r.src_dst.from.to_string_lossy(),
                    &r.src_dst.to.to_string_lossy(),
                    &r.user
                ),
            )?;
        tracing::info!("Synced file {}", r.src_dst.to.to_string_lossy());
    } else {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(())
}

// #[instrument(level = "debug")]
// This fn runs in thread and should not block.
fn serv_watching(
    inotify: &mut Inotify,
    w2s_tx: &mpsc::Sender<Record>,
    comm_rx: &mut mpsc::Receiver<(InotifyActions, oneshot::Sender<InotifyResults>)>,
    records: &mut HashMap<Record, FromTo<WatchDescriptor>>,
) -> Result<LoopCtrl> {
    let mut event_buf = [0; 32]; // TODO: Move out of the loop?
    match inotify.read_events(&mut event_buf) {
        Ok(events) => {
            tracing::info!("Inotify events: {events:?}");
            for event in events {
                records
                    .extract_if(|_, v| v.from == event.wd || v.to == event.wd)
                    .next()
                    .map(|(k, FromTo { from, to })| {
                        inotify.watches().remove(to)?;
                        if let Err(e) = w2s_tx.send(k.clone()) {
                            tracing::warn!("To Sync failed: {e:?}");
                        };
                        let to = inotify
                            .watches()
                            .add(k.src_dst.to.clone(), *SRC_MASK.get().unwrap())?;
                        // Find then insert involves mutable borrow records while it is still borrowed.
                        // Wonder if there is a solution without the extract.
                        records.insert(k.clone(), FromTo { from, to });
                        Ok(()) as Result<()>
                    });
            }
        }
        Err(e) if e.kind() == ErrorKind::WouldBlock => (),
        x => {
            x?;
        }
    };
    let ret = if let Ok((ia, res)) = comm_rx.try_recv() {
        tracing::info!("Process actions: {ia:?}");
        let ir = match &ia {
            InotifyActions::Add(r) => {
                let result = try {
                    // Logically, this blocking is necesssary. The watches must be added after file is synced.
                    // But, if the file is large, we may need a queue to continue process in next loop.
                    std::process::Command::new("cp")
                        .uid(r.user)
                        .gid(r.group)
                        .args([
                            "-f",
                            &r.src_dst.from.to_string_lossy(),
                            &r.src_dst.to.to_string_lossy(),
                        ])
                        .status()?
                        .success()
                        .to_result(
                            (),
                            anyhow!(
                                "Failed sync from {} to {} as {}",
                                &r.src_dst.from.to_string_lossy(),
                                &r.src_dst.to.to_string_lossy(),
                                &r.user
                            ),
                        )?;

                    let from = inotify
                        .watches()
                        .add(&r.src_dst.from, *SRC_MASK.get().unwrap())?;
                    let to = inotify
                        .watches()
                        .add(&r.src_dst.to, *DST_MASK.get().unwrap())?;
                    records.insert(r.clone(), FromTo { from, to });
                };
                InotifyResults::Add(result)
            }
            InotifyActions::Del(r, me) => {
                let result = records
                    .extract_if(|k, _| {
                        k.user == *me
                            && (Some(k.src_dst.from.clone()) == r.from
                                || Some(k.src_dst.to.clone()) == r.to)
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|(k, v)| {
                        let f = inotify.watches().remove(v.from.clone());
                        let t = inotify.watches().remove(v.to.clone());
                        if f.is_err() || t.is_err() {
                            let ret = SuccOrFail::Fail {
                                filenames: k.src_dst.clone(),
                                error: format!("From {f:?}, To {t:?}"),
                            };
                            records.insert(k, v);
                            ret
                        } else {
                            SuccOrFail::Succ(k.src_dst.clone())
                        }
                    })
                    .collect();
                InotifyResults::Del(Ok(result))
            }
            InotifyActions::List => {
                InotifyResults::List(records.keys().map(|x| x.clone()).collect())
            }
            InotifyActions::Stop => InotifyResults::List(vec![]), // return Ok(LoopCtrl::Break), // This is so ugly
        };
        match ia {
            InotifyActions::Add(_) | InotifyActions::Del(_, _) => {
                let x = format!("{ir:?}");
                res.send(ir)
                    .map_err(|x| anyhow!("Failed to send response {x:?} for {ia:?}"))?;
                tracing::info!("Actions results: {x}");
                LoopCtrl::ContinuePersist
            }
            InotifyActions::Stop => LoopCtrl::Break,
            InotifyActions::List => LoopCtrl::Continue,
        }
    } else {
        LoopCtrl::Continue
    };

    Ok(ret)
}

#[instrument(level = "debug")]
async fn serv(cli: Cli) -> Result<()> {
    tracing::info!("Server starts");
    let mut tasks = Vec::new();
    let x: Result<_> = try {
        match fs::try_exists(&cli.db).await {
            Ok(true) => (), // file exists
            Ok(false) => {
                tracing::info!("DB file does not exist. Create empty one");
                fs::write(&cli.db, b"[]").await?; // file not exist, create empty
            }
            Err(e) => Err(e)?, // cannot check
        }

        Inotify::init()?
    };
    let mut inotify = x.unwrap_or_else(|e| {
        tracing::error!("Server initializing error: {e:?}");
        exit(1);
    });

    tracing::debug!("Load records");
    let x: Result<_> = try {
        let v = tokio::fs::read(&cli.db).await?;
        let v: Vec<Record> = serde_json::from_slice(&v)?;
        v
    };
    let records = x.unwrap_or_else(|e| {
        tracing::error!("Loading records error: {e:?}");
        exit(1);
    });

    // Communication between Cli and Inotify
    let (comm_tx, mut comm_rx) =
        mpsc::channel::<(InotifyActions, oneshot::Sender<InotifyResults>)>();

    // CLI interface
    let u = cli.uds.clone();
    let c = comm_tx.clone();
    let h = tokio::spawn(async move {
        serv_cli(
            &u,
            &Server {
                // Maybe move this out of the spawn and just borrow? I wonder borrowed one still `serv`.
                inotify_request: c,
            },
        )
        .await
    });
    tasks.push(h);

    // Watcher to Syncing
    let (w2s_tx, mut w2s_rx) = mpsc::channel::<Record>();
    // Syncing
    let h = tokio::spawn(async move {
        loop {
            if let Err(e) = serv_syncing(&mut w2s_rx).await {
                tracing::warn!("Syncing error: {e:?}");
            }
        }
    });
    tasks.push(h);

    // Watching
    // In original async design, WatchDescriptor may become invalid for no apparent reason.
    // I suspect that it should not be Send/Sync.
    // Now have it within a thread.
    thread::spawn(move || {
        let mut hm: HashMap<Record, FromTo<WatchDescriptor>> = HashMap::new();
        loop {
            match serv_watching(&mut inotify, &w2s_tx, &mut comm_rx, &mut hm) {
                Err(e) => tracing::warn!("Watching error: {e:?}"),
                Ok(LoopCtrl::Break) => break,
                Ok(LoopCtrl::Continue) => (),
                Ok(LoopCtrl::ContinuePersist) => {
                    let x: Result<()> = try {
                        let file = File::open(&cli.db)?;
                        serde_json::to_writer_pretty(file, &(hm.keys().collect::<Vec<_>>()))?;
                    };
                    if let Err(e) = x {
                        tracing::warn!("Cannot persist records to file as {e:?}");
                    }
                }
            }
        }
    });

    // Looping records to add syncs
    records
        .into_iter()
        .map(|r| {
            let (tx, rx) = oneshot::channel();
            comm_tx.send((InotifyActions::Add(r), tx))?;
            drop(rx);
            Ok(()) as Result<()>
        })
        .try_collect::<Vec<_>>()?;

    tracing::info!("All tasks running");
    signal::ctrl_c().await?; // Block until SIGINT
    tracing::info!("Server ending");

    let (tx, rx) = oneshot::channel();
    comm_tx.send((InotifyActions::Stop, tx))?;
    drop(rx);
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

    tracing::info!("Server ends");

    Ok(())
}
