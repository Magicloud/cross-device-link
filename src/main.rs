#![feature(try_blocks)]

mod cdl_msg;
mod cli;
mod traits;
mod types;

use clap::Parser;
use eyre::{Result, anyhow};
use futures::{
    StreamExt,
    future::{ready, try_join_all},
};
use inotify::{Inotify, WatchDescriptor, WatchMask};
use std::{
    collections::HashMap, io::ErrorKind, os::unix::fs::PermissionsExt, path::Path, process::exit,
    sync::Arc, time::Duration,
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
async fn serv_syncing(
    w2s_rx: &mut mpsc::Receiver<WatchDescriptor>,
    records: Arc<RwLock<Arc<HashMap<Record, FromTo<WatchDescriptor>>>>>,
) -> Result<()> {
    if let Some(wd) = w2s_rx.recv().await {
        tracing::debug!("From watcher: {wd:?}");
        if let Some((r, _)) = records
            .get_cloned()
            .await
            .iter()
            .find(|&(_, wds)| wds.to == wd.clone() || wds.from == wd.clone())
        {
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
        }
    }
    Ok(())
}

#[instrument(level = "debug")]
async fn serv_watching(
    inotify: &mut Inotify,
    w2s_tx: &mpsc::Sender<WatchDescriptor>,
    comm_rx: &mut mpsc::Receiver<(InotifyActions, oneshot::Sender<InotifyResults>)>,
    records: Arc<RwLock<Arc<HashMap<Record, FromTo<WatchDescriptor>>>>>,
) -> Result<()> {
    let mut event_buf = [0; 1024];
    match inotify.read_events(&mut event_buf) {
        Ok(events) => {
            tracing::info!("Inotify events: {events:?}");
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
        tracing::info!("Process actions: {ia:?}");
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
            InotifyActions::Del(ref r) => {
                let wds = records.get_cloned().await;
                let wds = wds.get(r).unwrap();
                InotifyResults::Del(
                    inotify
                        .watches()
                        .remove(wds.from.clone())
                        .and_then(|_| inotify.watches().remove(wds.to.clone())),
                )
            }
        };
        let x = format!("{ir:?}");
        res.send(ir)
            .map_err(|x| anyhow!("Failed to send response {x:?} for {ia:?}"))?;
        tracing::info!("Actions results: {x}");
    } else {
        sleep(Duration::from_millis(1)).await
    }

    Ok(())
}

#[instrument(level = "debug")]
async fn serv(cli: Cli) -> Result<()> {
    tracing::info!("Server starts");
    let mut tasks = Vec::new(); // replace the whole part with tokio-shutdown/tokio-graceful?
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
        let records = v.into_iter().map(async |r| try {
            (
                r.clone(),
                FromTo {
                    from: inotify
                        .watches()
                        .add(&r.src_dst.from, *SRC_MASK.get().unwrap())?, // Oncecell is set at the beginning
                    to: inotify
                        .watches()
                        .add(&r.src_dst.to, *DST_MASK.get().unwrap())?, // Oncecell is set at the beginning
                },
            )
        });
        let records: Result<_> = try_join_all(records).await; // Why this cannot be replaced with JoinSet due to inotify outlive?
        Arc::new(RwLock::new(Arc::new(HashMap::from_iter(records?))))
    };
    let records = x.unwrap_or_else(|e| {
        tracing::error!("Loading records error: {e:?}");
        exit(1);
    });

    // Initial sync to have all dsts in place. This cannot be parallel with inotify watching as it would trigger it.
    records
        .get_cloned()
        .await
        .iter()
        .map(|(r, _)| {
            let r = r.clone();
            async move {
                let x: Result<_> = try {
                    if !tokio::fs::try_exists(&r.src_dst.to).await?
                        && tokio::fs::try_exists(&r.src_dst.from).await?
                        && !Command::new("cp")
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
                    {
                        // SRC exists, DST does not exist, copying over failed
                        Err(anyhow!(
                            "Failed sync from {} to {} as {}",
                            &r.src_dst.from.to_string_lossy(),
                            &r.src_dst.to.to_string_lossy(),
                            &r.user
                        ))?
                    }
                };
                if let Err(e) = x {
                    tracing::warn!("Initial syncing error: {e:?}");
                };
            }
        })
        .collect::<JoinSet<_>>()
        .join_all()
        .await;

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

    // Watcher to Syncing
    let (w2s_tx, mut w2s_rx) = tokio::sync::mpsc::channel::<WatchDescriptor>(256);
    // Syncing
    let r = records.clone();
    let h = tokio::spawn(async move {
        loop {
            if let Err(e) = serv_syncing(&mut w2s_rx, r.clone()).await {
                tracing::warn!("Syncing error: {e:?}");
            }
        }
    });
    tasks.push(h);

    // Watching
    let h = tokio::spawn(async move {
        loop {
            if let Err(e) =
                serv_watching(&mut inotify, &w2s_tx, &mut comm_rx, records.clone()).await
            {
                tracing::warn!("Watching error: {e:?}");
            }
        }
    });
    tasks.push(h);

    tracing::info!("All tasks running");
    signal::ctrl_c().await?; // Block until SIGINT
    tracing::info!("Server ending");

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
