use std::path::PathBuf;

use clap::*;

#[derive(Subcommand, Debug)]
#[command(rename_all = "lower")]
pub enum SubCmd {
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
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    #[arg(long, global = true, default_value = "/etc/cross-device-link.json")]
    pub db: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = "/var/run/cross-device-link.socket"
    )]
    pub uds: PathBuf,

    #[command(subcommand)]
    pub cmd: SubCmd,
}
