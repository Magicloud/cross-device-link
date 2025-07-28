use std::path::PathBuf;

use inotify::WatchDescriptor;

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash, Clone)]
pub struct FromTo<T> {
    pub from: T,
    pub to: T,
}

#[derive(Debug)]
pub enum InotifyActions {
    Add(FromTo<PathBuf>),
    Del(Record), // Sending a WatchDescriptor over channel seems breaking the Rust data and underneath C data. Inotify would say invalid argument (unknown watch descriptor).
}

#[derive(Debug)]
pub enum InotifyResults {
    Add(std::io::Result<FromTo<WatchDescriptor>>),
    Del(std::io::Result<()>),
}

#[derive(serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Record {
    #[serde(flatten)]
    pub src_dst: FromTo<PathBuf>,
    pub user: u32,
    pub group: u32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum SuccOrFail {
    Succ(FromTo<PathBuf>),
    Fail {
        filenames: FromTo<PathBuf>,
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_record() {
        let r = Record {
            src_dst: FromTo {
                from: "/root/a".into(),
                to: "/tmp/a".into(),
            },
            user: 0,
            group: 0,
        };
        eprintln!("{:?}", serde_json::to_string_pretty(&r));
    }
}
