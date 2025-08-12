use eyre::Result;
use std::path::PathBuf;

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash, Clone)]
pub struct FromTo<T> {
    pub from: T,
    pub to: T,
}

pub type UID = u32;
pub type GID = u32;

#[derive(Debug)]
pub enum InotifyActions {
    Add(Record),
    Del(FromTo<Option<PathBuf>>, u32),
    List,
    Stop,
}

#[derive(Debug)]
pub enum InotifyResults {
    Add(Result<()>),
    Del(Result<Vec<SuccOrFail>>),
    List(Vec<Record>),
}

#[derive(serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Record {
    #[serde(flatten)]
    pub src_dst: FromTo<PathBuf>,
    pub user: UID,
    pub group: GID,
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
