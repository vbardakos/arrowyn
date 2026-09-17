use std::{borrow::Cow, ops::Deref};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Manifest<'a> {
    #[serde(borrow)]
    entries: Cow<'a, [Entry<'a>]>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Entry<'a> {
    version: u32,
    #[serde(borrow)]
    context: Option<Context<'a>>,
    #[serde(borrow)]
    schema: Cow<'a, str>,
    #[serde(borrow)]
    compression: Option<Cow<'a, str>>,
    #[serde(borrow)]
    shards: Cow<'a, Shard<'a>>,
    created: u64,
    #[serde(borrow)]
    writer: Cow<'a, str>,
}

impl<'a> Entry<'a> {
    const VERSION: u32 = 1;

    pub(crate) fn new() -> Self {
        Self {
            version: Entry::VERSION,
            context: None,
            schema: Cow::default(),
            compression: None,
            shards: Cow::default(),
            created: 0,
            writer: Cow::default(),
        }
    }

    pub(crate) fn is_completed(&self) -> bool {}
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Context<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
    #[serde(borrow)]
    dag: Cow<'a, str>,
    #[serde(borrow)]
    task: Cow<'a, str>,
    #[serde(borrow)]
    run: Cow<'a, str>,
    idx: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Shard<'a> {
    id: u32,
    #[serde(borrow)]
    name: Cow<'a, str>,
    rows: usize,
    size: usize,
    batches: Vec<u32>,
}
