use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use tracing::*;
use uuid::Uuid;

use super::super::sync::SyncProgress;
use crate::context::GlobalContext;

#[derive(Serialize, Deserialize, Clone)]
pub enum GroupRole {
    Root,
    Member,
}

#[derive(Serialize, Deserialize, Clone, Builder)]
pub struct Entry {
    pub telegram_message_id: i32,
    pub huly_message_id: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub huly_image_id: Option<Uuid>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub group_role: Option<GroupRole>,

    pub date: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct Segment {
    channel: i64,
    entries: Vec<Entry>,
    deleted: HashSet<i32>,
}

impl Segment {
    fn load(bytes: &[u8]) -> Result<Self> {
        Ok(ciborium::from_reader(bytes)?)
        //Ok(serde_json::from_slice(bytes)?)
    }

    fn store(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes)?;
        Ok(bytes)
        //Ok(serde_json::to_vec(self)?)
    }
}

#[derive(Default)]
struct Index {
    by_telegram_id: HashMap<i32, Entry>,
}

impl Index {
    fn values(&self) -> Vec<Entry> {
        self.by_telegram_id.values().cloned().collect()
    }
}

impl Index {
    fn include(&mut self, entry: &Entry) {
        self.by_telegram_id
            .insert(entry.telegram_message_id, entry.to_owned());
    }

    #[allow(dead_code)]
    fn clear(&mut self) {
        self.by_telegram_id.clear();
    }
}

pub struct SyncState {
    channel: i64,
    index: Index,
    segment: Option<usize>,

    path: String,
    added: Index,
    deleted: HashSet<i32>,

    context: Arc<GlobalContext>,

    is_dirty: bool,
}

impl SyncState {
    fn segment_path(path: &String, segment: usize) -> String {
        format!("{}_{}", path, segment)
    }

    #[instrument(level = "trace", skip(context), fields(channel = %channel, path = %path))]
    pub async fn load(channel: i64, path: String, context: Arc<GlobalContext>) -> Result<Self> {
        let kvs = context.kvs();
        let mut segment = None;
        let mut segments = Vec::new();

        for n in 0.. {
            if let Some(bytes) = kvs.get(&Self::segment_path(&path, n)).await? {
                segments.push(Segment::load(bytes.as_slice())?);
                segment = Some(n);
            } else {
                break;
            }
        }

        let mut state = SyncState {
            channel,
            index: Index::default(),
            segment,
            path,
            added: Index::default(),
            deleted: HashSet::new(),
            context,
            is_dirty: false,
        };

        if let Some((last, others)) = segments.split_last() {
            let mut deleted = HashSet::<i32>::new();
            for segment in others {
                deleted.extend(segment.deleted.iter().cloned());
            }

            let mut index = Index::default();
            for segment in &segments {
                for entry in &segment.entries {
                    index.include(entry);
                }
            }

            state.added = Index::default();

            for entry in &last.entries {
                state.added.include(entry);
            }

            state.deleted = last.deleted.to_owned();
        }

        Ok(state)
    }

    pub fn ids(&self) -> Vec<i32> {
        self.index
            .by_telegram_id
            .keys()
            .chain(self.added.by_telegram_id.keys())
            .filter(|id| !self.deleted.contains(id))
            .copied()
            .collect()
    }

    pub fn lookup(&self, id: i32) -> Option<&Entry> {
        //let mut x = self.added.by_telegram_id.keys().collect::<Vec<_>>();
        //x.sort();

        //trace!(id, ids = ?x, "Lookup");

        self.added
            .by_telegram_id
            .get(&id)
            .or_else(|| self.index.by_telegram_id.get(&id))
            .filter(|entry| !self.deleted.contains(&entry.telegram_message_id))
    }

    pub fn is_deleted(&self, id: i32) -> bool {
        self.deleted.contains(&id)
    }

    pub fn delete(&mut self, id: i32) {
        self.deleted.insert(id);
        self.is_dirty = true;
    }

    pub fn upsert(&mut self, entry: &Entry) {
        self.added.include(entry);
        self.is_dirty = true;
    }

    #[instrument(level = "trace", skip(self), fields(dirty = %self.is_dirty))]
    pub async fn persist(&mut self) -> Result<()> {
        if self.is_dirty {
            let segment = Segment {
                channel: self.channel,
                entries: self.added.values(),
                deleted: self.deleted.clone(),
            };

            let bytes = segment.store()?;
            let kvs = self.context.kvs();

            let segment = self.segment.unwrap_or(0);
            self.segment = Some(segment);

            kvs.upsert(&Self::segment_path(&self.path, segment), &bytes)
                .await?;

            self.is_dirty = false;

            trace!(segment, "State persisted");

            if bytes.len() > 1024 * 200 {
                // rotate
                self.segment = Some(segment + 1);
                self.deleted.clear();
                self.added.clear();

                trace!(segment = self.segment.unwrap(), "State rotated");
            }
        }

        Ok(())
    }

    async fn set_progress(&mut self, _progress: SyncProgress) -> Result<()> {
        //
        unimplemented!()
    }

    fn progress(&self) -> SyncProgress {
        SyncProgress::Unsynced
    }
}
