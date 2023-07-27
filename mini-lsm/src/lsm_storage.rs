use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::FusedIterator;
use crate::lsm_iterator::LsmIterator;
use crate::mem_table::{map_bound, Memtable};
use crate::table::{SsTableBuilder, SsTableIterator};
use crate::{block::Block, table::SsTable};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable
    memtable: Arc<Memtable>,
    /// Immutable memTable, from earliest to latest
    imm_memtable: Vec<Arc<Memtable>>,
    /// L0 SsTable, from earliest to latest
    l0_sstable: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SsTable ID.
    next_ssd_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(Memtable::create()),
            imm_memtable: vec![],
            l0_sstable: vec![],
            levels: vec![],
            next_ssd_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    flush_lock: Mutex<()>,
    path: PathBuf,
    block_cache: Arc<BlockCache>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            flush_lock: Mutex::new(()),
            path: path.as_ref().to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1 << 20)), // 4GB block cache
        })
    }

    /// Get a key from the storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.inner.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // search on the current memtable
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // search on immutable memetables.
        for memtable in snapshot.imm_memtable.iter().rev() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut iters = Vec::new();
        iters.reserve(snapshot.l0_sstable.len());

        for table in snapshot.l0_sstable.iter().rev() {
            iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                table.clone(),
                key,
            )?));
        }

        let iter = MergeIterator::create(iters);
        if iter.is_valid() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    /// put a key-value pair into the storage by writing into the current memetable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");

        let guard = self.inner.read();
        guard.memtable.put(key, value);
        Ok(())
    }

    /// remove a key from the storage by writing an empty value
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");

        let guard = self.inner.read();
        guard.memtable.put(key, b"");
        Ok(())
    }

    fn path_of_sst(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.sst", id))
    }

    /// Persist data to disk
    pub fn sync(&self) -> Result<()> {
        let _flush_lock = self.flush_lock.lock();
        let flush_memtable;
        let sst_id;

        // Move mutable memtable to immmutable memtables
        {
            let mut guard = self.inner.write();
            // swap the current memtable with a new one
            let mut snapshot = guard.as_ref().clone();
            let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(Memtable::create()));

            flush_memtable = memtable.clone();
            sst_id = snapshot.next_ssd_id;
            // add the memetable to the immutable memtables
            snapshot.imm_memtable.push(memtable);
            // update the snapshot
            *guard = Arc::new(snapshot);
        }

        // At this point, the old memtable should be disabled for write, and all write threads
        // should be operating on the new memtable. We can safely flush the old memtable to
        // disk.
        let mut builder: SsTableBuilder = SsTableBuilder::new(4096);
        flush_memtable.flush(&mut builder)?;

        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        // Add the flushed L0 table to the list
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();

            // remove the memtable from the immutable memtables.
            snapshot.imm_memtable.pop();
            // add L0 table
            snapshot.l0_sstable.push(sst);
            // update SST ID
            snapshot.next_ssd_id += 1;
            // update the snapshot
            *guard = Arc::new(snapshot)
        }
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.inner.read();
            Arc::clone(&guard)
        };

        let mut memtable_iters = Vec::new();
        memtable_iters.reserve(snapshot.imm_memtable.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));

        for memtable in snapshot.imm_memtable.iter().rev() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }

        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::new();

        for table in snapshot.l0_sstable.iter().rev() {
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table.clone(), key)?
                }

                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table.clone())?,
            };
            table_iters.push(Box::new(iter));
        }

        let table_iter = MergeIterator::create(table_iters);

        let iter = TwoMergeIterator::create(memtable_iter, table_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
