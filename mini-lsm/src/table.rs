mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes};

use crate::block::Block;
use crate::lsm_storage::BlockCache;

pub use builder::SsTableBuilder;
pub use iterator::SsTableIterator;

/*
SST like below
| data block | data block | data block | data block | meta block | meta block offset (u32) |


meta block:
vec of BlockMeta
includes the first key in each block and the offset of each block.
|offset as u32| first key len as u16|first key|
*/

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block
    pub offset: usize,
    /// The first key of the data block
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.    
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = 0;

        for meta in block_meta {
            // 对应下面的buf, offset 用u32 存储，first_key len 用u16
            estimated_size += std::mem::size_of::<u32>();
            estimated_size += std::mem::size_of::<u16>();
            estimated_size += meta.first_key.len();
        }

        buf.reserve(estimated_size);

        let ori_len = buf.len();

        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }

        assert_eq!(estimated_size, buf.len() - ori_len);
    }

    /// Decode block meta from a buffer
    fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();

        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;

            let first_key_len = buf.get_u16() as usize;

            let first_key = buf.copy_to_bytes(first_key_len);
            block_meta.push(BlockMeta { offset, first_key });
        }
        block_meta
    }
}
/// File name and total block with byte unite
#[derive(Debug)]
pub struct FileObject(File, u64);

impl FileObject {
    /// read from offset and length is len
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0.read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn new(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        Ok(FileObject(
            File::options().read(true).write(false).open(path)?,
            data.len() as u64,
        ))
    }

    pub fn open(_path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct SsTable {
    ///
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        // meta block offset (u32) u32 4个字节
        let raw_meta_offset = file.read(len - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, len - 4 - block_meta_offset)?;
        Ok(Self {
            file,
            block_metas: BlockMeta::decode_block_meta(&raw_meta[..]),
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
        })
    }

    /// Read a block from the disk
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset;
        let offset_end = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);

        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;

        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk with block cache
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;

            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_metas
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod table_test;
