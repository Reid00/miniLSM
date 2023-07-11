use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, Ok};
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::lsm_storage::BlockCache;


/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder{
    ///
    builder:BlockBuilder,
    first_key: Vec<u8>,
    data: Vec<u8>,
    pub(super) meta: Vec<BlockMeta>,
    block_size: usize,
}


impl SsTableBuilder {

    /// Create a builder based on target block size.    
    pub fn new(block_size: usize) ->Self {
        Self{
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }

        if self.builder.add(key, value) {
            return;
        }

        // create a new block builder and append block data
        self.finish_block();

        // add the key-value pair to the next block
        assert!(self.builder.add(key, value));
        self.first_key = key.to_vec();

    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let enc_block = builder.build().encode();
        self.meta.push(BlockMeta { offset: self.data.len(), 
            first_key: std::mem::take(&mut self.first_key).into() });

        self.data.extend(enc_block);
    }

    pub fn build(mut self, id: usize, block_cache: Option<Arc<BlockCache>>, 
                    path: impl AsRef<Path>) -> Result<SsTable> {
        
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);

        let file = FileObject::new(path.as_ref(), buf)?;
        Ok(SsTable { file, id, block_meta_offset: meta_offset, block_metas: self.meta, block_cache})
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

}