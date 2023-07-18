use super::{Block, SIZEOF_U16};
use bytes::BufMut;

/// Builds a block
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All key-value pairs in the block.
    data: Vec<u8>,
    ///  The expected block size of byte.
    block_size: usize,
}

impl BlockBuilder {
    pub fn new(size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: size,
        }
    }

    /// Return the size of a block except num_of_elements
    fn estimated_size(&self) -> usize {
        let cur_size = self.offsets.len() * SIZEOF_U16 + self.data.len() + SIZEOF_U16;
        // println!("cur size {}", cur_size);
        cur_size
    }

    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        // cur size + new key val size + (key/val len size + num_of_elements size)
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);

        // b"22" 字节字符串, 占两个字节，put_u16 两个字节
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        true
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    // Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
