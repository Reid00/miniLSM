mod builder;
mod iterator;

use bytes::{Bytes, BufMut, Buf};
pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
/*
block struct: 
|          data         |           offsets         |
|entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|

offsets and num: 
|offset|offset|num_of_elements|
|   0  |  12  |       2       |

----------------------------------------------------------
entry struct:
Key length and value length are 2 Bytes, which means their maximum length is 65536.

|                             entry1                            |
| key_len (2B) | key  | value_len (2B) | value  | ... |

*/
#[derive(Debug)]
pub struct Block{
    data: Vec<u8>,
    offsets: Vec<u16>,
}


impl Block {

    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offset_len = self.offsets.len();

        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // num_of_elements at the end of the block store as u16
        buf.put_u16(offset_len as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        // should be num_of_elements
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        // println!("entry_offsets_len: {}", entry_offsets_len);

        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len()-SIZEOF_U16];

        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();

        let data = data[0..data_end].to_vec();
        Self { data,  offsets}
    }
    
}

#[cfg(test)]
mod block_test;