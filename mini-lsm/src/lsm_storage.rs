

use crate::block::Block;
use std::sync::Arc;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;