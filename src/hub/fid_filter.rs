use crate::proto::{HubEvent, hub_event};
use std::{
    collections::HashSet,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct FidFilter {
    allowed_fids: Arc<RwLock<HashSet<u64>>>,
    filter_enabled: bool,
}

impl FidFilter {
    pub fn new(fids: Vec<u64>, enabled: bool) -> Self {
        let allowed_fids = fids.into_iter().collect::<HashSet<u64>>();
        info!("Initialized FID filter with {} allowed FIDs, enabled: {}", allowed_fids.len(), enabled);
        
        Self {
            allowed_fids: Arc::new(RwLock::new(allowed_fids)),
            filter_enabled: enabled,
        }
    }

    pub async fn is_allowed(&self, fid: u64) -> bool {
        if !self.filter_enabled {
            return true;
        }
        
        self.allowed_fids.read().await.contains(&fid)
    }

    pub async fn update_allowed_fids(&self, fids: Vec<u64>) {
        let new_set = fids.into_iter().collect::<HashSet<u64>>();
        let mut allowed = self.allowed_fids.write().await;
        *allowed = new_set;
        info!("Updated FID filter with {} allowed FIDs", allowed.len());
    }

    pub async fn filter_events(&self, events: &[HubEvent]) -> Vec<usize> {
        if !self.filter_enabled {
            return (0..events.len()).collect();
        }

        let mut keep_indices = Vec::new();
        let allowed_fids = self.allowed_fids.read().await;

        for (idx, event) in events.iter().enumerate() {
            let should_keep = match &event.body {
                Some(hub_event::Body::MergeMessageBody(body)) => match &body.message {
                    Some(msg) => match &msg.data {
                        Some(data) => {
                            // Only keep casts (types 1 and 2) from allowed FIDs
                            if data.r#type == 1 || data.r#type == 2 {
                                allowed_fids.contains(&data.fid)
                            } else {
                                false
                            }
                        },
                        None => false,
                    },
                    None => false,
                },
                Some(hub_event::Body::PruneMessageBody(body)) => match &body.message {
                    Some(msg) => match &msg.data {
                        Some(data) => {
                            // Only keep cast prunes from allowed FIDs
                            if data.r#type == 1 || data.r#type == 2 {
                                allowed_fids.contains(&data.fid)
                            } else {
                                false
                            }
                        },
                        None => false,
                    },
                    None => false,
                },
                _ => false, // Ignore all other event types
            };

            if should_keep {
                keep_indices.push(idx);
            }
        }

        keep_indices
    }

    pub fn is_enabled(&self) -> bool {
        self.filter_enabled
    }
}