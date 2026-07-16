// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// #730: scanner/data-usage state is partially migrated and still owns staged cache helpers.
#![allow(dead_code)]

pub mod local_snapshot;

use crate::storage_api_contracts::{
    bucket::{BucketOperations as _, BucketOptions},
    list::{ListOperations as _, StorageListObjectVersionsInfo},
    object::ObjectIO as _,
};
use crate::{
    bucket::{metadata_sys::get_replication_config, versioning::VersioningApi as _, versioning_sys::BucketVersioningSys},
    config::com::read_config,
    disk::DiskAPI,
    error::{Error, classify_system_path_failure_reason},
    object_api::ObjectInfo,
    runtime::sources as runtime_sources,
    store::{ECStore, list_objects::list_marker_key},
};
pub use local_snapshot::{LocalUsageSnapshot, read_snapshot as read_local_snapshot, snapshot_path};
use rustfs_data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, CompressionTotalInfo, DataUsageCache, DataUsageEntry, DataUsageInfo, DiskUsageStatus,
    SizeHistogram, SizeSummary, VersionsHistogram,
};
use rustfs_io_metrics::record_system_path_failure;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    future::Future,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::fs;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, instrument};

// Data usage storage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;
const DATA_USAGE_OBJ_NAME: &str = ".usage.json";
const DATA_COMPRESSION_TOTAL_NAME: &str = ".compression.json";
const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_TTL_SECS: u64 = 30;
/// Lifetime of a completed live recount. Deliberately not `DATA_USAGE_CACHE_TTL_SECS`:
/// a full `list_object_versions` walk of a large bucket costs minutes, so a 30s
/// entry would expire long before the next walk could replace it and every admin
/// poll would miss. Staleness stays bounded without a short TTL — local writes
/// invalidate via `invalidate_live_bucket_usage_cache`, and callers apply the
/// dirty memory overlay after the live entry, so only other nodes' writes can age
/// this value. That is still fresher than the scanner snapshot a miss falls back to.
const LIVE_BUCKET_USAGE_TTL_SECS: u64 = 300;
const LIVE_BUCKET_USAGE_MAX_ENTRIES: u64 = 1024;
/// Serialize background live bucket listings so cold multi-bucket admin polls
/// do not fan out concurrent full `list_object_versions` walks.
const LIVE_USAGE_REFRESH_PERMITS: usize = 1;
/// Hard outer bound for one background live recount *after* the semaphore permit
/// is acquired. Permit wait is intentionally uncapped so a deep refresh queue
/// does not time out before it starts; the timeout only fences wedged listings
/// that would otherwise pin the process-wide permit forever.
const LIVE_USAGE_REFRESH_TIMEOUT: Duration = Duration::from_secs(300);
/// Bound coalesce retries when an in-flight leader is superseded mid-refresh so
/// newer-epoch waiters are not poisoned by moka sharing one failed `try_get_with`.
const LIVE_USAGE_COALESCE_MAX_ATTEMPTS: usize = 4;

#[derive(Debug, Clone)]
struct CachedBucketUsage {
    usage: BucketUsageInfo,
    refreshed_at: SystemTime,
    usage_updated_at: SystemTime,
    // Set by request-path mutations until a scanner snapshot catches up to the same core counts.
    dirty: bool,
    // Set when a newer scanner snapshot was observed but did not include the dirty counts yet.
    stale_snapshot_pending: bool,
}

type UsageMemoryCache = Arc<RwLock<HashMap<String, CachedBucketUsage>>>;
type CacheUpdating = Arc<RwLock<bool>>;

#[derive(Debug, Clone)]
struct LiveBucketUsageEntry {
    usage: BucketUsageInfo,
    /// Generation at the time this entry was computed. Bumped on invalidate so
    /// an in-flight refresh that finishes after a write cannot poison the TTL cache.
    epoch: u64,
}

type LiveBucketUsageCache = moka::future::Cache<String, LiveBucketUsageEntry>;
type LiveBucketUsageEpochs = Arc<RwLock<HashMap<String, u64>>>;
/// Bucket -> unique owner id captured at insert; end-of-task remove only when still owner.
type LiveBucketUsageInFlight = Arc<RwLock<HashMap<String, u64>>>;

static USAGE_MEMORY_CACHE: OnceLock<UsageMemoryCache> = OnceLock::new();
static USAGE_CACHE_UPDATING: OnceLock<CacheUpdating> = OnceLock::new();
static LIVE_BUCKET_USAGE_CACHE: OnceLock<LiveBucketUsageCache> = OnceLock::new();
static LIVE_BUCKET_USAGE_EPOCHS: OnceLock<LiveBucketUsageEpochs> = OnceLock::new();
static LIVE_BUCKET_USAGE_IN_FLIGHT: OnceLock<LiveBucketUsageInFlight> = OnceLock::new();
static LIVE_BUCKET_USAGE_NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);
static LIVE_USAGE_REFRESH_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(LIVE_USAGE_REFRESH_PERMITS));

#[cfg(test)]
static LIVE_APPLY_HIT_TEST_HOOK: std::sync::Mutex<Option<Arc<LiveApplyHitTestHook>>> = std::sync::Mutex::new(None);

#[cfg(test)]
static LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK: std::sync::Mutex<Option<Arc<LiveSchedulePreEnsureTestHook>>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static LIVE_DISCARD_TEST_HOOK: std::sync::Mutex<Option<Arc<LiveDiscardTestHook>>> = std::sync::Mutex::new(None);

#[cfg(test)]
struct LiveApplyHitTestHook {
    /// Signalled once the apply path has a validated live hit and is about to pause.
    entered: tokio::sync::Notify,
    /// Test releases this to let apply continue after an intentional invalidate.
    release: tokio::sync::Notify,
}

#[cfg(test)]
struct LiveSchedulePreEnsureTestHook {
    /// Signalled once schedule's first still-owned check has passed and is about to
    /// pause before ensure/coalesce (exposes the still_owned -> ensure -> still_owned TOCTOU).
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
struct LiveDiscardTestHook {
    /// Signalled once discard removed the TTL entry and is about to decide re-insert.
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

/// Deferred persist thresholds for compression totals: persist after this many
/// operations recorded, but no more often than the min interval.
const COMPRESSION_PERSIST_BATCH_SIZE: u64 = 100;
const COMPRESSION_PERSIST_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// In-memory compression accumulator with debounced persistence state.
#[derive(Debug, Clone)]
struct CompressionTotalState {
    info: CompressionTotalInfo,
    /// Operations recorded since the last persist attempt.
    ops_since_persist: u64,
    /// When the last persist was attempted (used for min-interval gating).
    last_persist: tokio::time::Instant,
    /// When `true`, recording is skipped (embedded mode without observability).
    inited: bool,
}

impl Default for CompressionTotalState {
    fn default() -> Self {
        Self {
            info: CompressionTotalInfo::default(),
            ops_since_persist: 0,
            last_persist: tokio::time::Instant::now(),
            inited: false,
        }
    }
}

static COMPRESSION_TOTAL_MEMORY_CACHE: LazyLock<Arc<RwLock<Option<CompressionTotalState>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Some(CompressionTotalState::default()))));

fn memory_cache() -> &'static UsageMemoryCache {
    USAGE_MEMORY_CACHE.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

fn cache_updating() -> &'static CacheUpdating {
    USAGE_CACHE_UPDATING.get_or_init(|| Arc::new(RwLock::new(false)))
}

fn live_bucket_usage_cache() -> &'static LiveBucketUsageCache {
    LIVE_BUCKET_USAGE_CACHE.get_or_init(|| {
        moka::future::Cache::builder()
            .max_capacity(LIVE_BUCKET_USAGE_MAX_ENTRIES)
            .time_to_live(Duration::from_secs(LIVE_BUCKET_USAGE_TTL_SECS))
            .build()
    })
}

fn live_bucket_usage_epochs() -> &'static LiveBucketUsageEpochs {
    LIVE_BUCKET_USAGE_EPOCHS.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

fn live_bucket_usage_in_flight() -> &'static LiveBucketUsageInFlight {
    LIVE_BUCKET_USAGE_IN_FLIGHT.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

fn next_live_bucket_usage_owner_id() -> u64 {
    LIVE_BUCKET_USAGE_NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed)
}

/// True when this task still owns the in-flight slot and forget has not cleared it.
async fn live_bucket_usage_schedule_still_owned(bucket: &str, owner_id: u64) -> bool {
    live_bucket_usage_in_flight().read().await.get(bucket) == Some(&owner_id)
}

async fn release_live_bucket_usage_in_flight_if_owner(bucket: &str, owner_id: u64) {
    let mut inflight = live_bucket_usage_in_flight().write().await;
    if inflight.get(bucket) == Some(&owner_id) {
        inflight.remove(bucket);
    }
}

/// Current epoch when the bucket is still tracked. `None` means the bucket was
/// forgotten (deleted) so in-flight publishes must not revive a TTL entry.
async fn live_bucket_usage_epoch_if_tracked(bucket: &str) -> Option<u64> {
    live_bucket_usage_epochs().read().await.get(bucket).copied()
}

async fn live_bucket_usage_epoch(bucket: &str) -> u64 {
    live_bucket_usage_epoch_if_tracked(bucket).await.unwrap_or(0)
}

async fn ensure_live_bucket_usage_epoch(bucket: &str) -> u64 {
    let mut epochs = live_bucket_usage_epochs().write().await;
    *epochs.entry(bucket.to_string()).or_insert(0)
}

async fn bump_live_bucket_usage_epoch(bucket: &str) -> u64 {
    let mut epochs = live_bucket_usage_epochs().write().await;
    let entry = epochs.entry(bucket.to_string()).or_insert(0);
    *entry = entry.wrapping_add(1);
    *entry
}

async fn invalidate_live_bucket_usage_cache(bucket: &str) {
    bump_live_bucket_usage_epoch(bucket).await;
    live_bucket_usage_cache().invalidate(bucket).await;
}

/// Fence in-flight refreshes and drop epoch tracking for a deleted bucket so
/// create/delete churn cannot grow the epoch map without bound.
async fn forget_live_bucket_usage(bucket: &str) {
    bump_live_bucket_usage_epoch(bucket).await;
    live_bucket_usage_cache().invalidate(bucket).await;
    live_bucket_usage_epochs().write().await.remove(bucket);
    // Drop in-flight ownership so a same-name recreate can schedule; fenced owners
    // observe the cleared in-flight slot and exit without ensure/publish.
    live_bucket_usage_in_flight().write().await.remove(bucket);
}

/// Drop a superseded TTL entry only when it still matches `superseded_epoch`.
/// `remove` + conditional re-insert avoids wiping a newer winner published after
/// a plain get-then-invalidate race.
async fn discard_superseded_live_cache_entry(bucket: &str, superseded_epoch: u64) {
    let Some(cached) = live_bucket_usage_cache().remove(bucket).await else {
        return;
    };
    #[cfg(test)]
    {
        let hook = LIVE_DISCARD_TEST_HOOK.lock().expect("live discard hook lock").clone();
        if let Some(hook) = hook {
            hook.entered.notify_one();
            hook.release.notified().await;
        }
    }
    if cached.epoch == superseded_epoch {
        return;
    }
    // Restored only while that newer generation is still the tracked epoch.
    if live_bucket_usage_epoch_if_tracked(bucket).await == Some(cached.epoch) {
        live_bucket_usage_cache().insert(bucket.to_string(), cached).await;
    }
}

async fn current_live_bucket_usage_entry(bucket: &str) -> Option<LiveBucketUsageEntry> {
    let entry = live_bucket_usage_cache().get(bucket).await?;
    let current = live_bucket_usage_epoch_if_tracked(bucket).await?;
    if entry.epoch != current {
        return None;
    }
    // Re-check after the cache read so a concurrent invalidate/delete loses the race cleanly.
    if live_bucket_usage_epoch_if_tracked(bucket).await != Some(current) {
        return None;
    }
    Some(entry)
}

async fn cached_live_bucket_usage_if_current(bucket: &str) -> Option<BucketUsageInfo> {
    current_live_bucket_usage_entry(bucket).await.map(|entry| entry.usage)
}

// Data usage storage paths
lazy_static::lazy_static! {
    pub static ref DATA_USAGE_BUCKET: String = format!("{}{}{}",
        crate::disk::RUSTFS_META_BUCKET,
        SLASH_SEPARATOR,
        crate::disk::BUCKET_META_PREFIX
    );
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_OBJ_NAME
    );
    pub static ref DATA_USAGE_BLOOM_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_BLOOM_NAME
    );
    pub static ref DATA_COMPRESSION_TOTAL_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_COMPRESSION_TOTAL_NAME
    );
}

/// Store data usage info to backend storage
#[instrument(skip(store))]
pub async fn store_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<(), Error> {
    // Prevent older data from overwriting newer persisted stats
    if let Ok(buf) = read_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await
        && let Ok(existing) = serde_json::from_slice::<DataUsageInfo>(&buf)
        && let (Some(new_ts), Some(existing_ts)) = (data_usage_info.last_update, existing.last_update)
        && new_ts <= existing_ts
    {
        info!(
            "Skip persisting data usage: incoming last_update {:?} <= existing {:?}",
            new_ts, existing_ts
        );
        return Ok(());
    }

    save_data_usage_in_backend(data_usage_info, store).await
}

async fn save_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<(), Error> {
    let data =
        serde_json::to_vec(&data_usage_info).map_err(|e| Error::other(format!("Failed to serialize data usage info: {e}")))?;

    // Save to backend using the same mechanism as original code
    crate::config::com::save_config(store, &DATA_USAGE_OBJ_NAME_PATH, data)
        .await
        .map_err(Error::other)?;

    Ok(())
}

fn set_buckets_count_from_usage(data_usage_info: &mut DataUsageInfo) {
    data_usage_info.buckets_count = u64::try_from(data_usage_info.buckets_usage.len()).unwrap_or(u64::MAX);
}

fn remove_bucket_usage_from_info(data_usage_info: &mut DataUsageInfo, bucket: &str) -> bool {
    if bucket.is_empty() {
        return false;
    }

    let removed_usage = data_usage_info.buckets_usage.remove(bucket).is_some();
    let removed_size = data_usage_info.bucket_sizes.remove(bucket).is_some();

    if !removed_usage && !removed_size {
        return false;
    }

    set_buckets_count_from_usage(data_usage_info);
    data_usage_info.calculate_totals();
    true
}

fn merge_bucket_usage_removal(candidate: DataUsageInfo, existing: Option<DataUsageInfo>, bucket: &str) -> Option<DataUsageInfo> {
    let mut data_usage_info = match existing {
        Some(existing) if data_usage_info_updated_at(&existing) >= data_usage_info_updated_at(&candidate) => existing,
        _ => candidate,
    };

    if remove_bucket_usage_from_info(&mut data_usage_info, bucket) {
        Some(data_usage_info)
    } else {
        None
    }
}

async fn clear_bucket_usage_memory(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    memory_cache().write().await.remove(bucket);
}

pub async fn remove_bucket_usage_from_backend(store: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    forget_live_bucket_usage(bucket).await;
    clear_bucket_usage_memory(bucket).await;

    let data_usage_info = load_data_usage_from_backend(store.clone()).await?;
    let existing = load_data_usage_from_backend(store.clone()).await.ok();

    if let Some(data_usage_info) = merge_bucket_usage_removal(data_usage_info, existing, bucket) {
        save_data_usage_in_backend(data_usage_info, store).await?;
    }

    Ok(())
}

/// Load data usage info from backend storage
#[instrument(skip(store))]
pub async fn load_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            let reason = classify_system_path_failure_reason(&e);
            record_system_path_failure("data_usage", "read_primary", reason);
            error!(
                path_kind = "data_usage",
                operation = "read_primary",
                reason,
                object = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                error = %e,
                "system path read failed"
            );

            match read_config(store.clone(), format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()).as_str()).await {
                Ok(data) => data,
                Err(e) => {
                    if e == Error::ConfigNotFound {
                        return Ok(DataUsageInfo::default());
                    }
                    let reason = classify_system_path_failure_reason(&e);
                    record_system_path_failure("data_usage", "read_backup", reason);
                    error!(
                        path_kind = "data_usage",
                        operation = "read_backup",
                        reason,
                        object = %format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
                        error = %e,
                        "system path read failed"
                    );
                    return Err(Error::other(e));
                }
            }
        }
    };
    let mut data_usage_info: DataUsageInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize data usage info: {e}")))?;

    info!("Loaded data usage info from backend with {} buckets", data_usage_info.buckets_count);

    // Handle backward compatibility
    if data_usage_info.buckets_usage.is_empty() {
        data_usage_info.buckets_usage = data_usage_info
            .bucket_sizes
            .iter()
            .map(|(bucket, &size)| {
                (
                    bucket.clone(),
                    BucketUsageInfo {
                        size,
                        ..Default::default()
                    },
                )
            })
            .collect();
    }

    if data_usage_info.bucket_sizes.is_empty() {
        data_usage_info.bucket_sizes = data_usage_info
            .buckets_usage
            .iter()
            .map(|(bucket, bui)| (bucket.clone(), bui.size))
            .collect();
    }

    // Handle replication info
    for (bucket, bui) in &data_usage_info.buckets_usage {
        if (bui.replicated_size_v1 > 0
            || bui.replication_failed_count_v1 > 0
            || bui.replication_failed_size_v1 > 0
            || bui.replication_pending_count_v1 > 0)
            && let Ok((cfg, _)) = get_replication_config(bucket).await
            && !cfg.role.is_empty()
        {
            data_usage_info.replication_info.insert(
                cfg.role.clone(),
                BucketTargetUsageInfo {
                    replication_failed_size: bui.replication_failed_size_v1,
                    replication_failed_count: bui.replication_failed_count_v1,
                    replicated_size: bui.replicated_size_v1,
                    replication_pending_count: bui.replication_pending_count_v1,
                    replication_pending_size: bui.replication_pending_size_v1,
                    ..Default::default()
                },
            );
        }
    }

    Ok(data_usage_info)
}

/// Aggregate usage information from local disk snapshots.
fn merge_snapshot(aggregated: &mut DataUsageInfo, mut snapshot: LocalUsageSnapshot, latest_update: &mut Option<SystemTime>) {
    if let Some(update) = snapshot.last_update
        && latest_update.is_none_or(|current| update > current)
    {
        *latest_update = Some(update);
    }

    snapshot.recompute_totals();

    aggregated.objects_total_count = aggregated.objects_total_count.saturating_add(snapshot.objects_total_count);
    aggregated.versions_total_count = aggregated.versions_total_count.saturating_add(snapshot.versions_total_count);
    aggregated.delete_markers_total_count = aggregated
        .delete_markers_total_count
        .saturating_add(snapshot.delete_markers_total_count);
    aggregated.objects_total_size = aggregated.objects_total_size.saturating_add(snapshot.objects_total_size);

    for (bucket, usage) in snapshot.buckets_usage.into_iter() {
        let bucket_size = usage.size;
        match aggregated.buckets_usage.entry(bucket.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().merge(&usage),
            Entry::Vacant(entry) => {
                entry.insert(usage.clone());
            }
        }

        aggregated
            .bucket_sizes
            .entry(bucket)
            .and_modify(|size| *size = size.saturating_add(bucket_size))
            .or_insert(bucket_size);
    }
}

pub async fn aggregate_local_snapshots(store: Arc<ECStore>) -> Result<(Vec<DiskUsageStatus>, DataUsageInfo), Error> {
    let mut aggregated = DataUsageInfo::default();
    let mut latest_update: Option<SystemTime> = None;
    let mut statuses: Vec<DiskUsageStatus> = Vec::new();
    let mut processed_disks: HashSet<String> = HashSet::new();

    for (pool_idx, pool) in store.pools.iter().enumerate() {
        for set_disks in pool.disk_set.iter() {
            let disk_entries = {
                let guard = set_disks.disks.read().await;
                guard.clone()
            };

            for (disk_index, disk_opt) in disk_entries.into_iter().enumerate() {
                let Some(disk) = disk_opt else {
                    continue;
                };

                if !disk.is_local() {
                    continue;
                }

                let disk_id = match disk.get_disk_id().await.map_err(Error::from)? {
                    Some(id) => id.to_string(),
                    None => continue,
                };

                let root = disk.path();
                let disk_key = format!("{}|{}", disk.endpoint(), root.display());

                // Skip if we've already processed this physical disk
                if !processed_disks.insert(disk_key.clone()) {
                    continue;
                }

                let mut status = DiskUsageStatus {
                    disk_id: disk_id.clone(),
                    pool_index: Some(pool_idx),
                    set_index: Some(set_disks.set_index),
                    disk_index: Some(disk_index),
                    last_update: None,
                    snapshot_exists: false,
                };

                let snapshot_result = read_local_snapshot(root.as_path(), &disk_id).await;

                // If a snapshot is corrupted or unreadable, skip it but keep processing others
                if let Err(err) = &snapshot_result {
                    info!(
                        "Failed to read data usage snapshot for disk {} (pool {}, set {}, disk {}): {}",
                        disk_id, pool_idx, set_disks.set_index, disk_index, err
                    );
                    // Best-effort cleanup so next scan can rebuild a fresh snapshot instead of repeatedly failing
                    let snapshot_file = snapshot_path(root.as_path(), &disk_id);
                    if let Err(remove_err) = fs::remove_file(&snapshot_file).await
                        && remove_err.kind() != std::io::ErrorKind::NotFound
                    {
                        info!("Failed to remove corrupted snapshot {:?}: {}", snapshot_file, remove_err);
                    }
                }

                if let Ok(Some(mut snapshot)) = snapshot_result {
                    status.last_update = snapshot.last_update;
                    status.snapshot_exists = true;

                    if snapshot.meta.disk_id.is_empty() {
                        snapshot.meta.disk_id = disk_id.clone();
                    }
                    if snapshot.meta.pool_index.is_none() {
                        snapshot.meta.pool_index = Some(pool_idx);
                    }
                    if snapshot.meta.set_index.is_none() {
                        snapshot.meta.set_index = Some(set_disks.set_index);
                    }
                    if snapshot.meta.disk_index.is_none() {
                        snapshot.meta.disk_index = Some(disk_index);
                    }

                    merge_snapshot(&mut aggregated, snapshot, &mut latest_update);
                }

                statuses.push(status);
            }
        }
    }

    aggregated.buckets_count = aggregated.buckets_usage.len() as u64;
    aggregated.last_update = latest_update;
    aggregated.disk_usage_status = statuses.clone();

    Ok((statuses, aggregated))
}

/// Calculate accurate bucket usage statistics by enumerating objects through the object layer.
#[derive(Default)]
struct BucketUsageAccumulator {
    current_object_name: Option<String>,
    // FileMeta caps versions per object, so replay detection remains bounded.
    current_object_versions: HashSet<Option<[u8; 16]>>,
    current_live_versions: u64,
    objects_count: u64,
    versions_count: u64,
    total_size: u64,
    delete_markers: u64,
    size_histogram: SizeHistogram,
    versions_histogram: VersionsHistogram,
}

impl BucketUsageAccumulator {
    fn record(&mut self, bucket: &str, object: &ObjectInfo) -> Result<(), Error> {
        if object.is_dir {
            return Ok(());
        }

        if self
            .current_object_name
            .as_deref()
            .is_some_and(|current_name| object.name.as_str() != current_name)
        {
            self.finish_current_object();
        }

        record_version_listing_entry(
            bucket,
            &mut self.current_object_name,
            &mut self.current_object_versions,
            &object.name,
            object.version_id.as_ref().map(|version_id| version_id.as_bytes()),
        )?;

        if object.delete_marker {
            self.delete_markers = self.delete_markers.saturating_add(1);
            return Ok(());
        }

        let object_size = object.size.max(0) as u64;
        self.current_live_versions = self.current_live_versions.saturating_add(1);
        self.size_histogram.add(object_size);
        self.total_size = self.total_size.saturating_add(object_size);
        self.versions_count = self.versions_count.saturating_add(1);
        Ok(())
    }

    fn finish_current_object(&mut self) {
        if self.current_live_versions > 0 {
            self.objects_count = self.objects_count.saturating_add(1);
            self.versions_histogram.add(self.current_live_versions);
        }
        self.current_live_versions = 0;
    }

    fn finish(mut self) -> BucketUsageInfo {
        self.finish_current_object();
        BucketUsageInfo {
            size: self.total_size,
            objects_count: self.objects_count,
            versions_count: self.versions_count,
            delete_markers_count: self.delete_markers,
            object_size_histogram: self.size_histogram.to_map(),
            object_versions_histogram: self.versions_histogram.to_map(),
            ..Default::default()
        }
    }
}

type UsageVersionPage = StorageListObjectVersionsInfo<ObjectInfo>;

pub async fn compute_bucket_usage(store: Arc<ECStore>, bucket_name: &str) -> Result<BucketUsageInfo, Error> {
    let bucket = bucket_name.to_string();
    compute_bucket_usage_with_pages(bucket_name, move |marker, version_marker| {
        let store = Arc::clone(&store);
        let bucket = bucket.clone();
        async move {
            store
                .list_object_versions(&bucket, "", marker, version_marker, None, 1000)
                .await
        }
    })
    .await
}

async fn compute_bucket_usage_with_pages<F, Fut>(bucket_name: &str, mut fetch_page: F) -> Result<BucketUsageInfo, Error>
where
    F: FnMut(Option<String>, Option<String>) -> Fut,
    Fut: Future<Output = Result<UsageVersionPage, Error>>,
{
    let mut marker: Option<String> = None;
    let mut version_marker: Option<String> = None;
    let mut usage = BucketUsageAccumulator::default();

    loop {
        let result = fetch_page(marker.clone(), version_marker.clone()).await?;

        let page_entries = result.objects.len();
        for object in result.objects.iter() {
            usage.record(bucket_name, object)?;
        }

        if !result.is_truncated {
            break;
        }
        ensure_truncated_version_page_has_entries(bucket_name, page_entries)?;

        advance_version_listing_cursor(
            bucket_name,
            &mut marker,
            &mut version_marker,
            result.next_marker,
            result.next_version_idmarker,
        )?;
    }

    Ok(usage.finish())
}

fn ensure_truncated_version_page_has_entries(bucket: &str, page_entries: usize) -> Result<(), Error> {
    if page_entries == 0 {
        return Err(Error::other(format!("bucket {bucket} version listing returned an empty truncated page")));
    }
    Ok(())
}

fn record_version_listing_entry(
    bucket: &str,
    current_object_name: &mut Option<String>,
    current_object_versions: &mut HashSet<Option<[u8; 16]>>,
    object_name: &str,
    version_id: Option<&[u8; 16]>,
) -> Result<(), Error> {
    match current_object_name.as_deref() {
        Some(current_name) if object_name < current_name => {
            return Err(Error::other(format!("bucket {bucket} version listing returned an out-of-order object")));
        }
        Some(current_name) if object_name > current_name => {
            current_object_versions.clear();
            *current_object_name = Some(object_name.to_string());
        }
        None => *current_object_name = Some(object_name.to_string()),
        Some(_) => {}
    }

    if !current_object_versions.insert(version_id.copied()) {
        return Err(Error::other(format!(
            "bucket {bucket} version listing returned a repeated object version"
        )));
    }
    Ok(())
}

fn advance_version_listing_cursor(
    bucket: &str,
    marker: &mut Option<String>,
    version_marker: &mut Option<String>,
    next_marker: Option<String>,
    next_version_marker: Option<String>,
) -> Result<(), Error> {
    let next_marker = next_marker
        .filter(|next_marker| !next_marker.is_empty())
        .ok_or_else(|| Error::other(format!("bucket {bucket} version listing was truncated without a key marker")))?;
    let next_version_marker = next_version_marker
        .filter(|next_version_marker| !next_version_marker.is_empty())
        .ok_or_else(|| Error::other(format!("bucket {bucket} version listing was truncated without a version marker")))?;
    let current_key_marker = marker.as_deref().map(list_marker_key);
    let next_key_marker = list_marker_key(&next_marker);
    if current_key_marker == Some(next_key_marker) && version_marker.as_deref() == Some(next_version_marker.as_str()) {
        return Err(Error::other(format!(
            "bucket {bucket} version listing returned a repeated continuation marker"
        )));
    }
    if current_key_marker.is_some_and(|marker| next_key_marker < marker) {
        return Err(Error::other(format!("bucket {bucket} version listing returned a regressing key marker")));
    }
    *marker = Some(next_marker);
    *version_marker = Some(next_version_marker);
    Ok(())
}

/// Why a coalesced live refresh did not publish. `Superseded` drives the retry
/// decision, so it must stay a distinct variant rather than an error-text match:
/// routing control flow on message prose breaks the moment a message is reworded.
#[derive(Debug)]
enum LiveRefreshError {
    /// A write/delete bumped the epoch (or forgot the bucket) mid-refresh.
    Superseded,
    /// The underlying object-layer listing itself failed.
    Failed(Error),
}

impl std::fmt::Display for LiveRefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiveRefreshError::Superseded => write!(f, "live usage refresh was superseded"),
            LiveRefreshError::Failed(err) => write!(f, "{err}"),
        }
    }
}

async fn coalesce_live_bucket_usage<F, Fut>(bucket: String, make_init: F) -> Result<BucketUsageInfo, Error>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<BucketUsageInfo, Error>> + Send + 'static,
{
    for _attempt in 0..LIVE_USAGE_COALESCE_MAX_ATTEMPTS {
        if let Some(entry) = current_live_bucket_usage_entry(&bucket).await {
            return Ok(entry.usage);
        }

        // Callers that need first-seen tracking must ensure before coalesce; an
        // untracked bucket here means forget fenced the refresh — never resurrect.
        let Some(epoch) = live_bucket_usage_epoch_if_tracked(&bucket).await else {
            return Err(Error::other(format!("live usage for {bucket} was superseded")));
        };
        let init_bucket = bucket.clone();
        let init_fut = make_init();
        let result = live_bucket_usage_cache()
            .try_get_with(bucket.clone(), async move {
                let usage = init_fut.await.map_err(LiveRefreshError::Failed)?;
                // A write/delete may have invalidated or forgotten us while listing.
                let Some(current) = live_bucket_usage_epoch_if_tracked(&init_bucket).await else {
                    return Err(LiveRefreshError::Superseded);
                };
                if current != epoch {
                    return Err(LiveRefreshError::Superseded);
                }
                Ok(LiveBucketUsageEntry { usage, epoch })
            })
            .await;

        match result {
            Ok(entry) => {
                if live_bucket_usage_epoch_if_tracked(&bucket).await != Some(entry.epoch) {
                    discard_superseded_live_cache_entry(&bucket, entry.epoch).await;
                    continue;
                }
                return Ok(entry.usage);
            }
            Err(err) => match &*err {
                LiveRefreshError::Superseded => {
                    // Retry only while the bucket is still tracked (invalidate). After
                    // forget(), the epoch key is gone — do not resurrect it via ensure.
                    if live_bucket_usage_epoch_if_tracked(&bucket).await.is_none() {
                        return Err(Error::other(format!("live usage for {bucket} was superseded")));
                    }
                    continue;
                }
                LiveRefreshError::Failed(inner) => return Err(Error::other(inner.to_string())),
            },
        }
    }
    Err(Error::other(format!("live usage for {bucket} was superseded")))
}

fn apply_live_bucket_usage_to_response(data_usage_info: &mut DataUsageInfo, bucket: &str, usage: &BucketUsageInfo) {
    data_usage_info.bucket_sizes.insert(bucket.to_string(), usage.size);
    data_usage_info.buckets_usage.insert(bucket.to_string(), usage.clone());
    set_buckets_count_from_usage(data_usage_info);
}

async fn run_live_usage_refresh_under_permit_with_timeout<F>(timeout: Duration, init: F) -> Result<BucketUsageInfo, Error>
where
    F: Future<Output = Result<BucketUsageInfo, Error>> + Send,
{
    let _permit = LIVE_USAGE_REFRESH_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| Error::other("live usage refresh semaphore closed"))?;
    tokio::time::timeout(timeout, init)
        .await
        .map_err(|_| Error::other("live usage refresh timed out"))?
}

async fn run_live_usage_refresh_under_permit<F>(init: F) -> Result<BucketUsageInfo, Error>
where
    F: Future<Output = Result<BucketUsageInfo, Error>> + Send,
{
    run_live_usage_refresh_under_permit_with_timeout(LIVE_USAGE_REFRESH_TIMEOUT, init).await
}

fn schedule_live_bucket_usage_refresh<F, Fut>(bucket: &str, make_init: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<BucketUsageInfo, Error>> + Send + 'static,
{
    let bucket_name = bucket.to_string();
    tokio::spawn(async move {
        let owner_id = next_live_bucket_usage_owner_id();
        {
            let mut inflight = live_bucket_usage_in_flight().write().await;
            if inflight.insert(bucket_name.clone(), owner_id).is_some() {
                // Another AccountInfo/DataUsageInfo poll already scheduled this bucket.
                return;
            }
        }

        if !live_bucket_usage_schedule_still_owned(&bucket_name, owner_id).await {
            release_live_bucket_usage_in_flight_if_owner(&bucket_name, owner_id).await;
            return;
        }

        #[cfg(test)]
        {
            let hook = LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK
                .lock()
                .expect("live schedule pre-ensure hook lock")
                .clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }

        if live_bucket_usage_epoch_if_tracked(&bucket_name).await.is_none() {
            ensure_live_bucket_usage_epoch(&bucket_name).await;
        }

        if !live_bucket_usage_schedule_still_owned(&bucket_name, owner_id).await {
            // forget() may have cleared in-flight after ensure resurrected the epoch above (TOCTOU
            // between the still-owned check and ensure). Undo the resurrection only when no
            // newer owner has claimed in-flight, otherwise we would wipe the recreate's tracking.
            if live_bucket_usage_in_flight().read().await.get(&bucket_name).is_none() {
                live_bucket_usage_epochs().write().await.remove(&bucket_name);
            }
            release_live_bucket_usage_in_flight_if_owner(&bucket_name, owner_id).await;
            return;
        }

        let result = coalesce_live_bucket_usage(bucket_name.clone(), || {
            let init = make_init();
            async move { run_live_usage_refresh_under_permit(init).await }
        })
        .await;

        release_live_bucket_usage_in_flight_if_owner(&bucket_name, owner_id).await;

        if let Err(err) = result {
            debug!(
                bucket = %bucket_name,
                error = %err,
                "background live bucket usage refresh failed"
            );
        }
    });
}

async fn apply_cached_live_bucket_usage_or_schedule_refresh<F, Fut>(
    data_usage_info: &mut DataUsageInfo,
    bucket: &str,
    make_init: F,
) where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<BucketUsageInfo, Error>> + Send + 'static,
{
    if let Some(entry) = current_live_bucket_usage_entry(bucket).await {
        let entry_epoch = entry.epoch;

        #[cfg(test)]
        {
            let hook = LIVE_APPLY_HIT_TEST_HOOK.lock().expect("live apply hit hook lock").clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }

        let prior_usage = data_usage_info.buckets_usage.get(bucket).cloned();
        let prior_size = data_usage_info.bucket_sizes.get(bucket).copied();
        apply_live_bucket_usage_to_response(data_usage_info, bucket, &entry.usage);
        // Delete/invalidate may have raced after the cache read; restore scanner/overlay
        // state. Compare against the entry's epoch (not a post-read resample of "current"),
        // otherwise an invalidate between get and resample is invisible.
        if live_bucket_usage_epoch_if_tracked(bucket).await != Some(entry_epoch) {
            match prior_usage {
                Some(usage) => {
                    data_usage_info.buckets_usage.insert(bucket.to_string(), usage);
                }
                None => {
                    data_usage_info.buckets_usage.remove(bucket);
                }
            }
            match prior_size {
                Some(size) => {
                    data_usage_info.bucket_sizes.insert(bucket.to_string(), size);
                }
                None => {
                    data_usage_info.bucket_sizes.remove(bucket);
                }
            }
            set_buckets_count_from_usage(data_usage_info);
            data_usage_info.calculate_totals();
            // Invalidate still tracks the bucket; forget leaves epoch untracked and must
            // not schedule a refresh that would resurrect TTL state for a deleted bucket.
            if live_bucket_usage_epoch_if_tracked(bucket).await.is_some() {
                schedule_live_bucket_usage_refresh(bucket, make_init);
            }
        }
        return;
    }
    schedule_live_bucket_usage_refresh(bucket, make_init);
}

/// Apply any TTL-cached live bucket usage into the response and schedule a
/// background object-layer recount on cache miss.
///
/// Admin handlers must not await a full `list_object_versions` walk on the
/// request path (rustfs/rustfs#4902). Completed live results stay response-local
/// and are not promoted into the quota memory cache.
pub async fn apply_cached_or_schedule_live_bucket_usage(store: Arc<ECStore>, data_usage_info: &mut DataUsageInfo, bucket: &str) {
    let bucket_name = bucket.to_string();
    apply_cached_live_bucket_usage_or_schedule_refresh(data_usage_info, bucket, move || {
        let store = store.clone();
        let bucket_name = bucket_name.clone();
        async move { compute_bucket_usage(store, &bucket_name).await }
    })
    .await;
}

pub async fn refresh_bucket_usage_from_object_layer(
    store: Arc<ECStore>,
    data_usage_info: &mut DataUsageInfo,
    bucket: &str,
) -> Result<BucketUsageInfo, Error> {
    let bucket_name = bucket.to_string();
    ensure_live_bucket_usage_epoch(&bucket_name).await;
    let usage = coalesce_live_bucket_usage(bucket_name.clone(), {
        let store = store.clone();
        move || {
            let store = store.clone();
            let bucket_name = bucket_name.clone();
            async move { compute_bucket_usage(store, &bucket_name).await }
        }
    })
    .await?;
    // Request-time listings are not linearizable with writes on other nodes.
    // Keep the live result response-local instead of promoting it into the quota cache.
    apply_live_bucket_usage_to_response(data_usage_info, bucket, &usage);
    data_usage_info.calculate_totals();
    Ok(usage)
}

pub async fn refresh_versioned_bucket_usage_from_object_layer(store: Arc<ECStore>, data_usage_info: &mut DataUsageInfo) {
    let listed_bucket_names = match store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(buckets) => buckets.into_iter().map(|bucket| bucket.name).collect::<Vec<_>>(),
        Err(err) => {
            debug!(error = %err, "failed to list buckets while refreshing versioned bucket usage");
            Vec::new()
        }
    };
    let mut buckets = data_usage_info.buckets_usage.keys().cloned().collect::<HashSet<String>>();
    buckets.extend(listed_bucket_names.into_iter().filter(|bucket| !bucket.is_empty()));
    let mut buckets = buckets.into_iter().collect::<Vec<_>>();
    buckets.sort();

    for bucket in buckets {
        let Ok(versioning) = BucketVersioningSys::get(&bucket).await else {
            continue;
        };
        if !versioning.enabled() && !versioning.suspended() {
            continue;
        }
        if let Err(err) = refresh_bucket_usage_from_object_layer(store.clone(), data_usage_info, &bucket).await {
            debug!(
                bucket = %bucket,
                error = %err,
                "failed to refresh versioned bucket usage from object layer"
            );
        }
    }
}

async fn ensure_bucket_usage_cached(bucket: &str) {
    let cache = memory_cache().read().await;
    if cache.contains_key(bucket) {
        return;
    }
    drop(cache);

    update_usage_cache_if_needed().await;
}

fn cached_bucket_usage_from_backend(usage: BucketUsageInfo, updated_at: SystemTime) -> CachedBucketUsage {
    CachedBucketUsage {
        usage,
        refreshed_at: SystemTime::now(),
        usage_updated_at: updated_at,
        dirty: false,
        stale_snapshot_pending: false,
    }
}

fn cached_bucket_usage_now(usage: BucketUsageInfo) -> CachedBucketUsage {
    let now = SystemTime::now();
    CachedBucketUsage {
        usage,
        refreshed_at: now,
        usage_updated_at: now,
        dirty: false,
        stale_snapshot_pending: false,
    }
}

fn data_usage_info_updated_at(data_usage_info: &DataUsageInfo) -> SystemTime {
    data_usage_info.last_update.unwrap_or(SystemTime::UNIX_EPOCH)
}

fn bucket_usage_counts_match(left: &BucketUsageInfo, right: &BucketUsageInfo) -> bool {
    left.size == right.size
        && left.objects_count == right.objects_count
        && left.versions_count == right.versions_count
        && left.delete_markers_count == right.delete_markers_count
}

#[cfg(test)]
async fn replace_bucket_usage_memory_from_authoritative(bucket: &str, usage: BucketUsageInfo, refresh_started_at: SystemTime) {
    let mut cache = memory_cache().write().await;
    if let Some(existing) = cache.get(bucket)
        && existing.usage_updated_at > refresh_started_at
    {
        return;
    }

    cache.insert(bucket.to_string(), cached_bucket_usage_from_backend(usage, refresh_started_at));
}

/// Fast in-memory update for immediate quota and admin usage consistency.
pub async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    record_bucket_object_write_memory_inner(bucket, previous_current_size, new_size, false).await;
}

/// Fast in-memory update for versioned object writes.
pub async fn record_bucket_object_version_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    record_bucket_object_write_memory_inner(bucket, previous_current_size, new_size, true).await;
}

async fn record_bucket_object_write_memory_inner(
    bucket: &str,
    previous_current_size: Option<u64>,
    new_size: u64,
    creates_new_version: bool,
) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    if creates_new_version {
        entry.usage.size = entry.usage.size.saturating_add(new_size);
        if previous_current_size.is_none() {
            entry.usage.objects_count = entry.usage.objects_count.saturating_add(1);
        }
        entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
    } else {
        match previous_current_size {
            Some(previous_size) => {
                entry.usage.size = entry.usage.size.saturating_sub(previous_size).saturating_add(new_size);
            }
            None => {
                entry.usage.size = entry.usage.size.saturating_add(new_size);
                entry.usage.objects_count = entry.usage.objects_count.saturating_add(1);
                entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
            }
        }
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    drop(cache);
    invalidate_live_bucket_usage_cache(bucket).await;
}

/// Degraded in-memory update for an object write whose previous current size
/// could not be determined (rustfs/backlog#1009: the pre-PUT lookup was
/// skipped and the rename_data backfill came back unknown — mixed-version
/// peers or sub-quorum metadata divergence). Applies only the components that
/// are correct regardless of the previous state: the new bytes always count,
/// and a versioned write always adds a version. objects_count (and the
/// non-versioned overwrite's old-size subtraction) are left to the next
/// scanner refresh, which replaces this cache with authoritative numbers.
pub async fn record_bucket_object_write_unknown_previous_memory(bucket: &str, new_size: u64, creates_new_version: bool) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.size = entry.usage.size.saturating_add(new_size);
    if creates_new_version {
        entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    drop(cache);
    invalidate_live_bucket_usage_cache(bucket).await;
}

/// Fast in-memory increment for immediate quota consistency.
pub async fn increment_bucket_usage_memory(bucket: &str, size_increment: u64) {
    record_bucket_object_write_memory(bucket, None, size_increment).await;
}

/// Fast in-memory update for successful object deletes.
pub async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.size = entry.usage.size.saturating_sub(deleted_size);
    if removed_current_object {
        entry.usage.objects_count = entry.usage.objects_count.saturating_sub(1);
        entry.usage.versions_count = entry.usage.versions_count.saturating_sub(1);
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    drop(cache);
    invalidate_live_bucket_usage_cache(bucket).await;
}

/// Fast in-memory update for successful delete marker creation.
pub async fn record_bucket_delete_marker_memory(bucket: &str) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.delete_markers_count = entry.usage.delete_markers_count.saturating_add(1);

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
    drop(cache);
    invalidate_live_bucket_usage_cache(bucket).await;
}

/// Fast in-memory decrement for immediate quota consistency
pub async fn decrement_bucket_usage_memory(bucket: &str, size_decrement: u64) {
    record_bucket_object_delete_memory(bucket, size_decrement, size_decrement > 0).await;
}

/// Get bucket usage from in-memory cache
pub async fn get_bucket_usage_memory(bucket: &str) -> Option<u64> {
    update_usage_cache_if_needed().await;

    let cache = memory_cache().read().await;
    cache.get(bucket).map(|cached| cached.usage.size)
}

async fn update_usage_cache_if_needed() {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);
    let double_ttl = ttl * 2;
    let now = SystemTime::now();

    let cache = memory_cache().read().await;
    let earliest_timestamp = cache.values().map(|cached| cached.refreshed_at).min();
    drop(cache);

    let age = match earliest_timestamp {
        Some(ts) => now.duration_since(ts).unwrap_or_default(),
        None => double_ttl,
    };

    if age < ttl {
        return;
    }

    let mut updating = cache_updating().write().await;
    if age < double_ttl {
        if *updating {
            return;
        }
        *updating = true;
        drop(updating);

        let updating_clone = (*cache_updating()).clone();
        tokio::spawn(async move {
            if let Some(store) = runtime_sources::object_store_handle()
                && let Ok(data_usage_info) = load_data_usage_from_backend(store.clone()).await
            {
                replace_bucket_usage_memory_from_info(&data_usage_info).await;
            }
            let mut updating = updating_clone.write().await;
            *updating = false;
        });
        return;
    }

    for retry in 0..10 {
        if !*updating {
            break;
        }
        drop(updating);
        let delay = Duration::from_millis(1 << retry);
        tokio::time::sleep(delay).await;
        updating = cache_updating().write().await;
    }

    *updating = true;
    drop(updating);

    if let Some(store) = runtime_sources::object_store_handle()
        && let Ok(data_usage_info) = load_data_usage_from_backend(store.clone()).await
    {
        replace_bucket_usage_memory_from_info(&data_usage_info).await;
    }

    let mut updating = cache_updating().write().await;
    *updating = false;
}

pub async fn replace_bucket_usage_memory_from_info(data_usage_info: &DataUsageInfo) {
    let usage_updated_at = data_usage_info_updated_at(data_usage_info);
    let mut cache = memory_cache().write().await;
    let mut next_cache = HashMap::new();

    for (bucket, bucket_usage) in data_usage_info.buckets_usage.iter() {
        if let Some(existing) = cache.get(bucket) {
            if existing.usage_updated_at > usage_updated_at {
                next_cache.insert(bucket.clone(), existing.clone());
                continue;
            }

            if existing.dirty && !bucket_usage_counts_match(&existing.usage, bucket_usage) {
                // A scanner snapshot can be saved after newer writes but still miss them if it listed the bucket earlier.
                let mut preserved = existing.clone();
                preserved.stale_snapshot_pending = true;
                next_cache.insert(bucket.clone(), preserved);
                continue;
            }
        }

        next_cache.insert(bucket.clone(), cached_bucket_usage_from_backend(bucket_usage.clone(), usage_updated_at));
    }

    for (bucket, existing) in cache.iter() {
        if !data_usage_info.buckets_usage.contains_key(bucket) {
            if existing.usage_updated_at > usage_updated_at {
                next_cache.insert(bucket.clone(), existing.clone());
                continue;
            }

            if existing.dirty {
                let mut preserved = existing.clone();
                preserved.stale_snapshot_pending = true;
                next_cache.insert(bucket.clone(), preserved);
            }
        }
    }

    *cache = next_cache;
}

pub async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut DataUsageInfo) {
    let cache = memory_cache().read().await;
    if cache.is_empty() {
        return;
    }

    let persisted_update = data_usage_info.last_update;
    let mut changed = false;

    for (bucket, cached) in cache.iter() {
        if !cached.stale_snapshot_pending && persisted_update.is_some_and(|persisted| cached.usage_updated_at <= persisted) {
            continue;
        }

        data_usage_info.buckets_usage.insert(bucket.clone(), cached.usage.clone());
        data_usage_info.bucket_sizes.insert(bucket.clone(), cached.usage.size);
        changed = true;
    }

    if changed {
        data_usage_info.buckets_count = data_usage_info.buckets_usage.len() as u64;
        data_usage_info.calculate_totals();
    }
}

/// Sync memory cache with backend data (called by scanner)
pub async fn sync_memory_cache_with_backend() -> Result<(), Error> {
    if let Some(store) = runtime_sources::object_store_handle() {
        match load_data_usage_from_backend(store.clone()).await {
            Ok(data_usage_info) => {
                replace_bucket_usage_memory_from_info(&data_usage_info).await;
            }
            Err(e) => {
                debug!("Failed to sync memory cache with backend: {}", e);
            }
        }
    }
    Ok(())
}

/// Create a data usage cache entry from size summary
pub fn create_cache_entry_from_summary(summary: &SizeSummary) -> DataUsageEntry {
    let mut entry = DataUsageEntry::default();
    entry.add_sizes(summary);
    entry
}

/// Convert data usage cache to DataUsageInfo
pub fn cache_to_data_usage_info(
    cache: &DataUsageCache,
    path: &str,
    buckets: &[crate::storage_api_contracts::bucket::BucketInfo],
) -> DataUsageInfo {
    let e = match cache.find(path) {
        Some(e) => e,
        None => return DataUsageInfo::default(),
    };
    let flat = cache.flatten(&e);

    let mut buckets_usage = HashMap::new();
    for bucket in buckets.iter() {
        let e = match cache.find(&bucket.name) {
            Some(e) => e,
            None => continue,
        };
        let flat = cache.flatten(&e);
        let mut bui = BucketUsageInfo {
            size: flat.size as u64,
            versions_count: flat.versions as u64,
            objects_count: flat.objects as u64,
            delete_markers_count: flat.delete_markers as u64,
            object_size_histogram: flat.obj_sizes.to_map(),
            object_versions_histogram: flat.obj_versions.to_map(),
            ..Default::default()
        };

        if let Some(rs) = &flat.replication_stats {
            bui.replica_size = rs.replica_size;
            bui.replica_count = rs.replica_count;

            for (arn, stat) in rs.targets.iter() {
                bui.replication_info.insert(
                    arn.clone(),
                    BucketTargetUsageInfo {
                        replication_pending_size: stat.pending_size,
                        replicated_size: stat.replicated_size,
                        replication_failed_size: stat.failed_size,
                        replication_pending_count: stat.pending_count,
                        replication_failed_count: stat.failed_count,
                        replicated_count: stat.replicated_count,
                        ..Default::default()
                    },
                );
            }
        }
        buckets_usage.insert(bucket.name.clone(), bui);
    }

    DataUsageInfo {
        last_update: cache.info.last_update,
        objects_total_count: flat.objects as u64,
        versions_total_count: flat.versions as u64,
        delete_markers_total_count: flat.delete_markers as u64,
        objects_total_size: flat.size as u64,
        buckets_count: e.children.len() as u64,
        buckets_usage,
        ..Default::default()
    }
}

// Helper functions for DataUsageCache operations
pub async fn load_data_usage_cache(store: &crate::set_disk::SetDisks, name: &str) -> crate::error::Result<DataUsageCache> {
    use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
    use crate::object_api::ObjectOptions;
    use http::HeaderMap;
    use rand::RngExt;
    use std::path::Path;
    use std::time::Duration;
    use tokio::time::sleep;

    let mut d = DataUsageCache::default();
    let mut retries = 0;
    while retries < 5 {
        let path = Path::new(BUCKET_META_PREFIX).join(name);
        match store
            .get_object_reader(
                RUSTFS_META_BUCKET,
                path.to_str().unwrap(),
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(mut reader) => {
                if let Ok(info) = DataUsageCache::unmarshal(&reader.read_all().await?) {
                    d = info
                }
                break;
            }
            Err(err) => match err {
                Error::FileNotFound | Error::VolumeNotFound => {
                    match store
                        .get_object_reader(
                            RUSTFS_META_BUCKET,
                            name,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                no_lock: true,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(mut reader) => {
                            if let Ok(info) = DataUsageCache::unmarshal(&reader.read_all().await?) {
                                d = info
                            }
                            break;
                        }
                        Err(_) => match err {
                            Error::FileNotFound | Error::VolumeNotFound => {
                                break;
                            }
                            _ => {}
                        },
                    }
                }
                _ => {
                    break;
                }
            },
        }
        retries += 1;
        let dur = {
            let mut rng = rand::rng();
            rng.random_range(0..1_000)
        };
        sleep(Duration::from_millis(dur)).await;
    }
    Ok(d)
}

#[instrument(skip(cache))]
pub async fn save_data_usage_cache(cache: &DataUsageCache, name: &str) -> crate::error::Result<()> {
    use crate::config::com::save_config;
    use crate::disk::BUCKET_META_PREFIX;
    use std::path::Path;

    let Some(store) = runtime_sources::object_store_handle() else {
        return Err(Error::other("errServerNotInitialized"));
    };
    let buf = cache.marshal_msg().map_err(Error::other)?;
    let buf_clone = buf.clone();

    let store_clone = store.clone();

    let name = Path::new(BUCKET_META_PREFIX).join(name).to_string_lossy().to_string();

    let name_clone = name.clone();
    tokio::spawn(async move {
        let _ = save_config(store_clone, &format!("{}{}", name_clone, ".bkp"), buf_clone).await;
    });
    save_config(store, &name, buf).await?;
    Ok(())
}

/// Persist the current in-memory compression total to the backend.
/// Resets the debounce counter so the next auto-persist won't fire
/// immediately after this manual flush (intended for shutdown paths).
/// Storage will not be triggered when uninitialized.
pub async fn store_compression_total_in_backend() {
    let snapshot = {
        let mut guard = COMPRESSION_TOTAL_MEMORY_CACHE.write().await;
        let Some(state) = guard.as_mut() else {
            return;
        };
        if !state.inited {
            return;
        }
        state.ops_since_persist = 0;
        state.last_persist = tokio::time::Instant::now();
        state.info.clone()
    };
    try_flush_compression_total(&snapshot).await;
}

/// Load compression total info from backend storage
#[instrument(skip(store))]
pub async fn load_compression_total_from_backend(store: Arc<ECStore>) -> Result<CompressionTotalInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_COMPRESSION_TOTAL_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            if e == Error::ConfigNotFound {
                return Ok(CompressionTotalInfo::default());
            }
            let reason = classify_system_path_failure_reason(&e);
            record_system_path_failure("compression_total", "read_primary", reason);
            error!(
                path_kind = "compression_total",
                operation = "read_primary",
                reason,
                object = %DATA_COMPRESSION_TOTAL_NAME_PATH.as_str(),
                error = %e,
                "system path read failed"
            );
            return Err(Error::other(e));
        }
    };
    let compression_total: CompressionTotalInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize compression total info: {e}")))?;

    info!(
        "Loaded compression total info: original={}, compressed={}, operations={}",
        compression_total.original_bytes_total,
        compression_total.compressed_bytes_total,
        compression_total.compression_operations_total
    );

    Ok(compression_total)
}

pub async fn load_compression_total_from_memory() -> Option<CompressionTotalInfo> {
    COMPRESSION_TOTAL_MEMORY_CACHE.read().await.as_ref().map(|s| s.info.clone())
}

/// Record a compression operation. Accumulates totals in memory and triggers a
/// background persist to backend after every `COMPRESSION_PERSIST_BATCH_SIZE`
/// operations, gated by a minimum interval to avoid excessive IO under high
/// throughput.
pub async fn record_compression_total_memory(original_size: u64, compressed_size: u64) {
    let mut guard = COMPRESSION_TOTAL_MEMORY_CACHE.write().await;
    let Some(state) = guard.as_mut() else {
        error!("compression total memory cache not initialized, discarding record");
        return;
    };
    // Compression totals are only recorded while this cache is initialized.
    if !state.inited {
        return;
    }
    state.info.original_bytes_total = state.info.original_bytes_total.saturating_add(original_size);
    state.info.compressed_bytes_total = state.info.compressed_bytes_total.saturating_add(compressed_size);
    state.info.compression_operations_total = state.info.compression_operations_total.saturating_add(1);
    state.ops_since_persist = state.ops_since_persist.saturating_add(1);
    if state.ops_since_persist >= COMPRESSION_PERSIST_BATCH_SIZE
        || state.last_persist.elapsed() >= COMPRESSION_PERSIST_MIN_INTERVAL
    {
        state.ops_since_persist = 0;
        state.last_persist = tokio::time::Instant::now();
        // Fire-and-forget: hold no lock during IO
        let info = state.info.clone();
        drop(guard);
        tokio::spawn(async move {
            try_flush_compression_total(&info).await;
        });
    }
}

/// Persist the given `CompressionTotalInfo` snapshot to backend storage.
/// Used by both the debounce path and the manual flush path.
async fn try_flush_compression_total(info: &CompressionTotalInfo) {
    let Some(store) = runtime_sources::object_store_handle() else {
        error!("object store not initialized, skipping compression total persist");
        return;
    };
    match serde_json::to_vec(info) {
        Ok(data) => {
            if let Err(e) = crate::config::com::save_config(store.clone(), &DATA_COMPRESSION_TOTAL_NAME_PATH, data).await {
                error!("Failed to persist compression total to backend: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to serialize compression total info: {}", e);
        }
    }
}

/// Initialize the compression total memory cache from backend storage.
/// Should be called at startup after the store is available if compression is enabled.
/// Compression totals are only recorded while this cache is initialized, so this must
/// be called before any compression operations are recorded.
#[instrument(skip(store))]
pub async fn init_compression_total_memory_from_backend(store: Arc<ECStore>) {
    let info = match load_compression_total_from_backend(store).await {
        Ok(info) => info,
        Err(e) => {
            // If load fails (e.g. ConfigNotFound or corrupt backup), start from zero.
            info!("Failed to init compression total from backend, starting from zero: {}", e);
            CompressionTotalInfo::default()
        }
    };
    *COMPRESSION_TOTAL_MEMORY_CACHE.write().await = Some(CompressionTotalState {
        info,
        ops_since_persist: 0,
        last_persist: tokio::time::Instant::now(),
        inited: true,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_data_usage::BucketUsageInfo;
    use serial_test::serial;

    async fn clear_usage_memory_cache_for_test() {
        memory_cache().write().await.clear();
        *cache_updating().write().await = false;
    }

    fn data_usage_info_for_test(bucket: &str, objects_count: u64, size: u64, last_update: SystemTime) -> DataUsageInfo {
        let mut info = DataUsageInfo {
            last_update: Some(last_update),
            ..Default::default()
        };
        info.buckets_usage.insert(
            bucket.to_string(),
            BucketUsageInfo {
                objects_count,
                versions_count: objects_count,
                size,
                ..Default::default()
            },
        );
        info.bucket_sizes.insert(bucket.to_string(), size);
        info.buckets_count = info.buckets_usage.len() as u64;
        info.calculate_totals();
        info
    }

    #[tokio::test]
    async fn compute_usage_preserves_same_object_across_1000_entry_page_boundary() {
        let first_page = UsageVersionPage {
            is_truncated: true,
            next_marker: Some("object-a".to_string()),
            next_version_idmarker: Some(uuid::Uuid::from_u128(1000).to_string()),
            objects: (1..=1000_u128)
                .map(|version| ObjectInfo {
                    name: "object-a".to_string(),
                    size: 1,
                    version_id: Some(uuid::Uuid::from_u128(version)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let second_page = UsageVersionPage {
            objects: vec![
                ObjectInfo {
                    name: "object-a".to_string(),
                    size: 1,
                    version_id: Some(uuid::Uuid::from_u128(1001)),
                    ..Default::default()
                },
                ObjectInfo {
                    name: "object-a".to_string(),
                    version_id: Some(uuid::Uuid::from_u128(1002)),
                    delete_marker: true,
                    ..Default::default()
                },
                ObjectInfo {
                    name: "object-b".to_string(),
                    size: 2,
                    version_id: Some(uuid::Uuid::from_u128(1003)),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pages = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from([first_page, second_page])));
        let fetch_pages = Arc::clone(&pages);

        let usage = compute_bucket_usage_with_pages("bucket-a", move |marker, version_marker| {
            let page = fetch_pages
                .lock()
                .expect("page queue lock should not be poisoned")
                .pop_front()
                .expect("pagination must not request an unexpected page");
            if page.is_truncated {
                assert_eq!((marker, version_marker), (None, None));
            } else {
                let expected_version_marker = uuid::Uuid::from_u128(1000).to_string();
                assert_eq!(marker.as_deref(), Some("object-a"));
                assert_eq!(version_marker.as_deref(), Some(expected_version_marker.as_str()));
            }
            async move { Ok(page) }
        })
        .await
        .expect("two-page usage aggregation should succeed");

        assert!(pages.lock().expect("page queue lock should not be poisoned").is_empty());
        assert_eq!(usage.objects_count, 2);
        assert_eq!(usage.versions_count, 1002);
        assert_eq!(usage.delete_markers_count, 1);
        assert_eq!(usage.size, 1003);
        assert_eq!(usage.object_versions_histogram.get("SINGLE_VERSION"), Some(&1));
        assert_eq!(usage.object_versions_histogram.get("BETWEEN_1000_AND_10000"), Some(&1));
    }

    #[tokio::test]
    #[serial]
    async fn live_bucket_usage_refreshes_are_coalesced_and_cached_until_invalidated() {
        const BUCKET: &str = "coalesced-live-usage-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(coalesce_live_bucket_usage(BUCKET.to_string(), move || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Ok(BucketUsageInfo {
                        objects_count: 7,
                        size: 42,
                        ..Default::default()
                    })
                }
            })));
        }

        for task in tasks {
            let usage = task
                .await
                .expect("coalesced refresh task should not panic")
                .expect("coalesced refresh should succeed");
            assert_eq!((usage.objects_count, usage.size), (7, 42));
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let calls_within_ttl = Arc::clone(&calls);
        let cached = coalesce_live_bucket_usage(BUCKET.to_string(), move || {
            let calls_within_ttl = Arc::clone(&calls_within_ttl);
            async move {
                calls_within_ttl.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(BucketUsageInfo::default())
            }
        })
        .await
        .expect("a later refresh within TTL should reuse the cached live result");
        assert_eq!((cached.objects_count, cached.size), (7, 42));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        invalidate_live_bucket_usage_cache(BUCKET).await;
        let calls_after_invalidate = Arc::clone(&calls);
        coalesce_live_bucket_usage(BUCKET.to_string(), move || {
            let calls_after_invalidate = Arc::clone(&calls_after_invalidate);
            async move {
                calls_after_invalidate.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(BucketUsageInfo {
                    objects_count: 1,
                    size: 2,
                    ..Default::default()
                })
            }
        })
        .await
        .expect("a refresh after invalidate should recompute");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn apply_cached_or_schedule_returns_immediately_on_cache_miss() {
        const BUCKET: &str = "live-schedule-miss-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;

        let mut response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, || async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(BucketUsageInfo {
                objects_count: 9,
                size: 99,
                ..Default::default()
            })
        })
        .await;

        assert!(
            !response.buckets_usage.contains_key(BUCKET),
            "cache miss must not block the response on the live recount"
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if cached_live_bucket_usage_if_current(BUCKET).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background live refresh should populate the TTL cache");

        let mut next_response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut next_response, BUCKET, || async move {
            panic!("cache hit must not schedule another live recount");
        })
        .await;
        assert_eq!(
            next_response
                .buckets_usage
                .get(BUCKET)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((9, 99))
        );
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn write_memory_invalidates_live_bucket_usage_cache() {
        const BUCKET: &str = "live-invalidate-on-write-test";
        clear_usage_memory_cache_for_test().await;
        invalidate_live_bucket_usage_cache(BUCKET).await;

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 3,
                size: 30,
                ..Default::default()
            })
        })
        .await
        .expect("seed live cache");
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_some());

        let persisted = data_usage_info_for_test(BUCKET, 3, 30, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_memory(BUCKET, None, 5).await;

        assert!(
            cached_live_bucket_usage_if_current(BUCKET).await.is_none(),
            "write-path memory updates must drop sticky live usage so dirty overlay can win"
        );
        invalidate_live_bucket_usage_cache(BUCKET).await;
        clear_usage_memory_cache_for_test().await;
    }

    #[tokio::test]
    #[serial]
    async fn invalidate_during_in_flight_refresh_does_not_poison_ttl_cache() {
        const BUCKET: &str = "live-invalidate-inflight-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;

        let release = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, {
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            let finished = Arc::clone(&finished);
            let calls = Arc::clone(&calls);
            move || {
                let release = Arc::clone(&release);
                let entered = Arc::clone(&entered);
                let finished = Arc::clone(&finished);
                let calls = Arc::clone(&calls);
                async move {
                    let attempt = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt == 0 {
                        entered.store(true, std::sync::atomic::Ordering::SeqCst);
                        release.notified().await;
                        finished.notify_waiters();
                        Ok(BucketUsageInfo {
                            objects_count: 1,
                            size: 10,
                            ..Default::default()
                        })
                    } else {
                        // Stop the post-supersede retry so this test only asserts the
                        // superseded snapshot is not published.
                        Err(Error::other("stop retry after superseded refresh"))
                    }
                }
            }
        })
        .await;

        tokio::time::timeout(Duration::from_secs(2), async {
            while !entered.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("background refresh must enter init before invalidate");

        invalidate_live_bucket_usage_cache(BUCKET).await;
        release.notify_one();

        tokio::time::timeout(Duration::from_secs(2), finished.notified())
            .await
            .expect("superseded refresh must finish init");
        // Init finished; coalesce must refuse to publish. Poll so we do not rely on a fixed sleep.
        for _ in 0..30 {
            assert!(
                cached_live_bucket_usage_if_current(BUCKET).await.is_none(),
                "a refresh that finished after invalidate must not become a usable TTL hit"
            );
            assert!(
                live_bucket_usage_cache().get(BUCKET).await.is_none(),
                "superseded init must not leave a stale epoch entry in the TTL cache"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mut next_response = data_usage_info_for_test(BUCKET, 5, 50, SystemTime::now());
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut next_response, BUCKET, || async move {
            Err(Error::other("background refresh must not be required for this assertion"))
        })
        .await;
        assert_eq!(
            next_response
                .buckets_usage
                .get(BUCKET)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((5, 50)),
            "admin response must keep scanner/overlay counts when live cache is superseded"
        );
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn coalesce_retries_after_invalidate_and_publishes_fresh_epoch() {
        const BUCKET: &str = "live-coalesce-superseded-retry-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;

        let release = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task = tokio::spawn(coalesce_live_bucket_usage(BUCKET.to_string(), {
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            let calls = Arc::clone(&calls);
            move || {
                let release = Arc::clone(&release);
                let entered = Arc::clone(&entered);
                let calls = Arc::clone(&calls);
                async move {
                    let attempt = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt == 0 {
                        entered.store(true, std::sync::atomic::Ordering::SeqCst);
                        release.notified().await;
                        Ok(BucketUsageInfo {
                            objects_count: 1,
                            size: 10,
                            ..Default::default()
                        })
                    } else {
                        Ok(BucketUsageInfo {
                            objects_count: 2,
                            size: 20,
                            ..Default::default()
                        })
                    }
                }
            }
        }));

        tokio::time::timeout(Duration::from_secs(2), async {
            while !entered.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("coalesce init must start (epoch already captured) before invalidate");

        invalidate_live_bucket_usage_cache(BUCKET).await;
        release.notify_one();

        let usage = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("coalesce must finish after release")
            .expect("coalesce task should not panic")
            .expect("coalesce must retry after supersede and publish the fresh epoch");
        assert_eq!((usage.objects_count, usage.size), (2, 20));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            cached_live_bucket_usage_if_current(BUCKET)
                .await
                .map(|u| (u.objects_count, u.size)),
            Some((2, 20))
        );
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn coalesce_after_forget_without_ensure_does_not_resurrect_epoch() {
        const BUCKET: &str = "live-coalesce-forget-no-ensure-test";
        forget_live_bucket_usage(BUCKET).await;

        let init_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Calls coalesce directly (no ensure beforehand), covering the attempt-0 path where
        // a `None if attempt == 0 => ensure(...)` regression would resurrect epoch tracking
        // for a bucket that forget() just fenced.
        let result = coalesce_live_bucket_usage(BUCKET.to_string(), {
            let init_calls = Arc::clone(&init_calls);
            move || {
                let init_calls = Arc::clone(&init_calls);
                async move {
                    init_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(BucketUsageInfo {
                        objects_count: 1,
                        size: 10,
                        ..Default::default()
                    })
                }
            }
        })
        .await;

        assert!(
            result.is_err(),
            "coalesce must fail for a forgotten bucket when nobody ensured its epoch first"
        );
        assert_eq!(
            init_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "init must never run for an untracked/forgotten bucket"
        );
        assert!(
            live_bucket_usage_epoch_if_tracked(BUCKET).await.is_none(),
            "epoch must remain untracked; resurrecting it here would leak an unbounded map entry"
        );
        assert!(
            cached_live_bucket_usage_if_current(BUCKET).await.is_none(),
            "no usable TTL entry must exist for a forgotten bucket"
        );
    }

    #[tokio::test]
    #[serial]
    async fn discard_superseded_live_cache_entry_does_not_wipe_newer_epoch() {
        const BUCKET: &str = "live-discard-cas-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 2,
                size: 20,
                ..Default::default()
            })
        })
        .await
        .expect("seed live cache at current epoch");
        let winner_epoch = live_bucket_usage_epoch(BUCKET).await;
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_some());

        // Simulate a superseded loser's trailing cleanup for an older epoch.
        discard_superseded_live_cache_entry(BUCKET, winner_epoch.wrapping_sub(1)).await;

        let still_cached = cached_live_bucket_usage_if_current(BUCKET)
            .await
            .expect("newer winner must survive discard of an older superseded epoch");
        assert_eq!((still_cached.objects_count, still_cached.size), (2, 20));

        discard_superseded_live_cache_entry(BUCKET, winner_epoch).await;
        assert!(
            cached_live_bucket_usage_if_current(BUCKET).await.is_none(),
            "discard matching the cached epoch must drop the entry"
        );
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn dirty_memory_overlay_wins_over_cached_live_hit() {
        const BUCKET: &str = "live-dirty-overlay-wins-test";
        clear_usage_memory_cache_for_test().await;
        invalidate_live_bucket_usage_cache(BUCKET).await;

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 1,
                size: 10,
                ..Default::default()
            })
        })
        .await
        .expect("seed live TTL cache");

        // Seed scanner baseline then a dirty write-path increment without clearing live
        // via a second invalidate race: write invalidates live, so re-seed live after
        // recording dirty memory by inserting through coalesce at the new epoch.
        let persisted = data_usage_info_for_test(BUCKET, 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_memory(BUCKET, None, 5).await;
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_none());

        // Re-publish a stale-shaped live snapshot at the post-write epoch (simulates a
        // TTL hit that is older than dirty memory counts).
        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 1,
                size: 10,
                ..Default::default()
            })
        })
        .await
        .expect("re-seed live cache after write invalidate");

        // Mirror AccountInfo: response last_update is the scanner snapshot time, not "now".
        // Overlay compares dirty usage_updated_at against that timestamp.
        let mut response = persisted.clone();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, || async {
            panic!("must use cached live hit");
        })
        .await;
        assert_eq!(
            response
                .buckets_usage
                .get(BUCKET)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1, 10)),
            "live hit applied before overlay"
        );

        // Same ordering as AccountInfo / DataUsageInfo admin paths.
        apply_bucket_usage_memory_overlay(&mut response).await;
        assert_eq!(
            response
                .buckets_usage
                .get(BUCKET)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((2, 15)),
            "dirty write-path overlay must win over TTL live snapshot"
        );

        invalidate_live_bucket_usage_cache(BUCKET).await;
        clear_usage_memory_cache_for_test().await;
    }

    #[tokio::test]
    #[serial]
    async fn background_live_usage_refreshes_are_serialized_by_semaphore() {
        const BUCKET_A: &str = "live-sem-a-test";
        const BUCKET_B: &str = "live-sem-b-test";
        invalidate_live_bucket_usage_cache(BUCKET_A).await;
        invalidate_live_bucket_usage_cache(BUCKET_B).await;

        let release = Arc::new(tokio::sync::Notify::new());
        let concurrent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_concurrent = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let spawn_blocked = |bucket: &'static str| {
            let release = Arc::clone(&release);
            let concurrent = Arc::clone(&concurrent);
            let max_concurrent = Arc::clone(&max_concurrent);
            let mut response = DataUsageInfo::default();
            async move {
                apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, bucket, move || {
                    let release = Arc::clone(&release);
                    let concurrent = Arc::clone(&concurrent);
                    let max_concurrent = Arc::clone(&max_concurrent);
                    async move {
                        let now = concurrent.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        max_concurrent.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                        release.notified().await;
                        concurrent.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(BucketUsageInfo {
                            objects_count: 1,
                            size: 1,
                            ..Default::default()
                        })
                    }
                })
                .await;
            }
        };

        spawn_blocked(BUCKET_A).await;
        spawn_blocked(BUCKET_B).await;

        // Wait until the first refresh is inside the critical section.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if max_concurrent.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("first refresh should acquire the permit");

        // While the first holder is blocked, concurrency must stay at 1.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            max_concurrent.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "LIVE_USAGE_REFRESH_PERMITS=1 must serialize background listings"
        );

        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // First waiter may consume the first notify; keep waking until both finish.
                release.notify_waiters();
                if cached_live_bucket_usage_if_current(BUCKET_A).await.is_some()
                    && cached_live_bucket_usage_if_current(BUCKET_B).await.is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("both serialized refreshes should complete");

        assert_eq!(max_concurrent.load(std::sync::atomic::Ordering::SeqCst), 1);
        invalidate_live_bucket_usage_cache(BUCKET_A).await;
        invalidate_live_bucket_usage_cache(BUCKET_B).await;
    }

    #[tokio::test]
    #[serial]
    async fn live_usage_refresh_timeout_releases_semaphore_permit() {
        let err = run_live_usage_refresh_under_permit_with_timeout(Duration::from_millis(30), async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(BucketUsageInfo::default())
        })
        .await
        .expect_err("refresh must time out");
        assert!(
            err.to_string().contains("timed out"),
            "timeout error should be distinguishable, got: {err}"
        );

        let _permit = tokio::time::timeout(Duration::from_millis(200), LIVE_USAGE_REFRESH_SEMAPHORE.acquire())
            .await
            .expect("timed-out refresh must release the semaphore permit")
            .expect("semaphore should remain open");
    }

    #[test]
    fn live_usage_ttl_outlives_the_refresh_that_produces_it() {
        // A live entry must not expire faster than the walk allowed to produce it,
        // otherwise every admin poll on a large bucket misses and re-lists forever
        // (rustfs/rustfs#4902). Guards against re-coupling the live TTL to the much
        // shorter quota-cache TTL, or raising the refresh bound past the TTL.
        assert!(
            Duration::from_secs(LIVE_BUCKET_USAGE_TTL_SECS) >= LIVE_USAGE_REFRESH_TIMEOUT,
            "live TTL ({LIVE_BUCKET_USAGE_TTL_SECS}s) must be >= the refresh bound ({LIVE_USAGE_REFRESH_TIMEOUT:?})"
        );
    }

    #[tokio::test]
    #[serial]
    async fn delete_memory_and_backend_remove_invalidate_live_cache() {
        const BUCKET: &str = "live-invalidate-on-delete-test";
        clear_usage_memory_cache_for_test().await;
        invalidate_live_bucket_usage_cache(BUCKET).await;

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 4,
                size: 40,
                ..Default::default()
            })
        })
        .await
        .expect("seed live cache");
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_some());

        let persisted = data_usage_info_for_test(BUCKET, 4, 40, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory(BUCKET, 40, true).await;

        assert!(
            cached_live_bucket_usage_if_current(BUCKET).await.is_none(),
            "delete-path memory updates must drop sticky live usage"
        );

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 1,
                size: 1,
                ..Default::default()
            })
        })
        .await
        .expect("re-seed after delete memory invalidate");
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_some());

        // remove_bucket_usage_from_backend also forgets live state before clearing memory.
        forget_live_bucket_usage(BUCKET).await;
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_none());
        assert!(
            live_bucket_usage_epoch_if_tracked(BUCKET).await.is_none(),
            "deleted buckets must drop epoch tracking to bound map growth"
        );

        clear_usage_memory_cache_for_test().await;
    }

    #[tokio::test]
    #[serial]
    async fn apply_cached_live_restores_prior_counts_when_invalidate_races() {
        const BUCKET: &str = "live-apply-delete-race-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;
        *LIVE_APPLY_HIT_TEST_HOOK.lock().expect("hook lock") = None;

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 9,
                size: 90,
                ..Default::default()
            })
        })
        .await
        .expect("seed live cache");

        let hook = Arc::new(LiveApplyHitTestHook {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *LIVE_APPLY_HIT_TEST_HOOK.lock().expect("hook lock") = Some(Arc::clone(&hook));

        let mut response = data_usage_info_for_test(BUCKET, 5, 50, SystemTime::now());
        let apply = tokio::spawn({
            let mut response = response.clone();
            async move {
                apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, || async {
                    Err(Error::other("scheduled refresh must not be required for restore assertion"))
                })
                .await;
                response
            }
        });

        tokio::time::timeout(Duration::from_secs(2), hook.entered.notified())
            .await
            .expect("apply path must pause after a live cache hit");
        invalidate_live_bucket_usage_cache(BUCKET).await;
        hook.release.notify_one();

        response = tokio::time::timeout(Duration::from_secs(2), apply)
            .await
            .expect("apply must finish after release")
            .expect("apply task should not panic");
        assert_eq!(
            response
                .buckets_usage
                .get(BUCKET)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((5, 50)),
            "invalidate during apply must restore scanner/overlay counts via the real apply helper"
        );

        *LIVE_APPLY_HIT_TEST_HOOK.lock().expect("hook lock") = None;
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn forget_during_inflight_refresh_does_not_resurrect_epoch() {
        const BUCKET: &str = "live-forget-no-epoch-resurrect-test";
        forget_live_bucket_usage(BUCKET).await;

        let release = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, {
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            move || {
                let release = Arc::clone(&release);
                let entered = Arc::clone(&entered);
                async move {
                    entered.store(true, std::sync::atomic::Ordering::SeqCst);
                    release.notified().await;
                    Ok(BucketUsageInfo {
                        objects_count: 1,
                        size: 10,
                        ..Default::default()
                    })
                }
            }
        })
        .await;

        tokio::time::timeout(Duration::from_secs(2), async {
            while !entered.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("background refresh must enter init before forget");

        forget_live_bucket_usage(BUCKET).await;
        release.notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // Schedule task drops inflight in its finally block after coalesce returns.
                // Poll until that happens and confirm forget did not leave a resurrected epoch.
                if live_bucket_usage_cache().get(BUCKET).await.is_none()
                    && live_bucket_usage_epoch_if_tracked(BUCKET).await.is_none()
                    && !live_bucket_usage_in_flight().read().await.contains_key(BUCKET)
                {
                    // Give the schedule task a moment after forget cleared inflight early.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    if live_bucket_usage_epoch_if_tracked(BUCKET).await.is_none()
                        && live_bucket_usage_cache().get(BUCKET).await.is_none()
                    {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("forget must leave no epoch tracking after the fenced refresh exits");
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_none());
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn apply_forget_during_hit_restore_does_not_schedule_or_resurrect_epoch() {
        const BUCKET: &str = "live-apply-forget-no-resurrect-test";
        forget_live_bucket_usage(BUCKET).await;
        *LIVE_APPLY_HIT_TEST_HOOK.lock().expect("hook lock") = None;

        ensure_live_bucket_usage_epoch(BUCKET).await;
        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 9,
                size: 90,
                ..Default::default()
            })
        })
        .await
        .expect("seed live cache");

        let hook = Arc::new(LiveApplyHitTestHook {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *LIVE_APPLY_HIT_TEST_HOOK.lock().expect("hook lock") = Some(Arc::clone(&hook));

        let init_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut response = data_usage_info_for_test(BUCKET, 5, 50, SystemTime::now());
        let apply = tokio::spawn({
            let init_calls = Arc::clone(&init_calls);
            let mut response = response.clone();
            async move {
                apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, move || {
                    let init_calls = Arc::clone(&init_calls);
                    async move {
                        init_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(BucketUsageInfo {
                            objects_count: 1,
                            size: 1,
                            ..Default::default()
                        })
                    }
                })
                .await;
                response
            }
        });

        tokio::time::timeout(Duration::from_secs(2), hook.entered.notified())
            .await
            .expect("apply path must pause after a live cache hit");
        forget_live_bucket_usage(BUCKET).await;
        hook.release.notify_one();

        response = tokio::time::timeout(Duration::from_secs(2), apply)
            .await
            .expect("apply must finish after release")
            .expect("apply task should not panic");
        assert_eq!(
            response
                .buckets_usage
                .get(BUCKET)
                .map(|usage| (usage.objects_count, usage.size)),
            Some((5, 50)),
            "forget during apply must restore scanner/overlay counts"
        );
        assert_eq!(
            init_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "forget after apply hit must not schedule a refresh that runs init"
        );
        assert!(live_bucket_usage_epoch_if_tracked(BUCKET).await.is_none());
        assert!(cached_live_bucket_usage_if_current(BUCKET).await.is_none());

        *LIVE_APPLY_HIT_TEST_HOOK.lock().expect("hook lock") = None;
    }

    #[tokio::test]
    #[serial]
    async fn forget_between_still_owned_and_ensure_does_not_resurrect_epoch() {
        const BUCKET: &str = "live-forget-pre-ensure-test";
        forget_live_bucket_usage(BUCKET).await;
        *LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK.lock().expect("hook lock") = None;

        let hook = Arc::new(LiveSchedulePreEnsureTestHook {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK.lock().expect("hook lock") = Some(Arc::clone(&hook));

        let init_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, {
            let init_calls = Arc::clone(&init_calls);
            move || {
                let init_calls = Arc::clone(&init_calls);
                async move {
                    init_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(BucketUsageInfo {
                        objects_count: 1,
                        size: 10,
                        ..Default::default()
                    })
                }
            }
        })
        .await;

        // Pauses after the first still_owned check has already passed, so forget()
        // below fences generation/in-flight *before* ensure resurrects epoch 0,
        // reproducing the still_owned -> ensure -> still_owned TOCTOU (Bug 1).
        tokio::time::timeout(Duration::from_secs(2), hook.entered.notified())
            .await
            .expect("schedule must pause after still_owned check and before ensure");
        forget_live_bucket_usage(BUCKET).await;
        hook.release.notify_one();

        // The fenced task's remaining work (ensure + second still_owned check + return) is
        // pure in-memory bookkeeping with no further await-yielding I/O, so a bounded sleep
        // deterministically lets it finish. A `contains_key`/`is_none` polling loop would be
        // wrong here: the state right after `forget` above already satisfies "untracked", so
        // it could pass trivially before the paused task ever resumes and resurrects it.
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(
            live_bucket_usage_epoch_if_tracked(BUCKET).await.is_none(),
            "forget between still_owned and ensure must leave the epoch untracked"
        );
        assert!(
            live_bucket_usage_cache().get(BUCKET).await.is_none(),
            "forget between still_owned and ensure must not leave a TTL publish"
        );
        assert!(
            !live_bucket_usage_in_flight().read().await.contains_key(BUCKET),
            "fenced schedule must not leave a stale in-flight entry"
        );
        assert!(
            cached_live_bucket_usage_if_current(BUCKET).await.is_none(),
            "no usable TTL entry must survive the fenced ensure resurrection"
        );
        assert_eq!(
            init_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "init must not run (or publish) once the post-ensure still_owned check fails"
        );

        *LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK.lock().expect("hook lock") = None;
    }

    #[tokio::test]
    #[serial]
    async fn in_flight_owner_remove_does_not_clear_newer_schedule() {
        const BUCKET: &str = "live-inflight-owner-token-test";
        forget_live_bucket_usage(BUCKET).await;
        *LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK.lock().expect("hook lock") = None;

        let first_hook = Arc::new(LiveSchedulePreEnsureTestHook {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK.lock().expect("hook lock") = Some(Arc::clone(&first_hook));

        let init_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let concurrent_inits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_concurrent_inits = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, {
            let init_calls = Arc::clone(&init_calls);
            let concurrent_inits = Arc::clone(&concurrent_inits);
            let max_concurrent_inits = Arc::clone(&max_concurrent_inits);
            move || {
                let init_calls = Arc::clone(&init_calls);
                let concurrent_inits = Arc::clone(&concurrent_inits);
                let max_concurrent_inits = Arc::clone(&max_concurrent_inits);
                async move {
                    let now = concurrent_inits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    max_concurrent_inits.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    init_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    concurrent_inits.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(BucketUsageInfo {
                        objects_count: 1,
                        size: 1,
                        ..Default::default()
                    })
                }
            }
        })
        .await;

        tokio::time::timeout(Duration::from_secs(2), first_hook.entered.notified())
            .await
            .expect("first schedule must reach pre-ensure hook");

        forget_live_bucket_usage(BUCKET).await;
        *LIVE_SCHEDULE_PRE_ENSURE_TEST_HOOK.lock().expect("hook lock") = None;

        let mut response = DataUsageInfo::default();
        apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, {
            let init_calls = Arc::clone(&init_calls);
            let concurrent_inits = Arc::clone(&concurrent_inits);
            let max_concurrent_inits = Arc::clone(&max_concurrent_inits);
            move || {
                let init_calls = Arc::clone(&init_calls);
                let concurrent_inits = Arc::clone(&concurrent_inits);
                let max_concurrent_inits = Arc::clone(&max_concurrent_inits);
                async move {
                    let now = concurrent_inits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    max_concurrent_inits.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    init_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    concurrent_inits.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(BucketUsageInfo {
                        objects_count: 1,
                        size: 1,
                        ..Default::default()
                    })
                }
            }
        })
        .await;

        first_hook.release.notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if init_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1
                    && live_bucket_usage_in_flight().read().await.contains_key(BUCKET)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("second schedule must own in-flight after forget fenced the first");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if init_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1
                    && !live_bucket_usage_in_flight().read().await.contains_key(BUCKET)
                    && cached_live_bucket_usage_if_current(BUCKET).await.is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("fenced first schedule must finish without clearing the second owner");

        assert_eq!(
            max_concurrent_inits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "old schedule finishing must not allow a third concurrent coalesce/init"
        );
        assert_eq!(init_calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        forget_live_bucket_usage(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn discard_superseded_live_cache_entry_race_keeps_newer_winner() {
        const BUCKET: &str = "live-discard-toctou-race-test";
        invalidate_live_bucket_usage_cache(BUCKET).await;
        *LIVE_DISCARD_TEST_HOOK.lock().expect("hook lock") = None;

        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 2,
                size: 20,
                ..Default::default()
            })
        })
        .await
        .expect("seed live cache");
        let stale_epoch = live_bucket_usage_epoch(BUCKET).await;

        let hook = Arc::new(LiveDiscardTestHook {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *LIVE_DISCARD_TEST_HOOK.lock().expect("hook lock") = Some(Arc::clone(&hook));

        // Pass the seeded (loser) entry's own epoch as the superseded epoch, matching how
        // callers invoke discard: the removed cached entry's epoch equals the superseded
        // epoch, so remove+conditional-reinsert must return without touching the newer
        // winner published while discard was paused.
        let discard = tokio::spawn(discard_superseded_live_cache_entry(BUCKET, stale_epoch));

        tokio::time::timeout(Duration::from_secs(2), hook.entered.notified())
            .await
            .expect("discard must pause after remove");

        invalidate_live_bucket_usage_cache(BUCKET).await;
        coalesce_live_bucket_usage(BUCKET.to_string(), || async {
            Ok(BucketUsageInfo {
                objects_count: 3,
                size: 30,
                ..Default::default()
            })
        })
        .await
        .expect("publish newer winner while discard is paused");

        hook.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), discard)
            .await
            .expect("discard must finish")
            .expect("discard task should not panic");

        let winner = cached_live_bucket_usage_if_current(BUCKET)
            .await
            .expect("newer winner must survive discard TOCTOU race");
        assert_eq!((winner.objects_count, winner.size), (3, 30));

        *LIVE_DISCARD_TEST_HOOK.lock().expect("hook lock") = None;
        invalidate_live_bucket_usage_cache(BUCKET).await;
    }

    #[tokio::test]
    #[serial]
    async fn concurrent_apply_cache_miss_schedules_single_in_flight_refresh() {
        const BUCKET: &str = "live-schedule-inflight-dedupe-test";
        forget_live_bucket_usage(BUCKET).await;

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let init_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let make_init = {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let init_calls = Arc::clone(&init_calls);
                move || {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    let init_calls = Arc::clone(&init_calls);
                    async move {
                        init_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        entered.notify_waiters();
                        release.notified().await;
                        Ok(BucketUsageInfo {
                            objects_count: 4,
                            size: 40,
                            ..Default::default()
                        })
                    }
                }
            };
            tasks.push(tokio::spawn(async move {
                let mut response = DataUsageInfo::default();
                apply_cached_live_bucket_usage_or_schedule_refresh(&mut response, BUCKET, make_init).await;
            }));
        }

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("one scheduled refresh must enter init");
        assert_eq!(
            init_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "in-flight dedupe must allow only one background init for concurrent cache misses"
        );
        assert!(live_bucket_usage_in_flight().read().await.contains_key(BUCKET));

        release.notify_waiters();
        for task in tasks {
            task.await.expect("concurrent apply task should not panic");
        }

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if cached_live_bucket_usage_if_current(BUCKET).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("single refresh should populate the TTL cache");
        assert_eq!(init_calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        forget_live_bucket_usage(BUCKET).await;
    }

    #[test]
    fn apply_live_bucket_usage_to_multiple_buckets_then_calculate_totals_once() {
        let mut response = DataUsageInfo::default();
        apply_live_bucket_usage_to_response(
            &mut response,
            "bucket-a",
            &BucketUsageInfo {
                objects_count: 2,
                versions_count: 2,
                size: 20,
                ..Default::default()
            },
        );
        apply_live_bucket_usage_to_response(
            &mut response,
            "bucket-b",
            &BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 30,
                ..Default::default()
            },
        );
        // Per-bucket apply must not recompute aggregates (avoids O(n^2) on AccountInfo).
        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);

        response.calculate_totals();

        assert_eq!(response.buckets_count, 2);
        assert_eq!(response.objects_total_count, 5);
        assert_eq!(response.versions_total_count, 5);
        assert_eq!(response.objects_total_size, 50);
    }

    #[tokio::test]
    #[serial]
    async fn live_usage_updates_response_without_replacing_quota_memory() {
        clear_usage_memory_cache_for_test().await;
        let persisted = data_usage_info_for_test("bucket-a", 100, 1_000, SystemTime::now());
        replace_bucket_usage_memory_from_info(&persisted).await;
        let mut response = persisted.clone();

        apply_live_bucket_usage_to_response(&mut response, "bucket-a", &BucketUsageInfo::default());

        assert_eq!(response.buckets_usage.get("bucket-a").map(|usage| usage.objects_count), Some(0));
        assert_eq!(get_bucket_usage_memory("bucket-a").await, Some(1_000));
    }

    #[test]
    fn version_listing_cursor_rejects_incomplete_or_non_advancing_pages() {
        let mut marker = Some("object-a".to_string());
        let mut version_marker = Some("version-a".to_string());

        assert!(
            advance_version_listing_cursor("bucket-a", &mut marker, &mut version_marker, None, Some("version-b".to_string()),)
                .is_err()
        );
        assert!(
            advance_version_listing_cursor("bucket-a", &mut marker, &mut version_marker, Some("object-a".to_string()), None,)
                .is_err()
        );
        assert!(
            advance_version_listing_cursor(
                "bucket-a",
                &mut marker,
                &mut version_marker,
                Some("object-a".to_string()),
                Some("version-a".to_string()),
            )
            .is_err()
        );
        assert!(ensure_truncated_version_page_has_entries("bucket-a", 0).is_err());
        ensure_truncated_version_page_has_entries("bucket-a", 1).expect("a truncated page with an entry can advance");

        let mut tagged_marker = Some("object-a[rustfs_cache:v2,id:old]".to_string());
        let mut tagged_version_marker = Some("version-a".to_string());
        assert!(
            advance_version_listing_cursor(
                "bucket-a",
                &mut tagged_marker,
                &mut tagged_version_marker,
                Some("object-a[rustfs_cache:v2,id:new]".to_string()),
                Some("version-a".to_string()),
            )
            .is_err(),
            "different cache tags for the same logical marker must not count as progress"
        );
    }

    #[test]
    fn version_listing_rejects_replayed_and_out_of_order_entries() {
        let version_a = uuid::Uuid::from_u128(1);
        let version_b = uuid::Uuid::from_u128(2);
        let mut current_object = None;
        let mut current_versions = HashSet::new();

        record_version_listing_entry(
            "bucket-a",
            &mut current_object,
            &mut current_versions,
            "object-b",
            Some(version_a.as_bytes()),
        )
        .expect("first version should be accepted");
        assert!(
            record_version_listing_entry(
                "bucket-a",
                &mut current_object,
                &mut current_versions,
                "object-b",
                Some(version_a.as_bytes()),
            )
            .is_err()
        );
        record_version_listing_entry(
            "bucket-a",
            &mut current_object,
            &mut current_versions,
            "object-c",
            Some(version_b.as_bytes()),
        )
        .expect("a lexicographically later object should reset version history");
        assert!(
            record_version_listing_entry(
                "bucket-a",
                &mut current_object,
                &mut current_versions,
                "object-a",
                Some(version_a.as_bytes()),
            )
            .is_err()
        );
    }

    fn aggregate_for_test(
        inputs: Vec<(DiskUsageStatus, Result<Option<LocalUsageSnapshot>, Error>)>,
    ) -> (Vec<DiskUsageStatus>, DataUsageInfo) {
        let mut aggregated = DataUsageInfo::default();
        let mut latest_update: Option<SystemTime> = None;
        let mut statuses = Vec::new();

        for (mut status, snapshot_result) in inputs {
            if let Ok(Some(snapshot)) = snapshot_result {
                status.snapshot_exists = true;
                status.last_update = snapshot.last_update;
                merge_snapshot(&mut aggregated, snapshot, &mut latest_update);
            }
            statuses.push(status);
        }

        aggregated.buckets_count = aggregated.buckets_usage.len() as u64;
        aggregated.last_update = latest_update;
        aggregated.disk_usage_status = statuses.clone();

        (statuses, aggregated)
    }

    #[test]
    fn aggregate_skips_corrupted_snapshot_and_preserves_other_disks() {
        let mut good_snapshot = LocalUsageSnapshot::new(local_snapshot::LocalUsageSnapshotMeta {
            disk_id: "good-disk".to_string(),
            pool_index: Some(0),
            set_index: Some(0),
            disk_index: Some(0),
        });
        good_snapshot.last_update = Some(SystemTime::now());
        good_snapshot.buckets_usage.insert(
            "bucket-a".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 42,
                ..Default::default()
            },
        );
        good_snapshot.recompute_totals();

        let bad_snapshot_err: Result<Option<LocalUsageSnapshot>, Error> = Err(Error::other("corrupted snapshot payload"));

        let inputs = vec![
            (
                DiskUsageStatus {
                    disk_id: "bad-disk".to_string(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    disk_index: Some(1),
                    last_update: None,
                    snapshot_exists: false,
                },
                bad_snapshot_err,
            ),
            (
                DiskUsageStatus {
                    disk_id: "good-disk".to_string(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    disk_index: Some(0),
                    last_update: None,
                    snapshot_exists: false,
                },
                Ok(Some(good_snapshot)),
            ),
        ];

        let (statuses, aggregated) = aggregate_for_test(inputs);

        // Bad disk stays non-existent, good disk is marked present
        let bad_status = statuses.iter().find(|s| s.disk_id == "bad-disk").unwrap();
        assert!(!bad_status.snapshot_exists);
        let good_status = statuses.iter().find(|s| s.disk_id == "good-disk").unwrap();
        assert!(good_status.snapshot_exists);

        // Aggregated data is from good snapshot only
        assert_eq!(aggregated.objects_total_count, 3);
        assert_eq!(aggregated.objects_total_size, 42);
        assert_eq!(aggregated.buckets_count, 1);
        assert_eq!(aggregated.buckets_usage.get("bucket-a").map(|b| (b.objects_count, b.size)), Some((3, 42)));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_reflects_recent_delete_before_scanner_persists() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 42, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[test]
    fn remove_bucket_usage_from_info_drops_bucket_and_recomputes_totals() {
        let last_update = SystemTime::now();
        let mut info = data_usage_info_for_test("bucket-a", 2, 84, last_update);
        info.buckets_usage.insert(
            "bucket-b".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 126,
                ..Default::default()
            },
        );
        info.bucket_sizes.insert("bucket-b".to_string(), 126);
        info.buckets_count = 2;
        info.calculate_totals();

        assert!(remove_bucket_usage_from_info(&mut info, "bucket-a"));

        assert_eq!(info.buckets_count, 1);
        assert_eq!(info.objects_total_count, 3);
        assert_eq!(info.objects_total_size, 126);
        assert_eq!(info.last_update, Some(last_update));
        assert!(!info.buckets_usage.contains_key("bucket-a"));
        assert!(!info.bucket_sizes.contains_key("bucket-a"));
        assert_eq!(
            info.buckets_usage
                .get("bucket-b")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((3, 126))
        );
    }

    #[test]
    fn merge_bucket_usage_removal_preserves_current_snapshot() {
        let now = SystemTime::now();
        let candidate = data_usage_info_for_test("bucket-a", 2, 84, now - Duration::from_secs(10));
        let mut existing = data_usage_info_for_test("bucket-a", 4, 168, now);
        existing.buckets_usage.insert(
            "bucket-c".to_string(),
            BucketUsageInfo {
                objects_count: 5,
                versions_count: 5,
                size: 210,
                ..Default::default()
            },
        );
        existing.bucket_sizes.insert("bucket-c".to_string(), 210);
        existing.buckets_count = 2;
        existing.calculate_totals();

        let merged = merge_bucket_usage_removal(candidate, Some(existing), "bucket-a")
            .expect("bucket-a should be removed from the current snapshot");

        assert_eq!(merged.last_update, Some(now));
        assert_eq!(merged.buckets_count, 1);
        assert_eq!(merged.objects_total_count, 5);
        assert_eq!(merged.objects_total_size, 210);
        assert!(!merged.buckets_usage.contains_key("bucket-a"));
        assert_eq!(
            merged
                .buckets_usage
                .get("bucket-c")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((5, 210))
        );
    }

    #[tokio::test]
    #[serial]
    async fn clear_bucket_usage_memory_prevents_deleted_bucket_overlay() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 42, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;
        clear_bucket_usage_memory("bucket-a").await;

        let mut response = DataUsageInfo {
            last_update: Some(SystemTime::now()),
            ..Default::default()
        };
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.buckets_count, 0);
        assert_eq!(response.objects_total_count, 0);
        assert!(!response.buckets_usage.contains_key("bucket-a"));
        assert!(!response.bucket_sizes.contains_key("bucket-a"));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_preserves_object_count_for_overwrite() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_memory("bucket-a", Some(10), 20).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.objects_total_size, 20);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1, 20))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_counts_versioned_overwrite_as_new_version() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_version_write_memory("bucket-a", Some(10), 20).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 2, 30))
        );
    }

    /// rustfs/backlog#1009: with an unknown previous state, only the
    /// always-correct components are recorded — bytes are added and a
    /// versioned write adds a version, but objects_count never moves.
    #[tokio::test]
    #[serial]
    async fn memory_overlay_unknown_previous_adds_size_only_for_unversioned_write() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_unknown_previous_memory("bucket-a", 20, false).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_unknown_previous_adds_size_and_version_for_versioned_write() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_unknown_previous_memory("bucket-a", 20, true).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 2, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_records_delete_marker_without_removing_versions() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 2, 30, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_delete_marker_memory("bucket-a").await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 2);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| { (usage.objects_count, usage.versions_count, usage.delete_markers_count, usage.size,) }),
            Some((2, 2, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_versioned_refresh_replaces_stale_dirty_memory() {
        clear_usage_memory_cache_for_test().await;

        let old_persisted = data_usage_info_for_test("bucket-a", 0, 0, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_write_memory("bucket-a", None, 15).await;

        let authoritative = BucketUsageInfo {
            objects_count: 1,
            versions_count: 1,
            delete_markers_count: 1,
            size: 10,
            ..Default::default()
        };
        replace_bucket_usage_memory_from_authoritative("bucket-a", authoritative.clone(), SystemTime::now()).await;

        let mut response = old_persisted.clone();
        response.buckets_usage.insert("bucket-a".to_string(), authoritative);
        response.bucket_sizes.insert("bucket-a".to_string(), 10);
        response.calculate_totals();

        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(response.objects_total_size, 10);
        assert_eq!(
            response.buckets_usage.get("bucket-a").map(|usage| (
                usage.objects_count,
                usage.versions_count,
                usage.delete_markers_count,
                usage.size
            )),
            Some((1, 1, 1, 10))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_versioned_refresh_preserves_newer_dirty_memory() {
        clear_usage_memory_cache_for_test().await;

        let old_persisted = data_usage_info_for_test("bucket-a", 0, 0, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        let refresh_started = SystemTime::now() - Duration::from_secs(1);
        record_bucket_object_write_memory("bucket-a", None, 15).await;

        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 1,
                versions_count: 1,
                delete_markers_count: 1,
                size: 10,
                ..Default::default()
            },
            refresh_started,
        )
        .await;

        let mut response = old_persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.delete_markers_total_count, 0);
        assert_eq!(response.objects_total_size, 15);
        assert_eq!(
            response.buckets_usage.get("bucket-a").map(|usage| (
                usage.objects_count,
                usage.versions_count,
                usage.delete_markers_count,
                usage.size
            )),
            Some((1, 1, 0, 15))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_dirty_delete_marker_with_later_snapshot() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 2, 30, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_delete_marker_memory("bucket-a").await;

        let scanner_without_marker = data_usage_info_for_test("bucket-a", 2, 30, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_without_marker).await;

        let mut response = scanner_without_marker.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 2);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| { (usage.objects_count, usage.versions_count, usage.delete_markers_count, usage.size,) }),
            Some((2, 2, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_does_not_replace_newer_persisted_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut newer_persisted = data_usage_info_for_test("bucket-a", 2, 84, now + Duration::from_secs(10));
        apply_bucket_usage_memory_overlay(&mut newer_persisted).await;

        assert_eq!(newer_persisted.objects_total_count, 2);
        assert_eq!(newer_persisted.objects_total_size, 84);
        assert_eq!(
            newer_persisted
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((2, 84))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_snapshot_can_reduce_object_layer_refreshed_memory_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 100,
                versions_count: 100,
                size: 1_000,
                ..Default::default()
            },
            now,
        )
        .await;

        let scanner_decrease = data_usage_info_for_test("bucket-a", 60, 600, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_decrease).await;

        let mut response = scanner_decrease.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 60);
        assert_eq!(response.objects_total_size, 600);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((60, 600))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_backend_snapshot_can_reduce_backend_memory_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let complete_snapshot = data_usage_info_for_test("bucket-a", 100, 1_000, now);
        replace_bucket_usage_memory_from_info(&complete_snapshot).await;

        let scanner_decrease = data_usage_info_for_test("bucket-a", 60, 600, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_decrease).await;

        let mut response = scanner_decrease.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 60);
        assert_eq!(response.objects_total_size, 600);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((60, 600))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_refresh_can_reduce_memory_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let complete_snapshot = data_usage_info_for_test("bucket-a", 100, 1_000, now);
        replace_bucket_usage_memory_from_info(&complete_snapshot).await;

        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 60,
                versions_count: 60,
                size: 600,
                ..Default::default()
            },
            now + Duration::from_secs(10),
        )
        .await;

        let mut response = data_usage_info_for_test("bucket-a", 60, 600, now + Duration::from_secs(10));
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 60);
        assert_eq!(response.objects_total_size, 600);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((60, 600))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_newer_memory_update() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let scanner_snapshot = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(5));
        replace_bucket_usage_memory_from_info(&scanner_snapshot).await;

        let mut response = scanner_snapshot.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_dirty_memory_update_with_later_partial_snapshot() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 900, 9_000, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;

        for _ in 0..100 {
            record_bucket_object_write_memory("bucket-a", None, 10).await;
        }

        let scanner_partial = data_usage_info_for_test("bucket-a", 950, 9_500, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_partial).await;

        let mut response = scanner_partial.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1000);
        assert_eq!(response.objects_total_size, 10_000);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1000, 10_000))
        );
    }

    // --- CompressionTotalState tests ---

    /// Reset the compression total cache to a known state for isolated tests.
    async fn reset_compression_cache(state: CompressionTotalState) {
        *COMPRESSION_TOTAL_MEMORY_CACHE.write().await = Some(state);
    }

    #[test]
    fn compression_state_default_values() {
        let state = CompressionTotalState::default();
        assert!(!state.inited, "default state should not be inited");
        assert_eq!(state.ops_since_persist, 0);
        assert_eq!(state.info.original_bytes_total, 0);
        assert_eq!(state.info.compressed_bytes_total, 0);
        assert_eq!(state.info.compression_operations_total, 0);
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_skips_when_not_inited() {
        let mut pre_state = CompressionTotalState::default();
        // Set known values that should remain unchanged when inited=false
        pre_state.info.original_bytes_total = 42;
        pre_state.info.compressed_bytes_total = 21;
        pre_state.info.compression_operations_total = 7;
        pre_state.ops_since_persist = 5;
        // inited is already false from default()

        reset_compression_cache(pre_state.clone()).await;

        // Call with sizes that would be accumulated if inited were true
        record_compression_total_memory(100, 50).await;

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");
        assert!(!state.inited);
        assert_eq!(state.info.original_bytes_total, 42, "original should be unchanged");
        assert_eq!(state.info.compressed_bytes_total, 21, "compressed should be unchanged");
        assert_eq!(state.info.compression_operations_total, 7, "operations should be unchanged");
        assert_eq!(state.ops_since_persist, 5, "ops_since_persist should be unchanged");
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_accumulates_totals() {
        let state = CompressionTotalState {
            info: CompressionTotalInfo::default(),
            ops_since_persist: 0,
            last_persist: tokio::time::Instant::now(),
            inited: true,
        };

        reset_compression_cache(state).await;

        for _ in 0..3 {
            record_compression_total_memory(100, 50).await;
        }

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");
        assert!(state.inited);
        assert_eq!(state.info.original_bytes_total, 300);
        assert_eq!(state.info.compressed_bytes_total, 150);
        assert_eq!(state.info.compression_operations_total, 3);
        assert_eq!(state.ops_since_persist, 3);
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_triggers_persist_on_batch_full() {
        // Simulate 99 previous records, so the next (100th) hits the batch threshold.
        let state = CompressionTotalState {
            info: CompressionTotalInfo {
                original_bytes_total: 9900,
                compressed_bytes_total: 4950,
                compression_operations_total: 99,
            },
            ops_since_persist: 99,
            last_persist: tokio::time::Instant::now(),
            inited: true,
        };

        reset_compression_cache(state).await;

        // 100th record — should trigger the debounce flush.
        record_compression_total_memory(100, 50).await;

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");

        // Totals are correctly accumulated (not reset by the flush).
        assert_eq!(state.info.original_bytes_total, 10_000); // 9900 + 100
        assert_eq!(state.info.compressed_bytes_total, 5_000); // 4950 + 50
        assert_eq!(state.info.compression_operations_total, 100); // 99 + 1

        // Debounce counter is reset after the persist trigger.
        assert_eq!(state.ops_since_persist, 0);

        // last_persist was refreshed; it should be very recent.
        assert!(
            state.last_persist.elapsed() < Duration::from_secs(3),
            "last_persist should have been refreshed to now"
        );

        // try_flush_compression_total is spawned asynchronously here.
        // In a unit test, runtime_sources::object_store_handle() returns None,
        // so the spawned task will hit the "object store not initialized" error
        // path and return gracefully. The state mutation (counter reset + totals
        // accumulation) is the critical behaviour verified above.
    }
}
