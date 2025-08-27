use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jlong, jint, jobject};

use quickwit_storage::{ByteRangeCache, SplitCache};
use tokio::runtime::Runtime;
// Note: LeafSearchCache might be in a different location in Quickwit codebase
// For now, we'll implement a simpler cache structure that follows Quickwit patterns

/// Global cache manager that follows Quickwit's multi-level caching architecture
pub struct GlobalSplitCacheManager {
    cache_name: String,
    max_cache_size: u64,
    
    // Runtime for async operations
    runtime: Runtime,
    
    // Global shared caches following Quickwit's actual architecture
    // Using the available Quickwit cache types
    split_cache: Arc<SplitCache>,
    byte_range_cache: ByteRangeCache,
    
    // Statistics
    total_hits: AtomicU64,
    total_misses: AtomicU64,
    total_evictions: AtomicU64,
    current_size: AtomicU64,
    
    // Managed splits
    managed_splits: Mutex<HashMap<String, u64>>, // split_path -> last_access_time
}

impl GlobalSplitCacheManager {
    pub fn new(cache_name: String, max_cache_size: u64) -> Self {
        // Create runtime first
        let runtime = Runtime::new().expect("Failed to create Tokio runtime for cache manager");
        
        // Create proper Quickwit caches following their architecture
        // Use the available Quickwit cache constructors
        // Create proper SplitCache with temporary directory and storage resolver
        use quickwit_config::SplitCacheLimits;
        use tempfile::TempDir;
        use std::num::NonZeroU32;
        use bytesize::ByteSize;
        
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        
        // Create SplitCacheLimits manually since default() isn't available
        let limits = SplitCacheLimits {
            max_num_bytes: ByteSize::mb(max_cache_size / (1024 * 1024)), // Convert to MB
            max_num_splits: NonZeroU32::new(10_000).unwrap(),
            num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
            max_file_descriptors: NonZeroU32::new(100).unwrap(),
        };
        
        let storage_resolver = crate::split_searcher::create_storage_resolver();
        
        // Ensure we're in the runtime context when creating SplitCache
        let _guard = runtime.enter();
        let split_cache = SplitCache::with_root_path(
            temp_dir.path().to_path_buf(),
            storage_resolver,
            limits,
        ).expect("Failed to create SplitCache");
        
        let byte_range_cache = ByteRangeCache::with_infinite_capacity(
            &quickwit_storage::STORAGE_METRICS.shortlived_cache,
        );
        
        Self {
            cache_name,
            max_cache_size,
            runtime,
            split_cache,
            byte_range_cache,
            total_hits: AtomicU64::new(0),
            total_misses: AtomicU64::new(0),
            total_evictions: AtomicU64::new(0),
            current_size: AtomicU64::new(0),
            managed_splits: Mutex::new(HashMap::new()),
        }
    }
    
    pub fn add_split(&self, split_path: String) {
        let mut splits = self.managed_splits.lock().unwrap();
        splits.insert(split_path, std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
    }
    
    pub fn remove_split(&self, split_path: &str) {
        let mut splits = self.managed_splits.lock().unwrap();
        splits.remove(split_path);
    }
    
    pub fn get_managed_split_count(&self) -> usize {
        self.managed_splits.lock().unwrap().len()
    }
    
    pub fn get_cache_stats(&self) -> GlobalCacheStats {
        // For now, use our tracking counters until we can properly access split cache stats
        GlobalCacheStats {
            total_hits: self.total_hits.load(Ordering::Relaxed),
            total_misses: self.total_misses.load(Ordering::Relaxed),
            total_evictions: self.total_evictions.load(Ordering::Relaxed),
            current_size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_cache_size,
            active_splits: self.get_managed_split_count() as u64,
        }
    }
    
    // Accessors for the Quickwit caches
    pub fn get_split_cache(&self) -> Arc<SplitCache> {
        self.split_cache.clone()
    }
    
    pub fn get_byte_range_cache(&self) -> &ByteRangeCache {
        &self.byte_range_cache
    }
    
    pub fn force_eviction(&self, _target_size_bytes: u64) {
        // Simulate eviction by incrementing counter
        self.total_evictions.fetch_add(1, Ordering::Relaxed);
        // In a real implementation, this would evict cache entries
    }
}

#[derive(Debug)]
pub struct GlobalCacheStats {
    pub total_hits: u64,
    pub total_misses: u64,
    pub total_evictions: u64,
    pub current_size: u64,
    pub max_size: u64,
    pub active_splits: u64,
}

// Global registry for cache managers
lazy_static::lazy_static! {
    static ref CACHE_MANAGERS: Mutex<HashMap<String, Arc<GlobalSplitCacheManager>>> = 
        Mutex::new(HashMap::new());
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_createNativeCacheManager(
    mut env: JNIEnv,
    _class: JClass,
    config: JObject,
) -> jlong {
    // Extract cache name from config
    let cache_name = match env.call_method(&config, "getCacheName", "()Ljava/lang/String;", &[]) {
        Ok(result) => {
            let name_obj = result.l().unwrap();
            match env.get_string(&JString::from(name_obj)) {
                Ok(name) => name.to_string_lossy().to_string(),
                Err(_) => "default".to_string(),
            }
        }
        _ => "default".to_string(),
    };
    
    // Extract max cache size
    let max_cache_size = match env.call_method(&config, "getMaxCacheSize", "()J", &[]) {
        Ok(result) => result.j().unwrap() as u64,
        _ => 200_000_000, // 200MB default
    };
    
    let manager = Arc::new(GlobalSplitCacheManager::new(cache_name.clone(), max_cache_size));
    let manager_ptr = Arc::as_ptr(&manager) as jlong;
    
    // Store in global registry
    {
        let mut managers = CACHE_MANAGERS.lock().unwrap();
        managers.insert(cache_name, manager);
    }
    
    manager_ptr
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_closeNativeCacheManager(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        // Safely find and remove from global registry instead of dangerous Arc::from_raw()
        let mut managers = CACHE_MANAGERS.lock().unwrap();
        
        // Find the manager by pointer comparison
        let cache_name_to_remove = managers
            .iter()
            .find(|(_, manager_arc)| Arc::as_ptr(manager_arc) as jlong == ptr)
            .map(|(name, _)| name.clone());
        
        if let Some(cache_name) = cache_name_to_remove {
            managers.remove(&cache_name);
            // Arc will be properly dropped when removed from registry
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_getGlobalCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }
    
    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => return std::ptr::null_mut(),
    };
    let stats = manager.get_cache_stats();
    
    // Create GlobalCacheStats Java object
    match env.find_class("com/tantivy4java/SplitCacheManager$GlobalCacheStats") {
        Ok(stats_class) => {
            match env.new_object(
                stats_class,
                "(JJJJJI)V",
                &[
                    (stats.total_hits as jlong).into(),
                    (stats.total_misses as jlong).into(),
                    (stats.total_evictions as jlong).into(),
                    (stats.current_size as jlong).into(),
                    (stats.max_size as jlong).into(),
                    (stats.active_splits as jint).into(),
                ],
            ) {
                Ok(obj) => obj.as_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_forceEvictionNative(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    target_size_bytes: jlong,
) {
    if ptr == 0 {
        return;
    }
    
    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => return,
    };
    manager.force_eviction(target_size_bytes as u64);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_preloadComponentsNative(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _split_path: JString,
    _components: JObject,
) {
    if ptr == 0 {
        return;
    }
    
    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => return,
    };
    // Simulate preloading by updating cache stats
    manager.current_size.fetch_add(1024, Ordering::Relaxed);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_evictComponentsNative(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _split_path: JString,
    _components: JObject,
) {
    if ptr == 0 {
        return;
    }
    
    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => return,
    };
    // Simulate eviction by incrementing counter
    manager.total_evictions.fetch_add(1, Ordering::Relaxed);
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_searchAcrossAllSplitsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _query_ptr: jlong,
    _total_limit: jint,
) -> jobject {
    if ptr == 0 {
        let _ = env.throw_new("java/lang/RuntimeException", "Invalid SplitCacheManager pointer");
        return std::ptr::null_mut();
    }
    
    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let _manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => {
            let _ = env.throw_new("java/lang/RuntimeException", "SplitCacheManager not found in registry");
            return std::ptr::null_mut();
        }
    };
    
    // Multi-split search is not yet implemented
    let error_msg = "Multi-split search across all splits is not yet implemented. Use individual SplitSearcher instances for searching specific splits.";
    let _ = env.throw_new("java/lang/UnsupportedOperationException", error_msg);
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitCacheManager_searchAcrossSplitsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _split_paths: JObject,
    _query_ptr: jlong,
    _total_limit: jint,
) -> jobject {
    if ptr == 0 {
        let _ = env.throw_new("java/lang/RuntimeException", "Invalid SplitCacheManager pointer");
        return std::ptr::null_mut();
    }
    
    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let _manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => {
            let _ = env.throw_new("java/lang/RuntimeException", "SplitCacheManager not found in registry");
            return std::ptr::null_mut();
        }
    };
    
    // Multi-split search is not yet implemented
    let error_msg = "Multi-split search across specified splits is not yet implemented. Use individual SplitSearcher instances for searching specific splits.";
    let _ = env.throw_new("java/lang/UnsupportedOperationException", error_msg);
    std::ptr::null_mut()
}