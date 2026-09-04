//! Tiny process-local TTL + LRU map used by PromQL caches.
//!
//! Evicts oldest entries under capacity pressure — never wipe the whole map
//! (that caused intermittent Grafana 100ms SLO misses under cell thrash).

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct TtlLruCache<K, V> {
    map: HashMap<K, Entry<V>>,
    order: VecDeque<K>,
    ttl: Duration,
    max: usize,
}

#[derive(Debug)]
struct Entry<V> {
    expires: Instant,
    value: V,
}

impl<K, V> TtlLruCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(ttl: Duration, max: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            ttl,
            max: max.max(1),
        }
    }

    pub fn get(&mut self, key: &K, now: Instant) -> Option<V> {
        let hit = self
            .map
            .get(key)
            .filter(|e| e.expires > now)
            .map(|e| e.value.clone());
        if hit.is_some() {
            self.touch(key);
        } else if self.map.contains_key(key) {
            self.map.remove(key);
            self.unlink(key);
        }
        hit
    }

    pub fn put(&mut self, key: K, value: V, now: Instant) {
        if self.map.contains_key(&key) {
            self.unlink(&key);
            self.map.remove(&key);
        }
        self.evict_expired(now);
        while self.map.len() >= self.max {
            let Some(victim) = self.order.pop_front() else {
                break;
            };
            self.map.remove(&victim);
        }
        self.map.insert(
            key.clone(),
            Entry {
                expires: now + self.ttl,
                value,
            },
        );
        self.order.push_back(key);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    fn touch(&mut self, key: &K) {
        self.unlink(key);
        self.order.push_back(key.clone());
    }

    fn unlink(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
    }

    fn evict_expired(&mut self, now: Instant) {
        let expired: Vec<K> = self
            .map
            .iter()
            .filter(|(_, e)| e.expires <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            self.map.remove(&key);
            self.unlink(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_oldest_not_clear_all() {
        let mut cache = TtlLruCache::new(Duration::from_secs(60), 4);
        let now = Instant::now();
        cache.put("a", 1, now);
        cache.put("b", 2, now);
        cache.put("c", 3, now);
        cache.put("d", 4, now);
        cache.put("e", 5, now);
        assert_eq!(cache.len(), 4);
        assert!(cache.get(&"a", now).is_none());
        assert_eq!(cache.get(&"e", now), Some(5));
        assert!(cache.len() > 2);
    }

    #[test]
    fn recently_touched_survives_pressure() {
        let mut cache = TtlLruCache::new(Duration::from_secs(60), 4);
        let now = Instant::now();
        cache.put("hot", 1, now);
        cache.put("b", 2, now);
        cache.put("c", 3, now);
        assert!(cache.get(&"hot", now).is_some());
        cache.put("d", 4, now);
        cache.put("e", 5, now);
        assert_eq!(cache.get(&"hot", now), Some(1));
    }

    #[test]
    fn expired_entries_miss_and_are_dropped() {
        let mut cache = TtlLruCache::new(Duration::from_millis(10), 8);
        let now = Instant::now();
        cache.put("k", 1, now);
        assert!(cache.get(&"k", now + Duration::from_millis(20)).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn get_does_not_extend_ttl() {
        // Fixed TTL only — hits must not slide expires (Grafana SLO thrash).
        let mut cache = TtlLruCache::new(Duration::from_millis(50), 8);
        let t0 = Instant::now();
        cache.put("k", 1, t0);
        assert_eq!(cache.get(&"k", t0 + Duration::from_millis(30)), Some(1));
        assert!(
            cache.get(&"k", t0 + Duration::from_millis(60)).is_none(),
            "get must not refresh expires past the original put TTL"
        );
    }
}
