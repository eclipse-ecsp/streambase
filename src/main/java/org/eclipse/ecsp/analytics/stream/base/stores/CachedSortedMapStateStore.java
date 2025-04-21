package org.eclipse.ecsp.analytics.stream.base.stores;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.dao.CacheBackedInMemoryBatchCompleteCallBack;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An ordered Java Map based in-memory state-store. The entries in this map are ordered by timestamp.
 *
 * @param <K> The key type parameter.
 * @param <V> The value type parameter.
 */
@Component
@Scope("prototype")
public class CachedSortedMapStateStore<K extends CacheKeyConverter<K>, V extends IgniteEntity>
        extends GenericSortedMapStateStore<K, V> implements MutableKeyValueStore<K, V> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(CachedSortedMapStateStore.class);
    
    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The bypass. */
    @Autowired
    private CacheBypass<K, V> bypass;
    
    /** The cache guage. */
    @Autowired
    private InternalCacheGuage cacheGuage;
    
    /** The svc. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String svc;
    
    /** The node name. */
    @Value("${NODE_NAME:localhost}")
    private String nodeName;
    
    /** The task id. */
    //This taskId has been added here to pass it down to CacheBypass from syncWithMapCacheMethod
    private String taskId;

    /**
     * Sets the cache.
     *
     * @param cache the new cache
     */
    public void setCache(IgniteCache cache) {
        this.cache = cache;
    }

    /**
     * Sets the bypass.
     *
     * @param bypass the bypass
     */
    public void setBypass(CacheBypass<K, V> bypass) {
        this.bypass = bypass;
    }
    /**
     * Sets the task ID. 
     *
     * @param taskId The topic partition ID.
     */
    
    public void setTaskId(String taskId) {
        //This check has been applied for DMARetryRecordDAO, DMARetryBucketDAO, and 
        //ShoulderTapDAO who needs CacheBypass.
        if (taskId == null) {
            logger.error("null taskId received");
            return;
        }
        this.taskId = taskId;
    }

    /**
     * Wrapper over Java Map's put functionality. 
     *
     * @param key The key to put in the map.
     * @param value The value associated with the key.
     * @param cacheType The name of the in-memory cache where this entry will be put.
     */
    public void put(K key, V value, String cacheType) {
        logger.debug("Invoking put of CacheBackedGenericInMemoryDAOImpl with key {} and value {}", key, value);
        put(key, value, Optional.empty(), cacheType);

    }
    
    /**
     * Put.
     *
     * @param key the key
     * @param value the value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     */
    @Override
    public void put(K key, V value, Optional<MutationId> mutationId, String cacheType) {
        logger.debug("Invoking put of CacheBackedGenericInMemoryDAOImpl with key {} and value {} "
            + "and mutationId {}", key, value, mutationId);
        putToCache(key, value, mutationId);
        super.put(key, value);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
    }

    /**
     * Put if absent.
     *
     * @param key the key
     * @param value the value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     * @return the v
     */
    @Override
    public V putIfAbsent(K key, V value, Optional<MutationId> mutationId, String cacheType) {
        logger.debug("Invoking put of CacheBackedGenericInMemoryDAOImpl with key {} and value {} "
            + "and mutationId {}", key, value, mutationId);
        V oldValue = super.putIfAbsent(key, value);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);

        if (oldValue == null) {
            putToCache(key, value, mutationId);
        }
        return oldValue;
    }

    /**
     * Delete.
     *
     * @param key the key
     * @param mutationId the mutation id
     * @param cacheType the cache type
     */
    @Override
    public void delete(K key, Optional<MutationId> mutationId, String cacheType) {
        logger.debug("Invoking delete of CacheBackedGenericInMemoryDAOImpl with key {} and "
            + "mutationId", key, mutationId);
        super.delete(key);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
        deleteFromCache(key, mutationId);
    }
    
    /**
     * Wrapper over Java Map's delete function.
     *
     * @param key The key to delete.
     * @param cacheType The name of the in-memory cache from where this key will be deleted. 
     * @return the deleted value.
     */
    public V delete(K key, String cacheType) {
        logger.debug("Invoking delete of CacheBackedGenericInMemoryDAOImpl with key {}", key);
        delete(key, Optional.empty(), cacheType);
        return null;
    }

    /**
     * Delete from cache.
     *
     * @param key the key
     * @param mutationId the mutation id
     */
    private void deleteFromCache(K key, Optional<MutationId> mutationId) {
        CacheEntity<K, V> entity = new CacheEntity<>();
        entity.withKey(key).withOperation(Operation.DEL);
        if (mutationId.isPresent()) {
            entity.withMutationId(mutationId);
        }
        entity.setLastUpdatedTime(LocalDateTime.now());
        bypass.processEvents(entity);
    }

    /**
     * Sets the call back.
     *
     * @param callBack the new call back
     */
    @Override
    public void setCallBack(CacheBackedInMemoryBatchCompleteCallBack callBack) {
        // Implementataion Pending

    }

    /**
     * Sync withcache.
     *
     * @param regex the regex
     * @param converter the converter
     */
    @Override
    public void syncWithcache(String regex, K converter) {
        Map<String, V> pairs = cache.getKeyValuePairsForRegex(regex, Optional.of(false));
        pairs.forEach((k, v) ->
            super.put(converter.convertFrom(k), v)
        );
    }

    /**
     * Put to cache.
     *
     * @param key the key
     * @param value the value
     * @param mutationId the mutation id
     */
    private void putToCache(K key, V value, Optional<MutationId> mutationId) {
        CacheEntity<K, V> entity = new CacheEntity<>();
        entity.withKey(key).withValue(value).withOperation(Operation.PUT);
        if (mutationId.isPresent()) {
            entity.withMutationId(mutationId);
        }
        entity.setLastUpdatedTime(LocalDateTime.now());
        bypass.processEvents(entity);
    }

    /**
     * Put to map.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @param mapEntryValue the map entry value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     */
    @Override
    public void putToMap(String mapKey, K mapEntryKey, V mapEntryValue, 
            Optional<MutationId> mutationId, String cacheType) {
        logger.debug("Invoking put to map of CachedMapStateStore with key {} and value {} "
            + "and mutationId {}", mapEntryKey, mapEntryValue, mutationId);
        putToMapCache(mapKey, mapEntryKey, mapEntryValue, mutationId);
        super.put(mapEntryKey, mapEntryValue);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
    }

    /**
     * Put to map if absent.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @param mapEntryValue the map entry value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     * @return the v
     */
    @Override
    public V putToMapIfAbsent(String mapKey, K mapEntryKey, V mapEntryValue, 
            Optional<MutationId> mutationId, String cacheType) {
        logger.debug("Invoking putIfAbsent to map of CachedMapStateStore with key {} and value {}", 
            mapEntryKey.convertToString(), mapEntryValue);
        V oldValue = super.putIfAbsent(mapEntryKey, mapEntryValue);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
        if (oldValue == null) {
            putToMapCache(mapKey, mapEntryKey, mapEntryValue, mutationId);
        }
        return oldValue;
    }

    /**
     * Delete from map.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @param mutationId the mutation id
     * @param cacheType the cache type
     */
    @Override
    public void deleteFromMap(String mapKey, K mapEntryKey, Optional<MutationId> mutationId, String cacheType) {
        logger.debug("Invoking delete from map of CachedMapStateStore with key {} and "
            + "mutationId {}", mapEntryKey, mutationId);
        super.delete(mapEntryKey);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
        deleteFromMapCache(mapKey, mapEntryKey, mutationId);
    }

    /**
     * Sync with map cache.
     *
     * @param mapKey the map key
     * @param converter the converter
     * @param cacheType the cache type
     */
    @Override
    public void syncWithMapCache(String mapKey, K converter, String cacheType) {
        GetMapOfEntitiesRequest getReq = new GetMapOfEntitiesRequest();
        getReq.withKey(mapKey);
        getReq.withNamespaceEnabled(false);
        Map<String, V> pairs = cache.getMapOfEntities(getReq);
        pairs.forEach((k, v) ->
            super.put(converter.convertFrom(k), v)
        );

        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
    }

    /**
     * Delete from map cache.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @param mutationId the mutation id
     */
    private void deleteFromMapCache(String mapKey, K mapEntryKey, Optional<MutationId> mutationId) {
        CacheEntity<K, V> entity = new CacheEntity<>();
        entity.withMapKey(mapKey).withKey(mapEntryKey);
        if (mutationId.isPresent()) {
            entity.withMutationId(mutationId);
        }
        entity.withOperation(Operation.DEL_FROM_MAP);
        entity.setLastUpdatedTime(LocalDateTime.now());
        bypass.processEvents(entity);
    }

    /**
     * Put to map cache.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @param mapEntryValue the map entry value
     * @param mutationId the mutation id
     */
    private void putToMapCache(String mapKey, K mapEntryKey, V mapEntryValue, Optional<MutationId> mutationId) {
        CacheEntity<K, V> entity = new CacheEntity<>();
        entity.withMapKey(mapKey).withKey(mapEntryKey).withValue(mapEntryValue);
        if (mutationId.isPresent()) {
            entity.withMutationId(mutationId);
        }
        entity.withOperation(Operation.PUT_TO_MAP);
        entity.setLastUpdatedTime(LocalDateTime.now());
        bypass.processEvents(entity);
    }

    /**
     * Force get.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @return the v
     */
    @Override
    public V forceGet(String mapKey, K mapEntryKey) {
        GetMapOfEntitiesRequest getReq = new GetMapOfEntitiesRequest();
        getReq.withKey(mapKey);
        Set<String> subKeys = new HashSet<>();
        String keyAsString = mapEntryKey.convertToString();
        subKeys.add(keyAsString);
        getReq.withFields(subKeys);
        getReq.withNamespaceEnabled(false);
        V result = null;
        Map<String, V> map = cache.getMapOfEntities(getReq);
        if (map != null) {
            result = map.get(keyAsString);
        }
        return result;
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        super.close();
        bypass.close();
    }
}