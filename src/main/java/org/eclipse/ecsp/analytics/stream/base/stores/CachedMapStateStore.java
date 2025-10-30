package org.eclipse.ecsp.analytics.stream.base.stores;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.dao.CacheBackedInMemoryBatchCompleteCallBack;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
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
 * A Java Map based in-memory state-store. 
 *
 * @param <K> The first type parameter.
 * @param <V> The second type parameter.
 */
@Component
@Scope("prototype")
public class CachedMapStateStore<K extends CacheKeyConverter<K>, V extends IgniteEntity>
        extends GenericMapStateStore<K, V>
        implements MutableKeyValueStore<K, V> {

    /** The logger. */
    private static IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(CachedMapStateStore.class);

    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The bypass. */
    @Autowired
    private CacheBypass<K, V> bypass;

    /** The cache guage. */
    @Autowired
    private InternalCacheGuage cacheGuage;
    
    /** The sub services. */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;
    
    /** The svc. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String svc;
    
    /** The node name. */
    @Value("${NODE_NAME:localhost}")
    private String nodeName;
    
    /** The persist in ignite cache. */
    private boolean persistInIgniteCache = true;
    
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
     * Sets the taskId. 
     *
     * @param taskId The topic's partition ID.
     **/
    public void setTaskId(String taskId) {
        //This check has been applied for DMARetryRecordDAO, DMARetryBucketDAO, 
        //and ShoulderTapDAO who needs CacheBypass.
        if (taskId == null) {
            LOGGER.error("null taskId received");
            return;
        }
        this.taskId = taskId;
    }
    
    /**
     * Wrapper over Java Map's putIfAbsent.
     *
     * @param key the key
     * @param value the value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     * @return the v
     */
    public V putIfAbsent(K key, V value, Optional<MutationId> mutationId, String cacheType) {
        LOGGER.debug("Invoking putIfAbsent of CacheBackedGenericInMemoryDAOImpl with key {} and value {}", key, value);
        V oldValue = super.putIfAbsent(key, value);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
        
        if (persistInIgniteCache && oldValue == null) {
            CacheEntity<K, V> entity = new CacheEntity<>();
            entity.withKey(key).withValue(value).withMutationId(mutationId).withOperation(Operation.PUT);
            entity.setLastUpdatedTime(LocalDateTime.now());
            bypass.processEvents(entity);
        }
        return oldValue;
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
        LOGGER.debug("Invoking put of CacheBackedGenericInMemoryDAOImpl with key {} and value {} "
            + "and mutationId {}", key, value, mutationId);
        if (persistInIgniteCache) {
            CacheEntity<K, V> entity = new CacheEntity<>();
            entity.withKey(key).withValue(value).withMutationId(mutationId).withOperation(Operation.PUT);
            entity.setLastUpdatedTime(LocalDateTime.now());
            bypass.processEvents(entity);
        }
        super.put(key, value);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
    }

    /**
     * this is used to set values to Cache.
     *
     * @param key the key
     * @param value the value
     * @param cacheType the cache type
     */
    public void put(K key, V value, String cacheType) {
        LOGGER.debug("Invoking put of CacheBackedGenericInMemoryDAOImpl with key {} and value {}", key, value);
        put(key, value, Optional.empty(), cacheType);
        
    }

    /**
     * this is used to set values to Cache.
     *
     * @param key the key
     * @param cacheType the cache type
     * @return the v
     */
    public V delete(K key, String cacheType) {
        LOGGER.debug("Invoking delete of CacheBackedGenericInMemoryDAOImpl with key {}", key);
        delete(key, Optional.empty(), cacheType);
        return null;
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
        LOGGER.debug("Invoking delete of CacheBackedGenericInMemoryDAOImpl with key {} and "
            + "mutationId {}", key, mutationId);
        super.delete(key);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
        
        if (persistInIgniteCache) {
            CacheEntity<K, V> entity = new CacheEntity<>();
            entity.withKey(key).withMutationId(mutationId).withOperation(Operation.DEL);
            entity.setLastUpdatedTime(LocalDateTime.now());
            bypass.processEvents(entity);
        }
    }

    /**
     * Sets the call back.
     *
     * @param callBack the new call back
     */
    @Override
    public void setCallBack(CacheBackedInMemoryBatchCompleteCallBack callBack) {
        // Method to set instance of callback

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
     * Sets the persist in ignite cache.
     *
     * @param persistInIgniteCache the new persist in ignite cache
     */
    public void setPersistInIgniteCache(boolean persistInIgniteCache) {
        this.persistInIgniteCache = persistInIgniteCache;
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
        LOGGER.debug("Invoking put to map of CachedMapStateStore with key {} and value {} "
            + "and mutationId {}", mapEntryKey, mapEntryValue, mutationId);
        if (persistInIgniteCache) {
            CacheEntity<K, V> entity = new CacheEntity<>();
            entity.withMapKey(mapKey).withKey(mapEntryKey).withValue(mapEntryValue).withMutationId(mutationId)
                    .withOperation(Operation.PUT_TO_MAP);
            entity.setLastUpdatedTime(LocalDateTime.now());
            bypass.processEvents(entity);
        }
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
        LOGGER.debug("Invoking putIfAbsent to map of CachedMapStateStore with key {} and value {}", 
            mapEntryKey, mapEntryValue);
        V oldValue = super.putIfAbsent(mapEntryKey, mapEntryValue);
        if (persistInIgniteCache && oldValue == null) {
            CacheEntity<K, V> entity = new CacheEntity<>();
            entity.withMapKey(mapKey).withKey(mapEntryKey).withValue(mapEntryValue).withMutationId(mutationId)
                    .withOperation(Operation.PUT_TO_MAP);
            entity.setLastUpdatedTime(LocalDateTime.now());
            bypass.processEvents(entity);
        }
        super.put(mapEntryKey, mapEntryValue);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
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
        LOGGER.debug("Invoking delete from map of CachedMapStateStore with map key {}, "
             + "entry key {}, and mutationId {}", mapKey, mapEntryKey.convertToString(), mutationId);
        super.delete(mapEntryKey);
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
        
        if (persistInIgniteCache) {
            CacheEntity<K, V> entity = new CacheEntity<>();
            entity.withMapKey(mapKey).withKey(mapEntryKey).withMutationId(mutationId)
                    .withOperation(Operation.DEL_FROM_MAP);
            entity.setLastUpdatedTime(LocalDateTime.now());
            bypass.processEvents(entity);
        }
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
        /*
         * If sub-services for a service exist then there would be multiple VEHICLE_DEVICE_MAPPING parent
         * keys in redis as hivemq will be putting device status data at sub-service level. 
         * Now, unlike redis, in DMA we have only one map for device status data and keys of 
         * that map must be unique, which they won't,
         * because in redis same VIN's data could exist under different VEHICLE_DEVICE_MAPPING keys 
         * and while syncing in-memory with redis, there can't be 2 VINs as key. 
         * Hence, in in-memory, for device status map, key will look like below:
         * <VIN>;<sub-service> 
         * to maintain its uniqueness and also for DMA to know which VIN is active for which sub-service.
         */
        if (mapKey.startsWith(DMAConstants.VEHICLE_DEVICE_MAPPING)) {
            if (StringUtils.isNotEmpty(subServices)) {
                String[] arr = mapKey.split(":");
                pairs.forEach((k, v) ->
                    super.put(converter.convertFrom(k + DMAConstants.SEMI_COLON + arr[arr.length - 1]), v)
                );
            } else {
                pairs.forEach((k, v) ->
                    super.put(converter.convertFrom(k), v)
                );
            }
        } else {
            pairs.forEach((k, v) ->
                super.put(converter.convertFrom(k), v)
            );
        }
        cacheGuage.set(super.approximateNumEntries(), cacheType, svc, nodeName, taskId);
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