/*
 *
 *
 *   ******************************************************************************
 *
 *    Copyright (c) 2023-24 Harman International
 *
 *
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *
 *    you may not use this file except in compliance with the License.
 *
 *    You may obtain a copy of the License at
 *
 *
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *    Unless required by applicable law or agreed to in writing, software
 *
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *    See the License for the specific language governing permissions and
 *
 *    limitations under the License.
 *
 *
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    *******************************************************************************
 *
 *
 */

package org.eclipse.ecsp.analytics.stream.base.stores;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.BatchWritingStore;
import org.apache.kafka.streams.state.internals.BlockBasedTableConfigWithAccessibleCache;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.context.StreamBaseSpringContext;
import org.eclipse.ecsp.analytics.stream.base.metrics.reporter.HarmanRocksDBMetricsExporter;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.getMetricsImpl;
import static org.eclipse.ecsp.analytics.stream.base.KafkaStreamsProcessorContext.StoreType.ROCKSDB;

/**
 * A persistent key-value store based on RocksDB.
 * Note that the use of array-typed keys is discouraged because they result
 * in incorrect caching behavior. If you intend to work on byte
 * arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than
 * {@code RocksDBStore<byte[], ...>}.
 *
 * @param <K>
 *         The key type
 * @param <V>
 *         The value type
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class HarmanRocksDBStore<K, V> implements KeyValueStore<K, V>, BatchWritingStore {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(HarmanRocksDBStore.class);
    
    /** The Constant COMPRESSION_TYPE. */
    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    
    /** The Constant COMPACTION_STYLE. */
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    
    /** The Constant WRITE_BUFFER_SIZE. */
    private static final long WRITE_BUFFER_SIZE = 32 * 1024 * 1024L;
    
    /** The Constant BLOCK_CACHE_SIZE. */
    private static final long BLOCK_CACHE_SIZE = 100 * 1024 * 1024L;
    
    /** The Constant BLOCK_SIZE. */
    private static final long BLOCK_SIZE = 4096L;
    
    /** The Constant MAX_WRITE_BUFFERS. */
    private static final int MAX_WRITE_BUFFERS = 3;
    
    /** The Constant DB_FILE_DIR. */
    private static final String DB_FILE_DIR = "rocksdb";
    
    /** The Constant LOG_FILE_TIME_TO_ROLL. */
    private static final long LOG_FILE_TIME_TO_ROLL = 30 * 60 * 60 * 24L;    //30-days
    
    /** The Constant KEEP_LOG_FILE_NUM. */
    private static final long KEEP_LOG_FILE_NUM = 5;    //5-files max
    
    /** The Constant MAX_LOG_FILE_SIZE. */
    private static final long MAX_LOG_FILE_SIZE = 200 * 1024 * 1024L;        //200Mb each file
    
    /** The Constant RECYCLE_LOG_FILE_NUM. */
    private static final long RECYCLE_LOG_FILE_NUM = 0L;
    
    /** The Constant POSITION_FILE_SUFFIX. */
    private static final String POSITION_FILE_SUFFIX = ".position";
    
    /** The Constant FROM_STORE. */
    private static final String FROM_STORE = " from store ";
    
    /** The prop. */
    private static Properties prop;
    
    /** The name. */
    private final String name;
    
    /** The parent dir. */
    private final String parentDir;
    
    /** The open iterators. */
    private final Set<KeyValueIterator> openIterators = new HashSet<>();
    
    /** The key serde. */
    private final Serde<K> keySerde;
    
    /** The value serde. */
    private final Serde<V> valueSerde;
    
    /** The metrics recorder. */
    private final RocksDBMetricsRecorder metricsRecorder;
    
    /** The open. */
    protected volatile boolean open = false;
    
    /** The context. */
    protected StateStoreContext context;
    
    /** The position. */
    protected Position position;

    /** The db dir. */
    File dbDir;
    
    /** The cache. */
    private Cache cache;
    
    /** The serdes. */
    private StateSerdes<K, V> serdes;
    
    /** The db. */
    private RocksDB db;
    // the following option objects will be created in the constructor and
    /** The options. */
    // closed in the close() method
    private Options options;
    
    /** The write options. */
    private WriteOptions writeOptions;
    
    /** The flush options. */
    private FlushOptions flushOptions;
    
    /** The state store config. */
    private Properties stateStoreConfig;
    
    /** The consistency enabled. */
    private boolean consistencyEnabled = false;    
    
    /** The user specified statistics. */
    private boolean userSpecifiedStatistics = false;
    
    /** The rocks DB enabled. */
    private boolean rocksDBEnabled;

    /**
     * Instantiates a new harman rocks DB store.
     *
     * @param name the name
     * @param keySerde the key serde
     * @param valueSerde the value serde
     * @param stateStoreConfig the state store config
     */
    public HarmanRocksDBStore(String name, Serde<K> keySerde, Serde<V> valueSerde, Properties stateStoreConfig) {
        this(name, DB_FILE_DIR, keySerde, valueSerde, stateStoreConfig,
                new RocksDBMetricsRecorder(ROCKSDB.toString(), name));
    }

    /**
     * HarmanRocksDBStore.
     *
     * @param name name
     * @param parentDir parentDir
     * @param keySerde keySerde
     * @param valueSerde valueSerde
     * @param stateStoreConfig stateStoreConfig
     * @param metricsRecorder metricsRecorder
     */
    public HarmanRocksDBStore(String name, String parentDir, Serde<K> keySerde,
            Serde<V> valueSerde, Properties stateStoreConfig,
            final RocksDBMetricsRecorder metricsRecorder) {
        this.name = name;
        this.parentDir = parentDir;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.stateStoreConfig = stateStoreConfig;
        this.metricsRecorder = metricsRecorder;
    }

    /**
     * Sets the properties.
     *
     * @param prop the new properties
     */
    public static void setProperties(Properties prop) {
        LOG.info("Setting env properties in HarmanRocksDBStore...");
        HarmanRocksDBStore.prop = prop;
    }

    /**
     * openDB connection.
     *
     * @param context StateStoreContext
     */
    @SuppressWarnings("unchecked")
    public void openDB(StateStoreContext context) {

        // initialize the default rocksdb options
        final BlockBasedTableConfigWithAccessibleCache tableConfig = new BlockBasedTableConfigWithAccessibleCache();
        cache = new LRUCache(BLOCK_CACHE_SIZE);
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(BLOCK_SIZE);

        options = new Options();
        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferSize(WRITE_BUFFER_SIZE);
        options.setCompressionType(COMPRESSION_TYPE);
        options.setCompactionStyle(COMPACTION_STYLE);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setCreateIfMissing(true);
        options.setErrorIfExists(false);

        // if values not provided via properties, it will be set to rocksdb provided default values.
        if (!stateStoreConfig.isEmpty()) {
            options.setLogFileTimeToRoll(stateStoreConfig.containsKey("rocksdb.log.file.roll.time")
                    ? Long.parseLong(stateStoreConfig.get("rocksdb.log.file.roll.time").toString())
                    : LOG_FILE_TIME_TO_ROLL);

            options.setKeepLogFileNum(stateStoreConfig.containsKey("rocksdb.log.files.to.keep")
                    ? Long.parseLong(stateStoreConfig.get("rocksdb.log.files.to.keep").toString())
                    : KEEP_LOG_FILE_NUM);

            options.setMaxLogFileSize(stateStoreConfig.containsKey("rocksdb.log.file.max.size")
                    ? Long.parseLong(stateStoreConfig.get("rocksdb.log.file.max.size").toString())
                    : MAX_LOG_FILE_SIZE);

            options.setRecycleLogFileNum(stateStoreConfig.containsKey("rocksdb.log.file.recycle.count")
                    ? Long.parseLong(stateStoreConfig.get("rocksdb.log.file.recycle.count").toString())
                    : RECYCLE_LOG_FILE_NUM);

            LOG.info(
                    "Rocks db properties finally set as: logFilesToKeep: {}, "
                            + "maxLogFileSize: {}, recycleLogFileNum: {}, logFileTimeToRoll: {}",
                    options.keepLogFileNum(), options.maxLogFileSize(),
                    options.recycleLogFileNum(), options.logFileTimeToRoll());
        }

        writeOptions = new WriteOptions();
        writeOptions.setDisableWAL(Boolean.valueOf((String) (!stateStoreConfig.isEmpty()
                && stateStoreConfig.containsKey("rocksdb.disable.wal")
                ? stateStoreConfig.get("rocksdb.disable.wal")
                : "true")));

        flushOptions = new FlushOptions();
        flushOptions.setWaitForFlush(true);

        final Map<String, Object> configs = context.appConfigs();
        final Class<RocksDBConfigSetter> configSetterClass = (Class<RocksDBConfigSetter>) configs
                .get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);
        if (configSetterClass != null) {
            final RocksDBConfigSetter configSetter = Utils.newInstance(configSetterClass);
            configSetter.setConfig(name, options, configs);
        }
        // we need to construct the serde while opening DB since
        // it is also triggered by windowed DB segments without initialization
        this.serdes = new StateSerdes<>(name,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.dbDir = new File(new File(context.stateDir(), parentDir), this.name);
        this.db = openRocksDb(this.dbDir, this.options);

        if (this.rocksDBEnabled) {
            maybeSetUpStatistics(configs);
            addValueProvidersToMetricsRecorder();
            LOG.info("Setting rocksDB instance in rocks db metrics exporter");
            setRocksDbObject();
        }
        open = true;
    }

    /**
     * Sets the rocks db object.
     */
    private void setRocksDbObject() {
        HarmanRocksDBMetricsExporter exporter;
        exporter = StreamBaseSpringContext.getBean(HarmanRocksDBMetricsExporter.class);
        LOG.info("Fetched bean: {} from spring context", exporter.getClass().getName());
        exporter.setRocksDb(this.db);
    }

    /**
     * Open rocks db.
     *
     * @param dir the dir
     * @param options the options
     * @return the rocks DB
     */
    private RocksDB openRocksDb(File dir, Options options) {
        try {
            dir.getParentFile().mkdirs();
            return RocksDB.open(options, dir.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + this.name + " at location " + dir.toString(), e);
        }
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return this.name;
    }

    /**
     * Persistent.
     *
     * @return true, if successful
     */
    @Override
    public boolean persistent() {
        return true;
    }

    /**
     * Checks if is open.
     *
     * @return true, if is open
     */
    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Gets the position.
     *
     * @return the position
     */
    @Override
    public Position getPosition() {
        return position;
    }

    /**
     * Gets the.
     *
     * @param key the key
     * @return the v
     */
    @Override
    public synchronized V get(K key) {
        validateStoreOpen();
        byte[] byteValue = getInternal(serdes.rawKey(key));
        if (byteValue == null) {
            return null;
        } else {
            return serdes.valueFrom(byteValue);
        }
    }

    /**
     * Validate store open.
     */
    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + this.name + " is currently closed");
        }
    }

    /**
     * Gets the internal.
     *
     * @param rawKey the raw key
     * @return the internal
     */
    private byte[] getInternal(byte[] rawKey) {
        try {
            return this.db.get(rawKey);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while getting value for key " + serdes.keyFrom(rawKey) 
                + FROM_STORE + this.name, e);
        }
    }

    /**
     * Put.
     *
     * @param key the key
     * @param value the value
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized void put(K key, V value) {
        validateStoreOpen();
        byte[] rawKey = serdes.rawKey(key);
        byte[] rawValue = serdes.rawValue(value);
        putInternal(rawKey, rawValue);
        StoreQueryUtils.updatePosition(position, context);
    }

    /**
     * Put if absent.
     *
     * @param key the key
     * @param value the value
     * @return the v
     */
    @Override
    public synchronized V putIfAbsent(K key, V value) {
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    /**
     * Put internal.
     *
     * @param rawKey the raw key
     * @param rawValue the raw value
     */
    private void putInternal(byte[] rawKey, byte[] rawValue) {
        if (rawValue == null) {
            try {
                db.delete(writeOptions, rawKey);
            } catch (RocksDBException e) {
                LOG.error("Error while removing key "
                        + serdes.keyFrom(rawKey) + " from store " + this.name, e);
                throw new ProcessorStateException("Error while removing key " + serdes.keyFrom(rawKey) 
                + FROM_STORE + this.name, e);
            }
        } else {
            try {
                db.put(writeOptions, rawKey, rawValue);
            } catch (RocksDBException e) {
                LOG.error("Error while executing put key " + serdes.keyFrom(rawKey)
                        + " and value " + serdes.keyFrom(rawValue) + " from store " + this.name, e);
                throw new ProcessorStateException("Error while executing put key " + serdes.keyFrom(rawKey) 
                + " and value " + serdes.keyFrom(rawValue) + FROM_STORE + this.name, e);
            }
        }
    }

    /**
     * Put all.
     *
     * @param entries the entries
     */
    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        try (WriteBatch batch = new WriteBatch()) {
            for (KeyValue<K, V> entry : entries) {
                final byte[] rawKey = serdes.rawKey(entry.key);
                if (entry.value == null) {
                    db.delete(rawKey);
                } else {
                    final byte[] value = serdes.rawValue(entry.value);
                    batch.put(rawKey, value);
                }
            }
            db.write(writeOptions, batch);
            StoreQueryUtils.updatePosition(position, context);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + this.name, e);
        }
    }

    /**
     * Query.
     *
     * @param <R> the generic type
     * @param query the query
     * @param positionBound the position bound
     * @param config the config
     * @return the query result
     */
    @Override
    public <R> QueryResult<R> query(
            final Query<R> query,
            final PositionBound positionBound,
            final QueryConfig config) {

        return StoreQueryUtils.handleBasicQueries(
                query,
                positionBound,
                config,
                this,
                position,
                context
        );
    }

    /**
     * Delete.
     *
     * @param key the key
     * @return the v
     */
    @Override
    public synchronized V delete(K key) {
        V value = get(key);
        put(key, null);
        return value;
    }

    /**
     * Range.
     *
     * @param from the from
     * @param to the to
     * @return the key value iterator
     */
    @Override
    public synchronized KeyValueIterator<K, V> range(K from, K to) {
        validateStoreOpen();
        // query rocksdb
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(db.newIterator(), serdes, from, to);
        openIterators.add(rocksDBRangeIterator);
        return rocksDBRangeIterator;
    }

    /**
     * All.
     *
     * @return the key value iterator
     */
    @Override
    public synchronized KeyValueIterator<K, V> all() {
        validateStoreOpen();
        // query rocksdb
        RocksIterator innerIter = db.newIterator();
        innerIter.seekToFirst();
        final RocksDbIterator rocksDbIterator = new RocksDbIterator(innerIter, serdes);
        openIterators.add(rocksDbIterator);
        return rocksDbIterator;
    }

    /**
     * Write.
     *
     * @param batch the batch
     * @throws RocksDBException the rocks DB exception
     */
    @Override
    public void write(final WriteBatch batch) throws RocksDBException {
        db.write(writeOptions, batch);
    }

    /**
     * Return an approximate count of key-value mappings in this store.
     *
     * <code>RocksDB</code> cannot return an exact entry count without doing a
     * full scan, so this method relies on the
     * <code>rocksdb.estimate-num-keys</code> property to get an approximate
     * count. The returned size also includes a count of dirty keys in the
     * store's in-memory cache, which may lead to some double-counting
     * of entries and inflate the estimate.
     *
     * @return an approximate count of key-value mappings in the store.
     */
    @Override
    public long approximateNumEntries() {
        long value;
        try {
            value = this.db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error fetching property from store " + this.name, e);
        }
        if (isOverflowing(value)) {
            return Long.MAX_VALUE;
        }
        return value;
    }

    /**
     * Checks if is overflowing.
     *
     * @param value the value
     * @return true, if is overflowing
     */
    private boolean isOverflowing(long value) {
        // RocksDB returns an unsigned 8-byte integer, which could overflow long
        // and manifest as a negative value.
        return value < 0;
    }

    /**
     * Flush.
     */
    @Override
    public synchronized void flush() {
        if (db == null) {
            return;
        }
        // flush RocksDB
        flushInternal();
    }

    /**
     * if flushing failed because of any internal store exceptions.
     *
     * @throws ProcessorStateException ProcessorStateException
     */
    private void flushInternal() {
        try {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + this.name, e);
        }
    }

    /**
     * Close.
     */
    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }
        open = false;
        closeOpenIterators();

        if (this.rocksDBEnabled) {
            metricsRecorder.removeValueProviders(name);
        }

        // Important: do not rearrange the order in which the below objects are closed!
        db.close();
        options.close();
        writeOptions.close();
        flushOptions.close();
        cache.close();

        options = null;
        writeOptions = null;
        flushOptions = null;
        db = null;
        cache = null;
    }

    /**
     * Close open iterators.
     */
    private void closeOpenIterators() {
        for (KeyValueIterator<?, ?> iterator : new HashSet<>(openIterators)) {
            iterator.close();
        }
        openIterators.clear();
    }

    /**
     * Maybe set up statistics.
     *
     * @param configs the configs
     */
    private void maybeSetUpStatistics(final Map<String, Object> configs) {
        if (options.statistics() != null) {
            userSpecifiedStatistics = true;
        }
        if (!userSpecifiedStatistics && RecordingLevel.forName(
                (String) configs.get(METRICS_RECORDING_LEVEL_CONFIG)) == RecordingLevel.DEBUG) {

            // metrics recorder will clean up statistics object
            final Statistics statistics = new Statistics();
            options.setStatistics(statistics);
        }
    }

    /**
     * prepareBatchForRestore().
     *
     * @param records records
     * @param batch batch
     * @throws RocksDBException RocksDBException
     */
    public void prepareBatchForRestore(final Collection<KeyValue<byte[], byte[]>> records,
            final WriteBatch batch) throws RocksDBException {
        for (final KeyValue<byte[], byte[]> record : records) {
            addToBatch(record.key, record.value, batch);
        }
    }

    /**
     * addToBatch().
     *
     * @param key key
     * @param value value
     * @param batch batch
     * @throws RocksDBException RocksDBException
     */
    public void addToBatch(final byte[] key,
            final byte[] value,
            final WriteBatch batch) throws RocksDBException {
        if (value == null) {
            batch.delete(key);
        } else {
            batch.put(key, value);
        }
    }

    /**
     * Adds the to batch.
     *
     * @param record the record
     * @param batch the batch
     * @throws RocksDBException the rocks DB exception
     */
    @Override
    public void addToBatch(KeyValue<byte[], byte[]> record, WriteBatch batch) throws RocksDBException {
        addToBatch(record.key, record.value, batch);
    }
    
    /**
     * Initialize HarmanRocksDBStore.
     *
     * @param context the context
     * @param root the root
     * @deprecated (Use StateStoreContext instead)
     */
    @Deprecated(since = "2.41.0-1")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        if (context instanceof StateStoreContext storeContext) {
            init(storeContext, root);
        } else {
            throw new UnsupportedOperationException(
                "Use RocksDBStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }

    /**
     * Inits the.
     *
     * @param context the context
     * @param root the root
     */
    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        String rocksDBMetricsEnabled = HarmanRocksDBStore.prop.getProperty(PropertyNames.ROCKSDB_METRICS_ENABLED);
        LOG.info("RocksDB metrics enabled set to : {}", rocksDBMetricsEnabled);
        this.rocksDBEnabled = !StringUtils.isEmpty(rocksDBMetricsEnabled)
                && rocksDBMetricsEnabled.equalsIgnoreCase(Constants.TRUE);
        if (this.rocksDBEnabled) {
            metricsRecorder.init(getMetricsImpl(context), context.taskId());
        }
        
        openDB(context);
        
        final File positionCheckpointFile = new File(context.stateDir(), FilenameUtils.getName(name() 
                + POSITION_FILE_SUFFIX));
        try {
            LOG.debug("Attempting to create position checkpoint file : {}", positionCheckpointFile.getCanonicalPath());
            if (!positionCheckpointFile.getCanonicalPath().startsWith(context.stateDir().getCanonicalPath())) {
                throw new IOException("Could not open file : " + name() + POSITION_FILE_SUFFIX 
                        + " Expected base path does not match the input base path. Possible file traversal attack!");
            }
        } catch (IOException e) {
            String errMsg = "Error creating position checkpoint file " + name()
                + POSITION_FILE_SUFFIX + " at location " + positionCheckpointFile.getAbsolutePath();
            throw new ProcessorStateException(errMsg, e);
        }
        OffsetCheckpoint positionCheckpoint;
        positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);
        
        context.register(
                root,
                (RecordBatchingStateRestoreCallback) this::restoreBatch,
                () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );
        
        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                context.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false);
            
        this.context = context;
    }

    /**
     * The Class RocksDbIterator.
     */
    class RocksDbIterator implements KeyValueIterator<K, V> {
        
        /** The iter. */
        private final RocksIterator iter;
        
        /** The serdes. */
        private final StateSerdes<K, V> serdes;
        
        /** The open. */
        private boolean open = true;

        /**
         * Instantiates a new rocks db iterator.
         *
         * @param iter the iter
         * @param serdes the serdes
         */
        RocksDbIterator(RocksIterator iter, StateSerdes<K, V> serdes) {
            this.iter = iter;
            this.serdes = serdes;
        }

        /**
         * Peek raw key.
         *
         * @return the byte[]
         */
        byte[] peekRawKey() {
            return iter.key();
        }

        /**
         * Gets the key value.
         *
         * @return the key value
         */
        private KeyValue<K, V> getKeyValue() {
            return new KeyValue<>(serdes.keyFrom(iter.key()), serdes.valueFrom(iter.value()));
        }

        /**
         * Checks for next.
         *
         * @return true, if successful
         */
        @Override
        public synchronized boolean hasNext() {
            if (!open) {
                throw new InvalidStateStoreException("store %s has closed");
            }
            return iter.isValid();
        }

        /**
         * KeyValue().
         *
         * @return the key value
         * @throws NoSuchElementException if no next element exist
         */
        @Override
        public synchronized KeyValue<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            KeyValue<K, V> entry = this.getKeyValue();
            iter.next();
            return entry;
        }

        /**
         * remove().
         *
         * @throws UnsupportedOperationException UnsupportedOperationException
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException("RocksDB iterator does not support remove");
        }

        /**
         * Close.
         */
        @Override
        public synchronized void close() {
            open = false;
            openIterators.remove(this);
            iter.close();
        }

        /**
         * Peek next key.
         *
         * @return the k
         */
        @Override
        public K peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return serdes.keyFrom(iter.key());

        }

    }

    /**
     * The Class RocksDBRangeIterator.
     */
    private class RocksDBRangeIterator extends RocksDbIterator {
        // RocksDB's JNI interface does not expose getters/setters that allow
        // the
        // comparator to be pluggable, and the default is lexicographic, so it's
        /** The comparator. */
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        
        /** The raw to key. */
        private byte[] rawToKey;

        /**
         * Instantiates a new rocks DB range iterator.
         *
         * @param iter the iter
         * @param serdes the serdes
         * @param from the from
         * @param to the to
         */
        RocksDBRangeIterator(RocksIterator iter, StateSerdes<K, V> serdes, K from, K to) {
            super(iter, serdes);
            iter.seek(serdes.rawKey(from));
            this.rawToKey = serdes.rawKey(to);
        }

        /**
         * Checks for next.
         *
         * @return true, if successful
         */
        @Override
        public synchronized boolean hasNext() {
            return super.hasNext() && comparator.compare(super.peekRawKey(), this.rawToKey) <= 0;
        }
    }

    /**
     * Adds the value providers to metrics recorder.
     */
    private void addValueProvidersToMetricsRecorder() {
        final TableFormatConfig tableFormatConfig = options.tableFormatConfig();
        final Statistics statistics = userSpecifiedStatistics ? null : options.statistics();
        if (tableFormatConfig instanceof BlockBasedTableConfigWithAccessibleCache accessibleCache) {
            final Cache blockCache = accessibleCache.blockCache();
            metricsRecorder.addValueProviders(name, db, blockCache, statistics);
        } else if (tableFormatConfig instanceof BlockBasedTableConfig) {
            throw new ProcessorStateException("The used block-based table format configuration does not expose the " 
        + "block cache. Use the BlockBasedTableConfig instance provided by Options#tableFormatConfig() to configure " 
                    + "the block-based table format of RocksDB. Do not provide a new instance "
                    + "of BlockBasedTableConfig to " + "the RocksDB options.");
        } else {
            metricsRecorder.addValueProviders(name, db, null, statistics);
        }
    }
    
    /**
     * Restore batch.
     *
     * @param records the records
     */
    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        try (final WriteBatch batch = new WriteBatch()) {
            final List<KeyValue<byte[], byte[]>> keyValues = new ArrayList<>();
            for (final ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                    consumerRecord,
                    consistencyEnabled,
                    position
                );
                // If version headers are not present or version is V0
                keyValues.add(new KeyValue<>(consumerRecord.key(), consumerRecord.value()));
            }
            prepareBatchForRestore(keyValues, batch);
            write(batch);
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + name, e);
        }
    }
}
