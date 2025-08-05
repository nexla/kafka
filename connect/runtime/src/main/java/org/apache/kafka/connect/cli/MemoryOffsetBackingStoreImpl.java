package org.apache.kafka.connect.cli;

import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MemoryOffsetBackingStoreImpl extends MemoryOffsetBackingStore {
    private final Map<String, Set<Map<String, Object>>> connectorPartitions = new HashMap<>();
    private final Converter keyConverter;

    public MemoryOffsetBackingStoreImpl(Converter keyConverter) {
        this.keyConverter = keyConverter;
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return connectorPartitions.getOrDefault(connectorName, Collections.emptySet());
    }

    @Override
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
            byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
            OffsetUtils.processPartitionKey(key, value, keyConverter, connectorPartitions);
        }
    }

}
