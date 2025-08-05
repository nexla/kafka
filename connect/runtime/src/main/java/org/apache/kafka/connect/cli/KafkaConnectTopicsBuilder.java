package org.apache.kafka.connect.cli;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.storage.*;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.SharedTopicAdmin;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;


public class KafkaConnectTopicsBuilder {

    public final boolean memoryKafkaConnectConfigTopics;
    public final boolean memoryKafkaConnectStatusTopics;
    public final boolean memoryKafkaConnectOffsetTopics;
    public final boolean deletionKafkaConnectOffsetTopics;

    public KafkaConnectTopicsBuilder() {
        this.memoryKafkaConnectConfigTopics = checkEnvBoolean("MEMORY_KAFKA_CONNECT_CONFIG_TOPICS");
        this.memoryKafkaConnectStatusTopics = checkEnvBoolean("MEMORY_KAFKA_CONNECT_STATUS_TOPICS");
        this.memoryKafkaConnectOffsetTopics = checkEnvBoolean("MEMORY_KAFKA_CONNECT_OFFSET_TOPICS");
        this.deletionKafkaConnectOffsetTopics = checkEnvBoolean("DELETION_KAFKA_CONNECT_OFFSET_TOPICS");
    }

    private static boolean checkEnvBoolean(String disableKafkaConnectConfigTopics) {
        return Optional.ofNullable(System.getenv(disableKafkaConnectConfigTopics))
                .map(Boolean::valueOf)
                .orElse(false);
    }

    public DistributedHerder buildDistributedHerder(Plugins plugins,
                                                    DistributedConfig config,
                                                    String kafkaClusterId,
                                                    URI advertisedUrl,
                                                    String workerId,
                                                    SharedTopicAdmin sharedAdmin,
                                                    Time time,
                                                    RestClient restClient) {

        final Converter keyConverter = plugins.newInternalConverter(true, JsonConverter.class.getName(),
                Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"));

        OffsetBackingStore offsetBackingStore = getOffsetBackingStore(sharedAdmin, config, keyConverter);
        offsetBackingStore.configure(config);
        final String clientIdBase = ConnectUtils.clientIdBase(config);

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        Worker worker = new Worker(workerId, time, plugins, config, offsetBackingStore, connectorClientConfigOverridePolicy);
        WorkerConfigTransformer configTransformer = worker.configTransformer();

        Converter internalValueConverter = worker.getInternalValueConverter();

        StatusBackingStore statusBackingStore = memoryKafkaConnectStatusTopics ?
                new MemoryStatusBackingStore() :
                new KafkaStatusBackingStore(time, internalValueConverter, sharedAdmin, clientIdBase);
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore = memoryKafkaConnectConfigTopics ?
                new MemoryConfigBackingStore(configTransformer) :
                new KafkaConfigBackingStore(internalValueConverter, config, configTransformer, sharedAdmin, clientIdBase);

        // Pass the shared admin to the distributed herder as an additional AutoCloseable object that should be closed when the
        // herder is stopped. This is easier than having to track and own the lifecycle ourselves.
        return new DistributedHerder(config, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl.toString(), restClient, connectorClientConfigOverridePolicy, new ArrayList<>());
    }

    private OffsetBackingStore getOffsetBackingStore(SharedTopicAdmin sharedAdmin, WorkerConfig config, Converter keyConverter) {
        if (memoryKafkaConnectOffsetTopics) {
            return new MemoryOffsetBackingStoreImpl(keyConverter);
        }
        if (deletionKafkaConnectOffsetTopics) {
            return new DeletionKafkaOffsetBackingStore(sharedAdmin, keyConverter);
        }
        return new KafkaOffsetBackingStore(sharedAdmin, () -> ConnectUtils.clientIdBase(config), keyConverter);
    }
}