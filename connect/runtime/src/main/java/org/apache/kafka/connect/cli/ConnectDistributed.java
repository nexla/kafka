/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.cli;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

/**
 * <p>
 * Command line utility that runs Kafka Connect in distributed mode. In this mode, the process joins a group of other
 * workers and work (connectors and tasks) is distributed among them. This is useful for running Connect as a service,
 * where connectors can be submitted to the cluster to be automatically executed in a scalable, distributed fashion.
 * This also allows you to easily scale out horizontally, elastically adding or removing capacity simply by starting or
 * stopping worker instances.
 * </p>
 */
public class ConnectDistributed extends AbstractConnectCli<DistributedHerder, DistributedConfig> {
    private static final Logger log = LoggerFactory.getLogger(ConnectDistributed.class);
    private static final Time time = Time.SYSTEM;
    public ConnectDistributed(String... args) {
        super(args);
    }

    @Override
    protected String usage() {
        return "ConnectDistributed worker.properties";
    }

    @Override
    protected DistributedHerder createHerder(DistributedConfig config, String workerId, Plugins plugins,
                                  ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                  RestServer restServer, RestClient restClient) {
        log.info("Initializing Worker id {}", workerId);
        String kafkaClusterId = config.kafkaClusterId();
        String clientIdBase = ConnectUtils.clientIdBase(config);
        Map<String, Object> adminProps = new HashMap<>(config.originals());
        ConnectUtils.addMetricsContextProperties(adminProps, config, kafkaClusterId);
        adminProps.put(CLIENT_ID_CONFIG, clientIdBase + "shared-admin");
        SharedTopicAdmin sharedAdmin = new SharedTopicAdmin(adminProps);
        final DistributedHerder herder = new KafkaConnectTopicsBuilder()
                .buildDistributedHerder(plugins, config, kafkaClusterId, restServer.advertisedUrl(), workerId, sharedAdmin, time, restClient);
        log.info("Initialized Worker id {}", workerId);
        return herder;
    }

    @Override
    protected DistributedConfig createConfig(Map<String, String> workerProps) {
        return new DistributedConfig(workerProps);
    }

    public static void main(String[] args) {
        ConnectDistributed connectDistributed = new ConnectDistributed(args);
        connectDistributed.run();
    }
}
