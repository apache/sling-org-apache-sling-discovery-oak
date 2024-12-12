/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.discovery.oak;

import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Dictionary;
import java.util.Hashtable;

import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_BACKOFF_STANDBY_FACTOR;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_CLUSTER_SYNC_SERVICE_INTERVAL;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_CLUSTER_SYNC_SERVICE_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_DISCOVERY_LITE_CHECK_INTERVAL;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_TOPOLOGY_CONNECTOR_INTERVAL;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_TOPOLOGY_CONNECTOR_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_BACKOFF_STABLE_FACTOR;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_MIN_EVENT_DELAY;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SOCKET_CONNECT_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SO_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SUPPRESSION_TIMEOUT_SECONDS;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_JOINER_DELAY_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConfigTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private Config config;

    @Before
    public void setUp() {
        Dictionary<String, Object> properties = new Hashtable<>();

        config = context.registerInjectActivateService(new Config(), properties);
    }

    @Test
    public void testIfServiceActive() {
        assertNotNull(config);
    }

    @Test
    public void testEmptyValues() {
        Dictionary<String, Object> properties = new Hashtable<>();
        properties.put("backoffStandbyFactor", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_BACKOFF_STANDBY_FACTOR, config.getBackoffStandbyFactor());
        properties = new Hashtable<>();
        properties.put("backoffStandbyFactor", "4");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(4, config.getBackoffStandbyFactor());
        properties = new Hashtable<>();
        properties.put("backoffStableFactor", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_BACKOFF_STABLE_FACTOR, config.getBackoffStandbyFactor());

        properties.put("connectorPingTimeout", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_TOPOLOGY_CONNECTOR_TIMEOUT, config.getConnectorPingTimeout());
        properties.put("connectorPingTimeout", "2");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(2, config.getConnectorPingTimeout());

        properties.put("connectorPingInterval", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_TOPOLOGY_CONNECTOR_INTERVAL, config.getConnectorPingInterval());
        properties.put("connectorPingInterval", "6");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(6, config.getConnectorPingInterval());

        properties.put("discoveryLiteCheckInterval", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_DISCOVERY_LITE_CHECK_INTERVAL, config.getDiscoveryLiteCheckInterval());
        properties.put("discoveryLiteCheckInterval", "11");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(11, config.getDiscoveryLiteCheckInterval());

        properties.put("clusterSyncServiceTimeout", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_CLUSTER_SYNC_SERVICE_TIMEOUT * 1000, config.getClusterSyncServiceTimeoutMillis());
        properties.put("clusterSyncServiceTimeout", "12");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(12 * 1000, config.getClusterSyncServiceTimeoutMillis());

        properties.put("clusterSyncServiceInterval", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_CLUSTER_SYNC_SERVICE_INTERVAL * 1000, config.getClusterSyncServiceIntervalMillis());
        properties.put("clusterSyncServiceInterval", "13");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(13 * 1000, config.getClusterSyncServiceIntervalMillis());

        properties.put("minEventDelay", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_MIN_EVENT_DELAY, config.getMinEventDelay());
        properties.put("minEventDelay", "14");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(14, config.getMinEventDelay());

        properties.put("socketConnectTimeout", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_SOCKET_CONNECT_TIMEOUT, config.getSocketConnectTimeout());
        properties.put("socketConnectTimeout", "14");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(14, config.getSocketConnectTimeout());

        properties.put("soTimeout", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_SO_TIMEOUT, config.getSoTimeout());
        properties.put("soTimeout", "15");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(15, config.getSoTimeout());

        properties.put("suppressionTimeoutSeconds", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_SUPPRESSION_TIMEOUT_SECONDS, config.getSuppressionTimeoutSeconds());
        properties.put("suppressionTimeoutSeconds", "16");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(16, config.getSuppressionTimeoutSeconds());

        properties.put("joinerDelaySeconds", "");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(DEFAULT_JOINER_DELAY_SECONDS * 1000, config.getJoinerDelayMillis());
        properties.put("joinerDelaySeconds", "17");
        config = context.registerInjectActivateService(new Config(), properties);
        assertEquals(17 * 1000, config.getJoinerDelayMillis());
    }
}