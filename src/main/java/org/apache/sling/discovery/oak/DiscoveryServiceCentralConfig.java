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

import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.AttributeDefinition;

@SuppressWarnings("java:S100")
@ObjectClassDefinition(
        localization = "OSGI-INF/l10n/metatype",
        name = "%config.name",
        description = "%config.description")
public @interface DiscoveryServiceCentralConfig {

    long DEFAULT_TOPOLOGY_CONNECTOR_TIMEOUT = 120;
    long DEFAULT_TOPOLOGY_CONNECTOR_INTERVAL = 30;
    long DEFAULT_DISCOVERY_LITE_CHECK_INTERVAL = 2;
    long DEFAULT_CLUSTER_SYNC_SERVICE_TIMEOUT = 120;
    long DEFAULT_CLUSTER_SYNC_SERVICE_INTERVAL = 2;
    int DEFAULT_MIN_EVENT_DELAY = 3;
    int DEFAULT_SOCKET_CONNECT_TIMEOUT = 10;
    int DEFAULT_SO_TIMEOUT = 10;
    int DEFAULT_BACKOFF_STANDBY_FACTOR = 5;
    int DEFAULT_BACKOFF_STABLE_FACTOR = 5;
    long DEFAULT_LEADER_ELECTION_PREFIX = 1;
    String[] DEFAULT_TOPOLOGY_CONNECTOR_WHITELIST = {"localhost", "127.0.0.1"};
    String DEFAULT_DISCOVERY_RESOURCE_PATH = "/var/discovery/oak/";

    boolean DEFAULT_INVERT_LEADER_ELECTION_PREFIX_ORDER = false;
    boolean DEFAULT_SUPPRESS_PARTIALLY_STARTED_INSTANCES = false;
    String JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME = "org.apache.sling.discovery.oak.joinerdelay.enabled";
    long DEFAULT_SUPPRESSION_TIMEOUT_SECONDS = -1;
    long DEFAULT_JOINER_DELAY_SECONDS = 0;
    /**
     * The default lifetime of a HMAC shared key in ms. (4h)
     */
    long DEFAULT_SHARED_KEY_INTERVAL = 3600 * 1000 * 4;

    /**
     * Configure the timeout (in seconds) after which an instance is considered dead/crashed.
     */
    @AttributeDefinition(name = "%connectorPingTimeout.name", description = "%connectorPingTimeout.description")
    long connectorPingTimeout() default DEFAULT_TOPOLOGY_CONNECTOR_TIMEOUT;

    /**
     * Configure the interval (in seconds) according to which the heartbeats are exchanged in the topology.
     */
    @AttributeDefinition(name = "%connectorPingInterval.name", description = "%connectorPingInterval.description")
    long connectorPingInterval() default DEFAULT_TOPOLOGY_CONNECTOR_INTERVAL;

    @AttributeDefinition(name = "%discoveryLiteCheckInterval.name", description = "%discoveryLiteCheckInterval.description")
    long discoveryLiteCheckInterval() default DEFAULT_DISCOVERY_LITE_CHECK_INTERVAL;

    @AttributeDefinition(name = "%clusterSyncServiceTimeout.name", description = "%clusterSyncServiceTimeout.description")
    long clusterSyncServiceTimeout() default DEFAULT_CLUSTER_SYNC_SERVICE_TIMEOUT;

    @AttributeDefinition(name = "%clusterSyncServiceInterval.name", description = "%clusterSyncServiceInterval.description")
    long clusterSyncServiceInterval() default DEFAULT_CLUSTER_SYNC_SERVICE_INTERVAL;

    /**
     * If set to true a syncToken will be used on top of waiting for deactivating instances to be fully processed.
     * If set to false, only deactivating instances will be waited for to be fully processed.
     */
    @AttributeDefinition(name = "%enableSyncToken.name", description = "%enableSyncToken.description")
    boolean enableSyncToken() default true;

    /**
     * Configure the time (in seconds) which must be passed at minimum between sending TOPOLOGY_CHANGING/_CHANGED (avoid flooding).
     */
    @AttributeDefinition(name = "%minEventDelay.name", description = "%minEventDelay.description")
    int minEventDelay() default DEFAULT_MIN_EVENT_DELAY;

    /**
     * Configure the socket connect timeout for topology connectors.
     */
    @AttributeDefinition(name = "%socketConnectTimeout.name", description = "%socketConnectTimeout.description")
    int socketConnectTimeout() default DEFAULT_SOCKET_CONNECT_TIMEOUT;

    /**
     * Configure the socket read timeout (SO_TIMEOUT) for topology connectors.
     */
    @AttributeDefinition(name = "%soTimeout.name", description = "%soTimeout.description")
    int soTimeout() default DEFAULT_SO_TIMEOUT;

    /**
     * URLs where to join a topology, eg http://localhost:4502/libs/sling/topology/connector
     */
    @AttributeDefinition (cardinality = 1024)
    String[] topologyConnectorUrls() default {};

    /**
     * list of ips and/or hostnames which are allowed to connect to /libs/sling/topology/connector
     */
    @AttributeDefinition (cardinality = 1024)
    String[] topologyConnectorWhitelist() default {"localhost", "127.0.0.1"};

    /**
     * Path of resource where to keep discovery information, e.g /var/discovery/oak/
     */
    @AttributeDefinition
    String discoveryResourcePath() default DEFAULT_DISCOVERY_RESOURCE_PATH;

    /**
     * If set to true, local-loops of topology connectors are automatically stopped when detected so.
     */
    @AttributeDefinition
    boolean autoStopLocalLoopEnabled() default false;

    /**
     * If set to true, request body will be gzipped - only works if counter-part accepts gzip-requests!
     */
    @AttributeDefinition
    boolean gzipConnectorRequestsEnabled() default false;

    /**
     * If set to true, hmac is enabled and the white list is disabled.
     */
    @AttributeDefinition(name = "%hmacEnabled.name", description = "%hmacEnabled.description")
    boolean hmacEnabled() default false;

    /**
     * If set to true, and the whitelist is disabled, messages will be encrypted.
     */
    @AttributeDefinition(name = "%enableEncryption.name", description = "%enableEncryption.description")
    boolean enableEncryption() default false;

    /**
     * The value fo the shared key, shared amongst all instances in the same cluster.
     */
    @AttributeDefinition(name = "%sharedKey.name", description = "%sharedKey.description")
    String sharedKey() default "";

    @AttributeDefinition(name = "%hmacSharedKeyTTL.name", description = "%hmacSharedKeyTTL.description")
    long hmacSharedKeyTTL() default DEFAULT_SHARED_KEY_INTERVAL;

    /**
     * The property for defining the backoff factor for standby (loop) connectors
     */
    @AttributeDefinition(name = "%backoffStandbyFactor.name", description = "%backoffStandbyFactor.description")
    int backoffStandbyFactor() default DEFAULT_BACKOFF_STANDBY_FACTOR;

    /**
     * The property for defining the maximum backoff factor for stable connectors
     */
    @AttributeDefinition(name = "%backoffStableFactor.name", description = "%backoffStableFactor.description")
    int backoffStableFactor() default DEFAULT_BACKOFF_STABLE_FACTOR;

    @AttributeDefinition
    long leaderElectionPrefix() default DEFAULT_LEADER_ELECTION_PREFIX;

    @AttributeDefinition
    boolean invertLeaderElectionPrefixOrder() default DEFAULT_INVERT_LEADER_ELECTION_PREFIX_ORDER;

    @AttributeDefinition
    boolean suppressPartiallyStartedInstance() default DEFAULT_SUPPRESS_PARTIALLY_STARTED_INSTANCES;

    @AttributeDefinition
    long suppressionTimeoutSeconds() default DEFAULT_SUPPRESSION_TIMEOUT_SECONDS;

    @AttributeDefinition
    long joinerDelaySeconds() default DEFAULT_JOINER_DELAY_SECONDS;
}
