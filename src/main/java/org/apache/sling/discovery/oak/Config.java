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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.sling.discovery.base.connectors.BaseConfig;
import org.apache.sling.discovery.commons.providers.spi.base.DiscoveryLiteConfig;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentException;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_BACKOFF_STABLE_FACTOR;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_BACKOFF_STANDBY_FACTOR;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_CLUSTER_SYNC_SERVICE_INTERVAL;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_CLUSTER_SYNC_SERVICE_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_DISCOVERY_LITE_CHECK_INTERVAL;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_DISCOVERY_RESOURCE_PATH;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_INVERT_LEADER_ELECTION_PREFIX_ORDER;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_JOINER_DELAY_SECONDS;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_LEADER_ELECTION_PREFIX;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_MIN_EVENT_DELAY;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SOCKET_CONNECT_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SO_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SUPPRESSION_TIMEOUT_SECONDS;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_SUPPRESS_PARTIALLY_STARTED_INSTANCES;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_TOPOLOGY_CONNECTOR_INTERVAL;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_TOPOLOGY_CONNECTOR_TIMEOUT;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.DEFAULT_TOPOLOGY_CONNECTOR_WHITELIST;
import static org.apache.sling.discovery.oak.DiscoveryServiceCentralConfig.JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME;

import static org.osgi.util.converter.Converters.standardConverter;
/**
 * Configuration object used as a central config point for the discovery service
 * implementation
 * <p>
 * The properties are described below under.
 */
@Component(service = {Config.class, BaseConfig.class, DiscoveryLiteConfig.class})
@Designate(ocd = DiscoveryServiceCentralConfig.class)
public class Config implements BaseConfig, DiscoveryLiteConfig {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * resource used to keep instance information such as last heartbeat, properties, incoming announcements
     **/
    private static final String CLUSTERINSTANCES_RESOURCE = "clusterInstances";

    /**
     * resource used to store the sync tokens as part of a topology change
     **/
    private static final String SYNC_TOKEN_RESOURCE = "syncTokens";

    /**
     * resource used to store the clusterNodeIds to slingIds map
     **/
    private static final String ID_MAP_RESOURCE = "idMap";

    /**
     * True when auto-stop of a local-loop is enabled. Default is false.
     **/
    private boolean autoStopLocalLoopEnabled;

    /**
     * True when the hmac is enabled and signing is disabled.
     */
    private boolean hmacEnabled;

    /**
     * the shared key.
     */
    private String sharedKey;

    /**
     * The key interval.
     */
    private long keyInterval;

    /**
     * true when encryption is enabled.
     */
    private boolean encryptionEnabled;

    /**
     * true when topology connector requests should be gzipped
     */
    private boolean gzipConnectorRequestsEnabled;

    /**
     * the backoff factor to be used for standby (loop) connectors
     **/
    private int backoffStandbyFactor = DEFAULT_BACKOFF_STANDBY_FACTOR;

    /**
     * the maximum backoff factor to be used for stable connectors
     **/
    private int backoffStableFactor = DEFAULT_BACKOFF_STABLE_FACTOR;

    /**
     * Whether, on top of waiting for deactivating instances,
     * a syncToken should also be used
     */
    private boolean syncTokenEnabled;

    /**
     * only check system property JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME every 5 minutes, here's to the next check
     */
    private long joinerDelayOverwriteNextCheck;

    /**
     * cache of last read of system property JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME
     **/
    private boolean joinerDelayOverwrite;

    protected long connectorPingTimeout = DEFAULT_TOPOLOGY_CONNECTOR_TIMEOUT;
    protected long connectorPingInterval = DEFAULT_TOPOLOGY_CONNECTOR_INTERVAL;
    protected long discoveryLiteCheckInterval = DEFAULT_DISCOVERY_LITE_CHECK_INTERVAL;
    protected long clusterSyncServiceTimeout = DEFAULT_CLUSTER_SYNC_SERVICE_TIMEOUT;
    protected long clusterSyncServiceInterval = DEFAULT_CLUSTER_SYNC_SERVICE_INTERVAL;
    protected int minEventDelay = DEFAULT_MIN_EVENT_DELAY;
    private int socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;
    private int soTimeout = DEFAULT_SO_TIMEOUT;
    private URL[] topologyConnectorUrls = {null};
    protected String[] topologyConnectorWhitelist = DEFAULT_TOPOLOGY_CONNECTOR_WHITELIST;
    protected String discoveryResourcePath = DEFAULT_DISCOVERY_RESOURCE_PATH;

    protected long leaderElectionPrefix = DEFAULT_LEADER_ELECTION_PREFIX;
    protected boolean invertLeaderElectionPrefixOrder = DEFAULT_INVERT_LEADER_ELECTION_PREFIX_ORDER;
    protected boolean suppressPartiallyStartedInstance = DEFAULT_SUPPRESS_PARTIALLY_STARTED_INSTANCES;
    protected long suppressionTimeoutSeconds = DEFAULT_SUPPRESSION_TIMEOUT_SECONDS;
    protected long joinerDelaySeconds = DEFAULT_JOINER_DELAY_SECONDS;

    @Activate
    protected void activate(BundleContext context, DiscoveryServiceCentralConfig config) {
        logger.debug("activate: config activated.");
        configure(config);
    }

    protected void configure(final DiscoveryServiceCentralConfig config) {
        this.connectorPingTimeout = config.connectorPingTimeout();
        logger.debug("configure: connectorPingTimeout='{}'", this.connectorPingTimeout);

        this.connectorPingInterval = config.connectorPingInterval();
        logger.debug("configure: connectorPingInterval='{}'", this.connectorPingInterval);

        this.discoveryLiteCheckInterval = config.discoveryLiteCheckInterval();
        logger.debug("configure: discoveryLiteCheckInterval='{}'", this.discoveryLiteCheckInterval);

        this.clusterSyncServiceTimeout = config.clusterSyncServiceTimeout();
        logger.debug("configure: clusterSyncServiceTimeout='{}'", this.clusterSyncServiceTimeout);

        this.clusterSyncServiceInterval = config.clusterSyncServiceInterval();
        logger.debug("configure: clusterSyncServiceInterval='{}'", this.clusterSyncServiceInterval);

        this.minEventDelay = config.minEventDelay();
        logger.debug("configure: minEventDelay='{}'", this.minEventDelay);

        this.socketConnectTimeout = config.socketConnectTimeout();
        logger.debug("configure: socketConnectTimeout='{}'", this.socketConnectTimeout);

        this.soTimeout = config.soTimeout();
        logger.debug("configure: soTimeout='{}'", this.soTimeout);

        String[] topologyConnectorUrlsStr = config.topologyConnectorUrls();
        if (topologyConnectorUrlsStr != null && topologyConnectorUrlsStr.length > 0) {
            List<URL> urls = new LinkedList<>();
            for (int i = 0; i < topologyConnectorUrlsStr.length; i++) {
                String anUrlStr = topologyConnectorUrlsStr[i];
                try {
                    if (anUrlStr != null && anUrlStr.length() > 0) {
                        URL url = new URL(anUrlStr);
                        logger.debug("configure: a topologyConnectorbUrl='{}'",
                                url);
                        urls.add(url);
                    }
                } catch (MalformedURLException e) {
                    logger.error("configure: could not set a topologyConnectorUrl: " + e,
                            e);
                }
            }
            if (urls.size() > 0) {
                this.topologyConnectorUrls = urls.toArray(new URL[urls.size()]);
                logger.debug("configure: number of topologyConnectorUrls='{}''",
                        urls.size());
            } else {
                this.topologyConnectorUrls = null;
                logger.debug("configure: no (valid) topologyConnectorUrls configured");
            }
        } else {
            this.topologyConnectorUrls = null;
            logger.debug("configure: no (valid) topologyConnectorUrls configured");
        }
        this.topologyConnectorWhitelist = config.topologyConnectorWhitelist();
        logger.debug("configure: topologyConnectorWhitelist='{}'", this.topologyConnectorWhitelist);

        this.discoveryResourcePath = config.discoveryResourcePath();
        while (this.discoveryResourcePath.endsWith("/")) {
            this.discoveryResourcePath = this.discoveryResourcePath.substring(0, this.discoveryResourcePath.length() - 1);
        }
        this.discoveryResourcePath = this.discoveryResourcePath + "/";
        if (this.discoveryResourcePath == null || this.discoveryResourcePath.length() <= 1) {
            // if the path is empty, or /, then use the default
            this.discoveryResourcePath = DEFAULT_DISCOVERY_RESOURCE_PATH;
        }
        logger.debug("configure: discoveryResourcePath='{}'", this.discoveryResourcePath);

        autoStopLocalLoopEnabled = config.autoStopLocalLoopEnabled();
        gzipConnectorRequestsEnabled = config.gzipConnectorRequestsEnabled();
        hmacEnabled = config.hmacEnabled();
        encryptionEnabled = config.enableEncryption();
        syncTokenEnabled = config.enableSyncToken();
        sharedKey = config.sharedKey();
        keyInterval = config.hmacSharedKeyTTL();

        backoffStandbyFactor = guard((c) -> config.backoffStandbyFactor(), "backoffStandbyFactor", DEFAULT_BACKOFF_STANDBY_FACTOR);
        backoffStableFactor = guard((c) -> config.backoffStableFactor(), "backoffStableFactor", DEFAULT_BACKOFF_STABLE_FACTOR);

        this.invertLeaderElectionPrefixOrder = config.invertLeaderElectionPrefixOrder();
        logger.debug("configure: invertLeaderElectionPrefixOrder='{}'", this.invertLeaderElectionPrefixOrder);

        this.leaderElectionPrefix = config.leaderElectionPrefix();
        logger.debug("configure: leaderElectionPrefix='{}'",  this.leaderElectionPrefix);

        this.suppressPartiallyStartedInstance = config.suppressPartiallyStartedInstance();
        logger.debug("configure: suppressPartiallyStartedInstance='{}'", this.suppressPartiallyStartedInstance);

        this.suppressionTimeoutSeconds = config.suppressionTimeoutSeconds();
        logger.debug("configure: suppressionTimeoutSeconds='{}'", this.suppressionTimeoutSeconds);

        this.joinerDelaySeconds = config.joinerDelaySeconds();
        logger.debug("configure: joinerDelaySeconds='{}'", this.joinerDelaySeconds);
    }

    private Integer guard(Function<Void, Integer> injectedConfig, String key, int def) {
        try {
            return injectedConfig.apply(null);
        } catch(RuntimeException re) {
            logger.info("configure: got RuntimeException for " + key + ", using default: " + re);
            return def;
        }
    }

    /**
     * Returns the socket connect() timeout used by the topology connector, 0 disables the timeout
     *
     * @return the socket connect() timeout used by the topology connector, 0 disables the timeout
     */
    public int getSocketConnectTimeout() {
        return socketConnectTimeout;
    }

    /**
     * Returns the socket read timeout (SO_TIMEOUT) used by the topology connector, 0 disables the timeout
     *
     * @return the socket read timeout (SO_TIMEOUT) used by the topology connector, 0 disables the timeout
     */
    public int getSoTimeout() {
        return soTimeout;
    }

    /**
     * Returns the minimum time (in seconds) between sending TOPOLOGY_CHANGING/_CHANGED events - to avoid flooding
     *
     * @return the minimum time (in seconds) between sending TOPOLOGY_CHANGING/_CHANGED events - to avoid flooding
     */
    public int getMinEventDelay() {
        return minEventDelay;
    }

    /**
     * Returns the URLs to which to open a topology connector - or null/empty if no topology connector
     * is configured (default is null)
     *
     * @return the URLs to which to open a topology connector - or null/empty if no topology connector
     * is configured
     */
    public URL[] getTopologyConnectorURLs() {
        return topologyConnectorUrls;
    }

    /**
     * Returns a comma separated list of hostnames and/or ip addresses which are allowed as
     * remote hosts to open connections to the topology connector servlet
     *
     * @return a comma separated list of hostnames and/or ip addresses which are allowed as
     * remote hosts to open connections to the topology connector servlet
     */
    public String[] getTopologyConnectorWhitelist() {
        return topologyConnectorWhitelist;
    }

    protected String getDiscoveryResourcePath() {
        return discoveryResourcePath;
    }

    /**
     * Returns the resource path where cluster instance informations are stored.
     *
     * @return the resource path where cluster instance informations are stored
     */
    public String getClusterInstancesPath() {
        return getDiscoveryResourcePath() + CLUSTERINSTANCES_RESOURCE;
    }

    @Override
    public String getSyncTokenPath() {
        return getDiscoveryResourcePath() + SYNC_TOKEN_RESOURCE;
    }

    @Override
    public String getIdMapPath() {
        return getDiscoveryResourcePath() + ID_MAP_RESOURCE;
    }

    /**
     * @return true if hmac is enabled.
     */
    public boolean isHmacEnabled() {
        return hmacEnabled;
    }

    /**
     * @return the shared key
     */
    public String getSharedKey() {
        return sharedKey;
    }

    /**
     * @return the interval of the shared key for hmac.
     */
    public long getKeyInterval() {
        return keyInterval;
    }

    /**
     * @return true if encryption is enabled.
     */
    public boolean isEncryptionEnabled() {
        return encryptionEnabled;
    }

    /**
     * @return true if requests on the topology connector should be gzipped
     * (which only works if the server accepts that.. ie discovery.impl 1.0.4+)
     */
    public boolean isGzipConnectorRequestsEnabled() {
        return gzipConnectorRequestsEnabled;
    }

    /**
     * @return true if the auto-stopping of local-loop topology connectors is enabled.
     */
    public boolean isAutoStopLocalLoopEnabled() {
        return autoStopLocalLoopEnabled;
    }

    /**
     * Returns the backoff factor to be used for standby (loop) connectors
     *
     * @return the backoff factor to be used for standby (loop) connectors
     */
    public int getBackoffStandbyFactor() {
        return backoffStandbyFactor;
    }

    /**
     * Returns the (maximum) backoff factor to be used for stable connectors
     *
     * @return the (maximum) backoff factor to be used for stable connectors
     */
    public int getBackoffStableFactor() {
        return backoffStableFactor;
    }

    /**
     * Returns the backoff interval for standby (loop) connectors in seconds
     *
     * @return the backoff interval for standby (loop) connectors in seconds
     */
    public long getBackoffStandbyInterval() {
        final int factor = getBackoffStandbyFactor();
        if (factor <= 1) {
            return -1;
        } else {
            return factor * getConnectorPingInterval();
        }
    }

    @Override
    public long getConnectorPingInterval() {
        return connectorPingInterval;
    }

    @Override
    public long getConnectorPingTimeout() {
        return connectorPingTimeout;
    }

    public long getDiscoveryLiteCheckInterval() {
        return discoveryLiteCheckInterval;
    }

    @Override
    public long getClusterSyncServiceTimeoutMillis() {
        return clusterSyncServiceTimeout * 1000;
    }

    @Override
    public long getClusterSyncServiceIntervalMillis() {
        return clusterSyncServiceInterval * 1000;
    }

    public boolean getSyncTokenEnabled() {
        return syncTokenEnabled;
    }

    public boolean isInvertLeaderElectionPrefixOrder() {
        return invertLeaderElectionPrefixOrder;
    }

    public long getLeaderElectionPrefix() {
        return leaderElectionPrefix;
    }

    /**
     * Checks if the system property JOIN_DELAY_SYSTEM_PROPERTY_NAME is set
     * and uses that value. Otherwise uses the provided default.
     *
     * @param configValue the provided default to be used unless the system property is set
     * @return the overwritten value to be used as the actual config value
     */
    private boolean applyJoinerDelayOverwrite(final boolean configValue) {
        final long now = System.currentTimeMillis();
        if (joinerDelayOverwriteNextCheck != 0 && now < joinerDelayOverwriteNextCheck) {
            // avoid reading system property too often, only do every 5 minutes.
            return joinerDelayOverwrite;
        }
        final String systemPropertyValue = System.getProperty(JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME);
        final boolean newJoinerDelayOverwrite = standardConverter().convert(systemPropertyValue).defaultValue(configValue).to(Boolean.class);
        if (joinerDelayOverwriteNextCheck == 0) {
            logger.info("applyJoinerDelayOverwrite : initialization."
                    + " system property '" + JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME + "' = " + systemPropertyValue
                    + ", config value = " + configValue
                    + ", resulting value = " + newJoinerDelayOverwrite);
        } else if (newJoinerDelayOverwrite != joinerDelayOverwrite) {
            logger.info("applyJoinerDelayOverwrite : value change."
                    + " system property '" + JOINER_DELAY_ENABLED_SYSTEM_PROPERTY_NAME + "' = " + systemPropertyValue
                    + ", config value = " + configValue
                    + ", resulting value = " + newJoinerDelayOverwrite);
        }
        joinerDelayOverwrite = newJoinerDelayOverwrite;
        joinerDelayOverwriteNextCheck = now + 300000; //re-check every 300sec, should be fast and slow enough
        return joinerDelayOverwrite;
    }

    public boolean getSuppressPartiallyStartedInstances() {
        // allow configured suppressPartiallyStartedInstance
        // to be overwritten by a system property (JOIN_DELAY_ENABLED_SYSTEM_PROPERTY_NAME)
        return applyJoinerDelayOverwrite(suppressPartiallyStartedInstance);
    }

    public long getSuppressionTimeoutSeconds() {
        return suppressionTimeoutSeconds;
    }

    public long getJoinerDelayMillis() {
        return Math.max(0, joinerDelaySeconds * 1000);
    }
}
