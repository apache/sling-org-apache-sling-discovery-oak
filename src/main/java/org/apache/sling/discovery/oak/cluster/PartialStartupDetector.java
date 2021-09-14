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
package org.apache.sling.discovery.oak.cluster;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.discovery.commons.providers.util.LogSilencer;
import org.apache.sling.discovery.oak.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovery.oak requires that both Oak and Sling are operating normally in
 * order to declare victory and announce a new topology.
 * <p/>
 * The startup phase is especially tricky in this regard, since there are
 * multiple elements that need to get updated (some are in the Oak layer, some
 * in Sling):
 * <ul>
 * <li>lease & clusterNodeId : this is maintained by Oak</li>
 * <li>idMap : this is maintained by IdMapService</li>
 * <li>leaderElectionId : this is maintained by OakViewChecker</li>
 * <li>syncToken : this is maintained by SyncTokenService</li>
 * </ul>
 * A successful join of a cluster instance to the topology requires all 4
 * elements to be set (and maintained, in case of lease and syncToken)
 * correctly.
 * <p/>
 * This PartialStartupDetector is in charge of ensuring that a newly joined
 * instance has all these elements set. Otherwise it is considered a "partially
 * started instance" (PSI) and suppressed.
 * <p/>
 * The suppression ensures that existing instances aren't blocked by a rogue,
 * partially starting instance. However, there's also a timeout after which the
 * suppression is no longer applied - at which point such a rogue instance will
 * block existing instances. Infrastructure must ensure that a rogue instance is
 * detected and restarted/fixed in a reasonable amount of time.
 */
public class PartialStartupDetector {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ResourceResolver resourceResolver;
    private final Config config;
    private final int me;
    private final long currentSeqNum;

    private final boolean syncTokenEnabled;

    private final boolean suppressingApplicable;
    private final Set<Integer> partiallyStartedClusterNodeIds = new HashSet<>();

    private final LogSilencer logSilencer;

    /**
     * @param lowestLocalSeqNum the lowest sequence number which
     * the local OakClusterViewService has handled as part of asClusterView
     * @param me the clusterNodeId (provided by oak) of the local instance (==me)
     * @param mySlingId the slingId of the local instance (==me)
     * @param timeoutMillis -1 or 0 disables the timeout, otherwise the suppression
     * is only done for the provided maximum number of milliseconds.
     * @param logSilencer the LogSilencer to use
     */
    PartialStartupDetector(ResourceResolver resourceResolver, Config config,
            long lowestLocalSeqNum, int me, String mySlingId, long currentSeqNum, long timeoutMillis,
            LogSilencer logSilencer) {
        this.resourceResolver = resourceResolver;
        this.config = config;
        this.me = me;
        this.currentSeqNum = currentSeqNum;

        this.syncTokenEnabled = config != null && config.getSyncTokenEnabled();

        // suppressing is enabled
        // * when so configured
        // * we haven't hit the timeout yet
        // * when the local instance ever showed to peers that it has fully started.
        // and one way to verify for that is to demand that it ever wrote a synctoken.
        // and to check that we keep note of the first ever successful seq num returned
        // here
        // and require the current syncToken to be at least that.
        final long now = System.currentTimeMillis();
        final long mySyncToken = readSyncToken(resourceResolver, mySlingId);
        final boolean suppressionConfigured = config != null && config.getSuppressPartiallyStartedInstances();
        // timeoutMillis can have 0 or non-0 depending on the following cases:
        // 1. if timeout is disabled by config it is always 0 - suppressing is applicable then
        // 2. before the first suppressing or after a non-suppressing case it is also 0 - suppressing is applicable then
        // 3. it is non-0 once suppressing starts but is within the timeout - then we keep suppressing
        // 4. it is non-0 after the timeout - then suppressing stops
        suppressingApplicable = suppressionConfigured
                && ((timeoutMillis <= 0) || (now < timeoutMillis))
                && (mySyncToken != -1) && (lowestLocalSeqNum != -1)
                && (mySyncToken >= lowestLocalSeqNum);
        if (logger.isDebugEnabled()) {
            logger.debug("<init> suppressionConfigured = " + suppressionConfigured + ", me = " + me + ", mySlingId = " + mySlingId +
                    ", timeoutMillis = " + timeoutMillis + ", mySyncToken = " + mySyncToken +
                    ", lowestLocalSeqNum = " + lowestLocalSeqNum + ", suppressingApplicable = " + suppressingApplicable);
        }
        this.logSilencer = logSilencer;
    }

    private boolean isSuppressible(int id) {
        return suppressingApplicable && (id != me);
    }

    private long readSyncToken(ResourceResolver resourceResolver, String slingId) {
        if (slingId == null) {
            throw new IllegalStateException("slingId must not be null");
        }
        final Resource syncTokenNode = resourceResolver
                .getResource(config.getSyncTokenPath());
        if (syncTokenNode == null) {
            return -1;
        }
        final ValueMap resourceMap = syncTokenNode.adaptTo(ValueMap.class);
        if (resourceMap == null) {
            return -1;
        }
        final String syncTokenStr = resourceMap.get(slingId, String.class);
        if (syncTokenStr == null) {
            return -1;
        }
        try {
            return Long.parseLong(syncTokenStr);
        } catch (NumberFormatException nfe) {
            logger.warn(
                    "readSyncToken: unparsable (non long) syncToken: " + syncTokenStr);
            return -1;
        }
    }

    boolean suppressMissingIdMap(int id) {
        if (!isSuppressible(id)) {
            return false;
        }
        partiallyStartedClusterNodeIds.add(id);
        logSilencer.infoOrDebug("suppressMissingIdMap-" + id,
                "suppressMissingIdMap: ignoring partially started clusterNode without idMap entry (in "
                        + config.getIdMapPath() + ") : " + id);
        return true;
    }

    boolean suppressMissingSyncToken(int id, String slingId) {
        if (!syncTokenEnabled || !isSuppressible(id)) {
            return false;
        }
        final long syncToken = readSyncToken(resourceResolver, slingId);
        if (syncToken != -1 && (syncToken >= currentSeqNum)) {
            return false;
        }
        partiallyStartedClusterNodeIds.add(id);
        logSilencer.infoOrDebug("suppressMissingSyncToken-"+slingId,
                "suppressMissingSyncToken: ignoring partially started clusterNode without valid syncToken (in "
                        + config.getSyncTokenPath() + "/" + slingId + ") : id=" + id
                        + " (expected at least: currentSyncToken=" + currentSeqNum + ", is: syncToken=" + syncToken + ")");
        return true;
    }

    boolean suppressMissingLeaderElectionId(int id) {
        if (!isSuppressible(id)) {
            return false;
        }
        partiallyStartedClusterNodeIds.add(id);
        logSilencer.infoOrDebug("suppressMissingLeaderElectionId-"+id,
                "suppressMissingLeaderElectionId: ignoring partially started clusterNode without leaderElectionId (in "
                        + config.getClusterInstancesPath() + ") : " + id);
        return true;
    }

    Collection<Integer> getPartiallyStartedClusterNodeIds() {
        return partiallyStartedClusterNodeIds;
    }
}