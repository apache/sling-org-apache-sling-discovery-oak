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

    static final long SUPPRESSION_TIMEOUT_MILLIS = 45 * 60 * 1000;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ResourceResolver resourceResolver;
    private final Config config;
    private final int me;
    private final long seqNum;

    private final boolean syncTokenEnabled;

    private final boolean suppressApplicable;
    private final Set<Integer> partiallyStartedClusterNodeIds = new HashSet<>();

    PartialStartupDetector(ResourceResolver resourceResolver, Config config,
            long lowestSeqNum, int me, String mySlingId, long seqNum, long timeout) {
        this.resourceResolver = resourceResolver;
        this.config = config;
        this.me = me;
        this.seqNum = seqNum;

        this.syncTokenEnabled = config != null && config.getSyncTokenEnabled();

        // suppressing is enabled
        // * when so configured
        // * when the local instance ever showed to peers that it has fully started.
        // and one way to verify for that is to demand that it ever wrote a synctoken.
        // and to check that we keep note of the first ever successful seq num returned
        // here
        // and require the current syncToken to be at least that.
        final long now = System.currentTimeMillis();
        final long mySyncToken = readSyncToken(resourceResolver, mySlingId);
        suppressApplicable = config.getSuppressPartiallyStartedInstances()
                && ((timeout == 0) || (now < timeout))
                && (mySyncToken != -1) && (lowestSeqNum != -1)
                && (mySyncToken >= lowestSeqNum);
    }

    private boolean isSuppressing(int id) {
        if (!suppressApplicable || (id == me)) {
            return false;
        }
        return true;
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
        final String syncTokenStr = resourceMap.get(slingId, String.class);
        try {
            return Long.parseLong(syncTokenStr);
        } catch (NumberFormatException nfe) {
            logger.warn(
                    "readSyncToken: unparsable (non long) syncToken: " + syncTokenStr);
            return -1;
        }
    }

    boolean suppressMissingIdMap(int id) {
        if (!isSuppressing(id)) {
            return false;
        }
        partiallyStartedClusterNodeIds.add(id);
        logger.info(
                "suppressMissingIdMap: ignoring partially started clusterNode without idMap entry (in "
                        + config.getIdMapPath() + ") : " + id);
        return true;
    }

    boolean suppressMissingSyncToken(int id, String slingId) {
        if (!syncTokenEnabled || !isSuppressing(id)) {
            return false;
        }
        final long syncToken = readSyncToken(resourceResolver, slingId);
        if (syncToken != -1 && (syncToken >= seqNum)) {
            return false;
        }
        partiallyStartedClusterNodeIds.add(id);
        logger.info(
                "suppressMissingSyncToken: ignoring partially started clusterNode without valid syncToken (in "
                        + config.getSyncTokenPath() + ") : " + id
                        + " (expected at least: " + seqNum + ", is: " + syncToken);
        return true;
    }

    boolean suppressMissingLeaderElectionId(int id) {
        if (!isSuppressing(id)) {
            return false;
        }
        partiallyStartedClusterNodeIds.add(id);
        logger.info(
                "suppressMissingLeaderElectionId: ignoring partially started clusterNode without leaderElectionId (in "
                        + config.getClusterInstancesPath() + ") : " + id);
        return true;
    }

    Collection<?> getPartiallyStartedClusterNodeIds() {
        return partiallyStartedClusterNodeIds;
    }
}