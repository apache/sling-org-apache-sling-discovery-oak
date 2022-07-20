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

import java.util.HashMap;
import java.util.Map;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.discovery.commons.providers.spi.base.IdMapService;
import org.apache.sling.discovery.oak.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to detect instances that are only partially started.
 * Partially in this context means that they have updated their
 * Oak lease but have not written the required data on the Sling 
 * level (which includes the idmap, the synctoken and the leaderElectionid).
 */
public class ClusterReader {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final IdMapService idMapService;

    private final ResourceResolver resourceResolver;

    private final Config config;

    private final Map<Integer, InstanceInfo> seenLocalInstances;

    public ClusterReader(ResourceResolver resourceResolver, Config config, IdMapService idMapService, Map<Integer, InstanceInfo> seenLocalInstances) {
        this.resourceResolver = resourceResolver;
        this.config = config;
        this.idMapService = idMapService;
        this.seenLocalInstances = seenLocalInstances == null ? new HashMap<>() : seenLocalInstances;
    }

    private long readSyncToken(String slingId) {
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
                    "readSyncToken: unparsable (non long) syncToken: {}", syncTokenStr);
            return -1;
        }
    }

    private String readLeaderElectionId(String slingId) {
        if (slingId==null) {
            throw new IllegalStateException("slingId must not be null");
        }
        final String myClusterNodePath = config.getClusterInstancesPath()+"/"+slingId;
        // SLING-6924 case 1 : /var/discovery/oak/clusterInstances/<slingId> can be non existant == null
        final Resource myClusterNode = resourceResolver.getResource(myClusterNodePath);
        if (myClusterNode == null) {
            // SLING-6924 : return null case 1
            return null;
        }
        ValueMap resourceMap = myClusterNode.adaptTo(ValueMap.class);
        // SLING-6924 case 2 : /var/discovery/oak/clusterInstances/<slingId> can exist BUT leaderElectionId not yet set
        //    namely the "leaderElectionId" is only written when resetLeaderElectionId() is called - which happens
        //    on OakViewChecker.activate (or when isolated) - and this activate *can* happen after properties
        //    or announcements have been written - those end up below /var/discovery/oak/clusterInstances/<slingId>/
        String result = resourceMap.get("leaderElectionId", String.class);

        // SLING-6924 : return null case 2 (if leaderElectionId is indeed null, that is)
        return result;
    }

    /**
     * Reads all discovery.oak related data for clusterNodeId
     * and fills the internal structure with the resulting info -
     * or remembers that the clusterNode was incomplete (partially started)
     * @param clusterNodeId the clusterNodeId to be read
     * @return the instance if it is has all required data stored (which does not
     * necessarily mean it has completely started - syncToken is not checked at this stage yet)
     * @throws PersistenceException 
     */
    public InstanceReadResult readInstance(int clusterNodeId, boolean failOnMissingSyncToken) throws PersistenceException {
        final String slingId = idMapService.toSlingId(clusterNodeId, resourceResolver);
        if (slingId == null) {
            idMapService.clearCache();
            return InstanceReadResult.fromErrorMsg("no slingId mapped for clusterNodeId=" + clusterNodeId);
        }
        final String leaderElectionId = readLeaderElectionId(slingId);
        if (leaderElectionId == null) {
            return InstanceReadResult.fromErrorMsg("no leaderElectionId available yet for slingId=" + slingId);
        }
        final boolean hasSeenLocalInstance = hasSeenLocalinstance(clusterNodeId, slingId, leaderElectionId);
        final long syncToken = readSyncToken(slingId);
        if (syncToken == -1 && failOnMissingSyncToken && !hasSeenLocalInstance) {
            return InstanceReadResult.fromErrorMsg("no syncToken available yet for slingId=" + slingId);
        }
        return InstanceReadResult.fromInstance(new InstanceInfo(clusterNodeId, slingId, syncToken, leaderElectionId));
    }

    private boolean hasSeenLocalinstance(int clusterNodeId, String slingId,
            String leaderElectionId) {
        InstanceInfo instance = seenLocalInstances.get(clusterNodeId);
        if (instance == null) {
            return false;
        }
        return instance.getSlingId().equals(slingId) &&
                instance.getLeaderElectionId().equals(leaderElectionId);
    }
}
