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

/**
 * Data object containing all infos about a particular Sling instance
 */
public class InstanceInfo {

    private final int clusterNodeId;
    private final String slingId;
    private final long syncToken;
    private final String leaderElectionId;

    public InstanceInfo(int clusterNodeId, String slingId, long syncToken, String leaderElectionId) {
        this.clusterNodeId = clusterNodeId;
        this.slingId = slingId;
        this.syncToken = syncToken;
        this.leaderElectionId = leaderElectionId;
    }

    public String getLeaderElectionId() {
        return leaderElectionId;
    }

    public int getClusterNodeId() {
        return clusterNodeId;
    }

    public String getSlingId() {
        return slingId;
    }

    public boolean isSyncTokenNewerOrEqual(long seqNum) {
        if (syncToken == -1) {
            return false;
        } else {
            return syncToken >= seqNum;
        }
    }

    @Override
    public String toString() {
        return "an Instance[clusterNodeId = " + clusterNodeId + ", slingId = " + slingId + ", leaderElectionId = " + leaderElectionId + ", syncToken = " + syncToken + "]";
    }
}
