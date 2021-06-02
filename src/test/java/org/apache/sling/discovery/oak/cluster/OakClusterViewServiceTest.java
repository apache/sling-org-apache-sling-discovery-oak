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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.sling.discovery.base.commons.ClusterViewService;
import org.apache.sling.discovery.base.commons.UndefinedClusterViewException;
import org.apache.sling.discovery.base.its.setup.VirtualInstance;
import org.apache.sling.discovery.commons.providers.spi.base.IdMapService;
import org.apache.sling.discovery.oak.its.setup.OakVirtualInstanceBuilder;
import org.junit.Test;

public class OakClusterViewServiceTest {

    @Test
    public void testLocalClusterView() throws Exception {
        OakVirtualInstanceBuilder builder1 =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance1")
                .newRepository("/foo/barrio/foo/", true)
                .setConnectorPingInterval(999)
                .setConnectorPingTimeout(999);
        VirtualInstance instance1 = builder1.build();

        ClusterViewService cvs1 = instance1.getClusterViewService();

        try {
            cvs1.getLocalClusterView();
            fail("should throw UndefinedClusterViewException");
        } catch (UndefinedClusterViewException e) {
            // ok
        }

        OakVirtualInstanceBuilder builder2 =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance2")
                .useRepositoryOf(instance1)
                .setConnectorPingInterval(999)
                .setConnectorPingTimeout(999);
        VirtualInstance instance2 = builder2.build();

        instance1.heartbeatsAndCheckView();
        instance2.heartbeatsAndCheckView();
        instance1.heartbeatsAndCheckView();
        instance2.heartbeatsAndCheckView();

        // this should not fail:
        assertNotNull(cvs1.getLocalClusterView());

        final ClusterViewService cvs2 = instance2.getClusterViewService();
        final IdMapService idMapService = builder2.getIdMapService();
        idMapService.clearCache();
        idMapService.clearCache();
        // this should not fail neither:
        assertNotNull(cvs2.getLocalClusterView());

        instance2.shutdownRepository();
        try {
            // but this should fail as the repository is shutdown
            cvs2.getLocalClusterView();
            fail("should throw UndefinedClusterViewException");
        } catch(UndefinedClusterViewException e) {
            // ok
        }
    }

    @Test
    public void testGetResourceResolverException() throws Exception {
        OakClusterViewService cvs = OakClusterViewService.testConstructor(null, null, null, null);
        try {
            cvs.getLocalClusterView();
            fail("should throw UndefinedClusterViewException");
        } catch (UndefinedClusterViewException e) {
            // ok
        }
    }
}
