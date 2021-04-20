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

import java.util.UUID;

import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.base.its.setup.mock.MockFactory;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.junit.Test;

public class TopologyWebConsolePluginTest {

    @Test
    /** trivial test that covers the first new Exception catch case */
    public void testNoResourceResolverFactory() throws Exception {
        final DefaultClusterView cluster = new DefaultClusterView(UUID.randomUUID().toString());
        final DummyTopologyView view = new DummyTopologyView()
                .addInstance(UUID.randomUUID().toString(), cluster, true, true);
        final TopologyWebConsolePlugin p = new TopologyWebConsolePlugin();
        p.activate(MockFactory.mockBundleContext());
        p.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, view));
        p.resourceResolverFactory = MockFactory.mockResourceResolverFactory();
        p.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, view));
    }

    @Test
    /** trivial test that covers the 2nd new Exception catch case */
    public void testResourceResolverFactory() throws Exception {
        final DefaultClusterView cluster = new DefaultClusterView(UUID.randomUUID().toString());
        final DummyTopologyView view = new DummyTopologyView()
                .addInstance(UUID.randomUUID().toString(), cluster, true, true);
        final TopologyWebConsolePlugin p = new TopologyWebConsolePlugin();
        p.resourceResolverFactory = MockFactory.mockResourceResolverFactory();
        p.activate(MockFactory.mockBundleContext());
        p.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, view));
    }
}
