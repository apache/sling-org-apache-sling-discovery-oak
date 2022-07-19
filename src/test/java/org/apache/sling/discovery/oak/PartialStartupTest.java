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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.discovery.base.commons.OakViewCheckerFactory;
import org.apache.sling.discovery.base.commons.UndefinedClusterViewException;
import org.apache.sling.discovery.base.its.setup.mock.MockFactory;
import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.apache.sling.discovery.commons.providers.spi.LocalClusterView;
import org.apache.sling.discovery.commons.providers.spi.base.DescriptorHelper;
import org.apache.sling.discovery.commons.providers.spi.base.DiscoveryLiteDescriptor;
import org.apache.sling.discovery.commons.providers.spi.base.DiscoveryLiteDescriptorBuilder;
import org.apache.sling.discovery.commons.providers.spi.base.IdMapService;
import org.apache.sling.discovery.commons.providers.spi.base.IdMapServiceAccessor;
import org.apache.sling.discovery.commons.providers.spi.base.RepositoryTestHelper;
import org.apache.sling.discovery.commons.providers.spi.base.SyncTokenService;
import org.apache.sling.discovery.oak.cluster.ClusterReader;
import org.apache.sling.discovery.oak.cluster.InstanceReadResult;
import org.apache.sling.discovery.oak.cluster.OakClusterViewService;
import org.apache.sling.discovery.oak.pinger.OakViewChecker;
import org.apache.sling.settings.SlingSettingsService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PartialStartupTest {

    private MemoryNodeStore nodeStore;

    private List<MiniInstance> instances = new LinkedList<>();

    static class Identifiable {
        final int me;

        Identifiable(int me) {
            this.me = me;
        }
    }

    static class MiniInstance extends Identifiable {

        private final Config config;
        private final String slingId;
        private final JackrabbitRepository jcrRepo;
        private final ResourceResolverFactory resourceResolverFactory;
        private final IdMapService idMapService;
        private final SlingSettingsService settingsService;
        private final OakClusterViewService ocvService;

        MiniInstance(int me, NodeStore nodeStore, final boolean suppressPartiallyStartedInstances, final boolean syncTokenEnabled) {
            this(me, UUID.randomUUID().toString(), nodeStore, suppressPartiallyStartedInstances, syncTokenEnabled);
        }

        MiniInstance(int me, String slingId, NodeStore nodeStore, final boolean suppressPartiallyStartedInstances, final boolean syncTokenEnabled) {
            this(me, slingId, nodeStore, new Config() {
                @Override
                public long getClusterSyncServiceIntervalMillis() {
                    return 9999;
                }

                @Override
                public boolean getSuppressPartiallyStartedInstances() {
                    return suppressPartiallyStartedInstances;
                }

                @Override
                public boolean getSyncTokenEnabled() {
                    return syncTokenEnabled;
                }
            });
        }

        MiniInstance(int me, String slingId, NodeStore nodeStore, Config config) {
            super(me);
            this.slingId = slingId;
            jcrRepo = (JackrabbitRepository) RepositoryTestHelper.createOakRepository(nodeStore);
            this.config = config;

            settingsService = Mockito.mock(SlingSettingsService.class);
            Mockito.when(settingsService.getSlingId()).thenReturn(slingId);

            resourceResolverFactory = MockFactory.mockResourceResolverFactory(jcrRepo);

            idMapService = IdMapService.testConstructor(config, settingsService, resourceResolverFactory);

            ocvService = OakClusterViewService.testConstructor(settingsService, resourceResolverFactory, idMapService, config);
        }

        private void setDescriptor(DiscoveryLiteDescriptorBuilder builder) throws Exception {
            DescriptorHelper.setDescriptor(resourceResolverFactory, DiscoveryLiteDescriptor.OAK_DISCOVERYLITE_CLUSTERVIEW, builder == null ? null : builder.me(me).asJson());
        }

        private void initLeaderElectionId() {
            OakViewChecker viewChecker = OakViewCheckerFactory.createOakViewChecker(settingsService, resourceResolverFactory, config);
            viewChecker.resetLeaderElectionId();
        }

        private void storeSyncToken(String syncToken) {
            SyncTokenService syncTokenService = SyncTokenService.testConstructorAndActivate(config, resourceResolverFactory, settingsService);
            final String clusterId = UUID.randomUUID().toString();
            final DefaultClusterView cluster = new DefaultClusterView(clusterId);
            BaseTopologyView view = new DummyTopologyView(syncToken).addInstance(slingId, cluster, true, true);
            final AtomicBoolean done = new AtomicBoolean();
            syncTokenService.sync(view, new Runnable() {

                @Override
                public void run() {
                    done.set(true);
                }

            });
            assertTrue(done.get());
        }

        private void storeSlingId() throws Exception {
            storeSlingId(simpleDesc(1).me(me));
        }

        private void storeSlingId(DiscoveryLiteDescriptorBuilder descBuilder) throws Exception {
            setDescriptor(descBuilder);
            try {
                assertTrue(idMapService.waitForInit(5000));
            } finally {
                setDescriptor(null);
            }
        }

        private LocalClusterView getLocalClusterView(DiscoveryLiteDescriptorBuilder descBuilder) throws Exception {
            setDescriptor(descBuilder);
            Log4jErrorCatcher catcher = Log4jErrorCatcher.installErrorCatcher(ocvService.getClass());
            try {
                return ocvService.getLocalClusterView();
            } finally {
                catcher.uninstall();
                setDescriptor(null);
                String theLastErrorMsg = catcher.getLastErrorMsg();
                if (theLastErrorMsg != null) {
                    fail("error encountered : " + theLastErrorMsg);
                }
            }
        }

        public void tearDown() {
            IdMapServiceAccessor.deactivate(idMapService);
        }

        @Override
        public String toString() {
            return "a MiniInstance[clusterNodeId = " + me + ", slingId = " + slingId + "]";
        }
    }

    private MiniInstance withNewSlingId(Identifiable baseInstance) throws Exception {
        return withSlingId(baseInstance, UUID.randomUUID().toString());
    }

    private MiniInstance withSlingId(Identifiable baseInstance, String newSlingId) throws Exception {
        return new MiniInstance(baseInstance.me, newSlingId, nodeStore, true, true);
    }

    private MiniInstance create(int id, boolean storeSlingId, boolean storeLeaderElectionId, String syncIdOrNull) throws Exception {
        return create(true, true, id, storeSlingId, storeLeaderElectionId, syncIdOrNull);
    }

    private MiniInstance create(boolean suppressPartiallyStartedInstances, boolean syncTokenEnabled, int id, boolean storeSlingId, boolean storeLeaderElectionId, String syncIdOrNull) throws Exception {
        MiniInstance instance = new MiniInstance(id, nodeStore, suppressPartiallyStartedInstances, syncTokenEnabled);
        if (storeSlingId) {
            instance.storeSlingId();
        }
        if (storeLeaderElectionId) {
            instance.initLeaderElectionId();
        }
        if (storeSlingId && storeLeaderElectionId) {
            try {
                @SuppressWarnings("deprecation")
                ClusterReader r = new ClusterReader(instance.resourceResolverFactory.getAdministrativeResourceResolver(null), instance.config, instance.idMapService, null);
                InstanceReadResult i = r.readInstance(id, false);
                assertNotNull(i.getInstanceInfo());
                System.out.println(" instance " + id + " read " + i.getInstanceInfo());
            } catch (LoginException | PersistenceException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if (syncIdOrNull != null) {
            instance.storeSyncToken(syncIdOrNull);
        }
        instances.add(instance);
        return instance;
    }

    @Before
    public void setup() throws Exception {
        nodeStore = new MemoryNodeStore();
    }

    @After
    public void tearDown() throws Exception {
        for (MiniInstance instance : instances) {
            instance.tearDown();
        }
    }

    private static DiscoveryLiteDescriptorBuilder desc() {
        return new DiscoveryLiteDescriptorBuilder();
    }

    private static DiscoveryLiteDescriptorBuilder simpleDesc(int seqNum) {
        return new DiscoveryLiteDescriptorBuilder().id("anyId").setFinal(true).seq(seqNum);
    }

    private static DiscoveryLiteDescriptorBuilder simpleDesc(int seqNum, Identifiable...instances) {
        final DiscoveryLiteDescriptorBuilder builder = new DiscoveryLiteDescriptorBuilder().id("anyId").setFinal(true).seq(seqNum);
        Integer[] activeIds = new Integer[instances.length];
        for (int i = 0; i < instances.length; i++ ) {
            activeIds[i] = instances[i].me;
        }
        builder.activeIds(activeIds);
        return builder;
    }

    private static void assertNoPartiallyStarted(LocalClusterView view) {
        assertFalse(view.hasPartiallyStartedInstances());
    }

    private static void assertPartiallyStarted(LocalClusterView view, int... suppressedIds) {
        if (suppressedIds.length == 0) {
            assertFalse(view.hasPartiallyStartedInstances());
            return;
        }
        assertTrue(view.hasPartiallyStartedInstances());
        final Set<Integer> suppressed = new HashSet<>();
        for (int i : suppressedIds) {
            suppressed.add(i);
        }
        assertEquals(suppressed, view.getPartiallyStartedClusterNodeIds());
    }

    @Test
    public void testNoDescriptor() throws Exception {
        doTestNoDescriptor(create(false, false, 1, false, false, null));
        doTestNoDescriptor(create(true, false, 1, false, false, null));
        doTestNoDescriptor(create(false, true, 1, false, false, null));
        doTestNoDescriptor(create(true, true, 1, false, false, null));
    }

    private void doTestNoDescriptor(MiniInstance instance) throws Exception {
        try {
            instance.getLocalClusterView(null);
            fail("should have thrown exception");
        } catch(UndefinedClusterViewException ucve) {
            // this empty catch is okay
        }
    }

    @Test
    public void testNotFinal() throws Exception {
        doTestNotFinal(create(false, false, 1, false, false, null));
        doTestNotFinal(create(true, false, 1, false, false, null));
        doTestNotFinal(create(false, true, 1, false, false, null));
        doTestNotFinal(create(true, true, 1, false, false, null));
    }

    private void doTestNotFinal(MiniInstance instance) throws Exception {
        try {
            instance.getLocalClusterView(desc().id("id").me(1).activeIds(1));
            fail("should have thrown exception");
        } catch(UndefinedClusterViewException ucve) {
            // this empty catch is okay
        }
    }

    @Test
    public void testNoLeaderElectionId() throws Exception {
        MiniInstance instance = create(1, true, false, null);
        try {
            instance.getLocalClusterView(simpleDesc(1).me(1).activeIds(1));
            fail("should have thrown exception");
        } catch(UndefinedClusterViewException ucve) {
            // this empty catch is okay
        }
    }

    @Test
    public void testSimple() throws Exception {
        doTestSimple(create(false, false, 1, true, true, null));
        doTestSimple(create(true, false, 1, true, true, null));
        doTestSimple(create(false, true, 1, true, true, null));
        doTestSimple(create(true, true, 1, true, true, null));
    }

    private void doTestSimple(MiniInstance instance) throws Exception {
        LocalClusterView localView = instance.getLocalClusterView(simpleDesc(1).me(instance.me).activeIds(instance.me));
        assertNotNull(localView);
        assertNoPartiallyStarted(localView);
        assertEquals(1, localView.getInstances().size());
        assertNotNull(localView.getLocalInstance());
        assertEquals(instance.slingId, localView.getLocalInstance().getSlingId());
    }

    @Test
    public void testNoLocalSyncTokenYet() throws Exception {
        MiniInstance instance1 = create(1, true, true, null);
        MiniInstance instance2 = create(2, true, true, "2");
        MiniInstance instance3 = create(3, false, false, null);
        try {
            instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance3 = create(3, true, false, null);
        try {
            instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance3 = create(3, false, true, null);
        try {
            instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance3 = create(3, false, false, "2");
        try {
            instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance3 = create(3, true, false, "2");
        try {
            instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance3 = create(3, false, true, "2");
        try {
            instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
        System.out.println(" => instance1 is " + instance1);
        System.out.println(" => instance2 is " + instance2);
        instance3 = create(3, true, true, null);
        instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
        instance3 = create(3, true, true, "2");
        instance1.getLocalClusterView(simpleDesc(2, instance1, instance2, instance3));
    }

    @Test
    public void testSlowJoiner_suppressionDisabled() throws Exception {
        // init just with 1 instance
        MiniInstance instance = create(false, false, 1, true, true, null);

        // let a slow instance 2 join
        try {
            instance.getLocalClusterView(simpleDesc(2).activeIds(1, 2));
            fail("should complain");
        } catch(Exception e) {
            // this empty catch is okay
        }
    }

    @Test
    public void testSlowJoiner() throws Exception {
        // init just with 1 instance
        MiniInstance instance1 = create(1, true, true, null);
        assertNotNull(instance1.getLocalClusterView(simpleDesc(1).me(instance1.me).activeIds(instance1.me)));

        instance1.storeSyncToken("1");

        // let a slow instance 2 join
        // this should result in a view with just instance 1 - so excludes slow instance 2 for now
        LocalClusterView localView = instance1.getLocalClusterView(simpleDesc(2).activeIds(1, 2));
        assertNotNull(localView);
        assertPartiallyStarted(localView, 2);
        assertEquals(1, localView.getInstances().size());
        assertNotNull(localView.getLocalInstance());
        assertEquals(instance1.slingId, localView.getLocalInstance().getSlingId());
    }

    @Test
    public void testFastJoiner() throws Exception {
        // init just with 1 instance
        MiniInstance instance1 = create(1, true, true, null);
        assertNotNull(instance1.getLocalClusterView(simpleDesc(1).me(instance1.me).activeIds(instance1.me)));

        instance1.storeSyncToken("1");

        // let a fast instance 2 join
        // but first don't store the syncToken yet
        MiniInstance instance2 = create(2, true, true, null);
        LocalClusterView localView = instance1.getLocalClusterView(simpleDesc(2, instance1, instance2));
        assertNotNull(localView);
        assertPartiallyStarted(localView, 2);
        assertEquals(1, localView.getInstances().size());

        // only store it now - and now it should have joined
        instance2.storeSyncToken("2");
        localView = instance1.getLocalClusterView(simpleDesc(2, instance1, instance2));
        assertNotNull(localView);
        assertNoPartiallyStarted(localView);
        assertEquals(2, localView.getInstances().size());
    }

    @Test
    public void testJoineLeaveMultiple() throws Exception {
        MiniInstance instance1 = create(1, true, true, null);
        assertNotNull(instance1.getLocalClusterView(simpleDesc(1).me(instance1.me).activeIds(instance1.me)));

        instance1.storeSyncToken("1");
        assertNotNull(instance1.getLocalClusterView(simpleDesc(1).me(instance1.me).activeIds(instance1.me)));

        // let a fast instance 2 join
        MiniInstance instance2 = create(2, true, true, "2");
        LocalClusterView localView = instance1.getLocalClusterView(simpleDesc(2, instance1, instance2));
        assertNotNull(localView);
        assertNoPartiallyStarted(localView);
        assertEquals(2, localView.getInstances().size());
        // from 2's point of view
        localView = instance2.getLocalClusterView(simpleDesc(2, instance1, instance2));
        assertNotNull(localView);
        assertNoPartiallyStarted(localView);
        assertEquals(2, localView.getInstances().size());

        // leave 2
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1));
        instance1.storeSyncToken("3");
        assertNotNull(localView);
        assertNoPartiallyStarted(localView);
        assertEquals(1, localView.getInstances().size());

        // slow join 3
        localView = instance1.getLocalClusterView(simpleDesc(4, instance1, instance2, new Identifiable(3)));
        instance1.storeSyncToken("4");
        assertNotNull(localView);
        assertEquals(2, localView.getInstances().size());
        assertPartiallyStarted(localView, 3);

        // rejoin 2
        localView = instance1.getLocalClusterView(simpleDesc(5, instance1, instance2, new Identifiable(3)));
        instance1.storeSyncToken("5");
        instance2.storeSyncToken("5");
        assertNotNull(localView);
        assertPartiallyStarted(localView, 3);
        assertEquals(2, localView.getInstances().size());
        // from 2's point of view
        localView = instance2.getLocalClusterView(simpleDesc(5, instance1, instance2, new Identifiable(3)));
        assertNotNull(localView);
        assertPartiallyStarted(localView, 3);
        assertEquals(2, localView.getInstances().size());

        MiniInstance instance3 = create(3, true, true, "6");
        localView = instance1.getLocalClusterView(simpleDesc(6, instance1, instance2, instance3));
        assertNotNull(localView);
        assertNoPartiallyStarted(localView);
        assertEquals(3, localView.getInstances().size());
    }

    @Test
    public void testRejoin_reuseClusterNodeId_newSlingId() throws Exception {
        MiniInstance instance1 = create(1, true, true, "1");
        MiniInstance instance2 = create(2, true, true, "1");

        LocalClusterView localView;
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(2, localView.getInstances().size());

        localView = instance1.getLocalClusterView(simpleDesc(2, instance1));
        assertEquals(1, localView.getInstances().size());
        instance1.storeSyncToken("2");
        localView = instance1.getLocalClusterView(simpleDesc(2, instance1));
        assertEquals(1, localView.getInstances().size());

        // instance2b starts up, reusing the clusterNodeId - but not sling level modifications yet - stealth
        MiniInstance instance2b = withNewSlingId(instance2);
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
        // storing the slingId means it is no longer considered as previously seen, hence suppression starts
        instance2b.storeSlingId();
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(1, localView.getInstances().size());
        // same with the leaderElectiOnId
        instance2b.initLeaderElectionId();
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(1, localView.getInstances().size());
        // but once the syncToken is set, it would rejoin
        instance2b.storeSyncToken("2");
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
    }

    @Test
    public void testRejoin_newClusterNodeId_reuseSlingId_slow() throws Exception {
        MiniInstance instance1 = create(1, true, true, "1");
        MiniInstance instance2 = create(2, true, true, "1");

        LocalClusterView localView;
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(2, localView.getInstances().size());

        localView = instance1.getLocalClusterView(simpleDesc(2, instance1));
        assertEquals(1, localView.getInstances().size());
        instance1.storeSyncToken("2");
        localView = instance1.getLocalClusterView(simpleDesc(2, instance1));
        assertEquals(1, localView.getInstances().size());

        // instance2b starts up, new clusterNodeId
        MiniInstance instance2b = withSlingId(new Identifiable(3), instance2.slingId);
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(1, localView.getInstances().size());
        // storing slingId already resolves things, as the leaderElectionId is still there from the old instance
        instance2b.storeSlingId();
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
        // so storing the leaderElectiOnId doesn't change anything now
        instance2b.initLeaderElectionId();
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
        // but once the syncToken is set, it would rejoin
        instance2b.storeSyncToken("3");
        localView = instance1.getLocalClusterView(simpleDesc(3, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
    }

    @Test
    public void testRejoin_newClusterNodeId_reuseSlingId_fast() throws Exception {
        MiniInstance instance1 = create(1, true, true, "1");
        MiniInstance instance2 = create(2, true, true, "1");

        LocalClusterView localView;
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(2, localView.getInstances().size());

        // instance2 silently crashes

        // instance2b starts up, new clusterNodeId, same slingId, stored immediately
        MiniInstance instance2b = withSlingId(new Identifiable(3), instance2.slingId);
        instance2b.storeSlingId();
        instance2b.initLeaderElectionId();
        localView = instance1.getLocalClusterView(simpleDesc(2, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
        instance2b.storeSyncToken("2");
        localView = instance1.getLocalClusterView(simpleDesc(2, instance1, instance2b));
        assertEquals(2, localView.getInstances().size());
    }

    @Test
    public void testCrashDuringJoin() throws Exception {
        LocalClusterView localView;

        MiniInstance instance1 = create(1, true, true, "1");
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1));
        assertEquals(1, localView.getInstances().size());

        MiniInstance instance2 = create(2, false, false, null);
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(1, localView.getInstances().size());

        MiniInstance instance3 = create(3, false, false, null);
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2, instance3));
        assertEquals(1, localView.getInstances().size());

        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(1, localView.getInstances().size());

        localView = instance1.getLocalClusterView(simpleDesc(1, instance1));
        assertEquals(1, localView.getInstances().size());
    }

    @Test
    public void testLocalInstanceNotInView() throws Exception {
        MiniInstance instance1 = create(1, true, true, "1");
        MiniInstance instance2 = create(2, true, true, "1");
        try {
            instance1.getLocalClusterView(simpleDesc(1, instance2));
            fail("should complain");
        } catch (Exception e) {
            // this empty catch is okay
        }
    }

    @Test
    public void testSuppressionTimeout() throws Exception {
        Config config = new Config() {
            @Override
            public long getClusterSyncServiceIntervalMillis() {
                return 9999;
            }

            @Override
            public boolean getSuppressPartiallyStartedInstances() {
                return true;
            }

            @Override
            public boolean getSyncTokenEnabled() {
                return true;
            }

            @Override
            public long getSuppressionTimeoutSeconds() {
                return 1;
            }
        };
        MiniInstance instance1 = new MiniInstance(1, UUID.randomUUID().toString(), nodeStore, config);
        instance1.storeSlingId();
        instance1.initLeaderElectionId();
        instance1.storeSyncToken("1");

        LocalClusterView localView;

        MiniInstance instance2 = create(2, false, false, null);

        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(1, localView.getInstances().size());
        Thread.sleep(1250);
        try {
            instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
            fail("should have failed");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance2.storeSlingId();
        try {
            instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
            fail("should have failed");
        } catch(Exception e) {
            // this empty catch is okay
        }
        instance2.initLeaderElectionId();
        localView = instance1.getLocalClusterView(simpleDesc(1, instance1, instance2));
        assertEquals(2, localView.getInstances().size());
    }
}
