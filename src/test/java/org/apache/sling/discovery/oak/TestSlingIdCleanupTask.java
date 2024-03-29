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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.scheduler.impl.QuartzScheduler;
import org.apache.sling.commons.scheduler.impl.SchedulerServiceFactory;
import org.apache.sling.commons.threads.impl.DefaultThreadPoolManager;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.discovery.base.its.setup.VirtualInstance;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.apache.sling.discovery.commons.providers.util.ResourceHelper;
import org.apache.sling.discovery.oak.its.setup.OakTestConfig;
import org.apache.sling.discovery.oak.its.setup.OakVirtualInstanceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import junitx.util.PrivateAccessor;

public class TestSlingIdCleanupTask {

    protected static final String PROPERTY_ID_ENDPOINTS = "endpoints";

    protected static final String PROPERTY_ID_SLING_HOME_PATH = "slingHomePath";

    protected static final String PROPERTY_ID_RUNTIME = "runtimeId";

    @SuppressWarnings("all")
    class DummyConf implements SlingIdCleanupTask.Conf {

        int initialDelay;
        int interval;
        int batchSize;
        long age;

        DummyConf(int initialDelay, int interval, int batchSize, long age) {
            this.initialDelay = initialDelay;
            this.interval = interval;
            this.batchSize = batchSize;
            this.age = age;
        }

        @Override
        public Class<? extends Annotation> annotationType() {
            return null;
        }

        @Override
        public int slingid_cleanup_initial_delay() {
            return initialDelay;
        }

        @Override
        public int slingid_cleanup_interval() {
            return interval;
        }

        @Override
        public int slingid_cleanup_batchsize() {
            return batchSize;
        }

        @Override
        public long slingid_cleanup_min_creation_age() {
            return age;
        }

    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private SlingIdCleanupTask cleanupTask;

    private Scheduler scheduler;

    private ResourceResolverFactory factory;

    private Config config;

    private VirtualInstance instance;

    private Closer closer;

    private Scheduler createScheduler() throws Exception {
        try {
            return createRealScheduler();
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    private Scheduler createRealScheduler() throws Throwable {
        final BundleContext ctx = Mockito.mock(BundleContext.class);
        final Map<String, Object> props = new HashMap<>();
        final DefaultThreadPoolManager threadPoolManager = new DefaultThreadPoolManager(
                ctx, new Hashtable<String, Object>());
        final QuartzScheduler qScheduler = new QuartzScheduler();
        Scheduler scheduler = new SchedulerServiceFactory();
        PrivateAccessor.setField(qScheduler, "threadPoolManager", threadPoolManager);
        PrivateAccessor.invoke(qScheduler, "activate",
                new Class[] { BundleContext.class, Map.class },
                new Object[] { ctx, props });
        PrivateAccessor.setField(scheduler, "scheduler", qScheduler);

        closer.register(new Closeable() {

            @Override
            public void close() throws IOException {
                try {
                    PrivateAccessor.invoke(qScheduler, "deactivate",
                            new Class[] { BundleContext.class }, new Object[] { ctx });
                } catch (Throwable e) {
                    throw new IOException(e);
                }
            }

        });

        return scheduler;
    }

    @Before
    public void setUp() throws Exception {
        closer = Closer.create();
    }

    private void createCleanupTask(int initialDelayMillis, int minCreationAgeMillis)
            throws Exception {
        createCleanupTask(initialDelayMillis, 1000, 50, minCreationAgeMillis);
    }

    private void createCleanupTask(int initialDelayMillis, int intervalMillis,
            int batchSize, long minCreationAgeMillis) throws Exception {
        createCleanupTask(initialDelayMillis, intervalMillis, batchSize,
                minCreationAgeMillis, SlingIdCleanupTask.MIN_CLEANUP_DELAY_MILLIS);
    }

    private void createCleanupTask(int initialDelayMillis, int intervalMillis,
            int batchSize, long minCreationAgeMillis, long minCleanupDelayMillis)
            throws Exception {
        OakVirtualInstanceBuilder builder = (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance").newRepository("/foo/bar/", true)
                .setConnectorPingInterval(999).setConnectorPingTimeout(999);
        builder.getConfig().setSuppressPartiallyStartedInstance(true);
        instance = builder.build();
        scheduler = createScheduler();
        factory = instance.getResourceResolverFactory();
        config = new OakTestConfig();
        System.setProperty(
                SlingIdCleanupTask.SLINGID_CLEANUP_ENABLED_SYSTEM_PROPERTY_NAME, "true");
        cleanupTask = SlingIdCleanupTask.create(scheduler, factory, config,
                initialDelayMillis, intervalMillis, batchSize, minCreationAgeMillis,
                minCleanupDelayMillis);
    }

    @After
    public void tearDown() throws Exception {
        closer.close();
        if (instance != null) {
            instance.stop();
            instance = null;
        }
        System.clearProperty(
                SlingIdCleanupTask.SLINGID_CLEANUP_ENABLED_SYSTEM_PROPERTY_NAME);
        if (cleanupTask != null) {
            cleanupTask.deactivate();
            cleanupTask = null;
        }
    }

    private TopologyView newView() {
        final DefaultClusterView cluster = new DefaultClusterView(
                UUID.randomUUID().toString());
        final DummyTopologyView view = new DummyTopologyView()
                .addInstance(UUID.randomUUID().toString(), cluster, true, true);
        return view;
    }

    private TopologyEvent newInitEvent(TopologyView view) {
        return new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, view);
    }

    private TopologyEvent newChangingEvent(TopologyView oldView) {
        return new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGING, oldView, null);
    }

    private TopologyEvent newPropertiesChangedEvent(TopologyView oldView,
            TopologyView newView) {
        return new TopologyEvent(TopologyEvent.Type.PROPERTIES_CHANGED, oldView, newView);
    }

    private TopologyEvent newChangedEvent(TopologyView oldView, TopologyView newView) {
        return new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, oldView, newView);
    }

    @Test
    public void testActivatde() throws Exception {
        createCleanupTask(0, 86400000);
        cleanupTask.activate(null, null);
        cleanupTask.activate(null, new DummyConf(2, 3, 4, 5));
        assertConfigs(2, 3, 4, 5);
        cleanupTask.deactivate();
    }

    @Test
    public void testModified() throws Exception {
        createCleanupTask(0, 86400000);
        cleanupTask.modified(null, null);
        cleanupTask.modified(null, new DummyConf(3, 4, 5, 6));
        assertConfigs(3, 4, 5, 6);
        cleanupTask.modified(null, new DummyConf(4, 5, 6, 7));
        assertConfigs(4, 5, 6, 7);
    }

    @Test
    public void testMillisOf() throws Exception {
        assertEquals(-1, SlingIdCleanupTask.millisOf(null));
        assertEquals(2, SlingIdCleanupTask.millisOf(new Date(2)));
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(3));
        assertEquals(3, SlingIdCleanupTask.millisOf(cal));
    }

    @Test
    public void testNoClusterInstancesResource() throws Exception {
        createCleanupTask(0, 1, 100, 86400000);
        cleanupTask.handleTopologyEvent(newInitEvent(newView()));
        Thread.sleep(1000);
        assertEquals(1, cleanupTask.getRunCount());
    }

    @Test
    public void testNoIdMapResource() throws Exception {
        createCleanupTask(0, 1, 100, 86400000);
        createPath(config.getClusterInstancesPath());
        cleanupTask.handleTopologyEvent(newInitEvent(newView()));
        Thread.sleep(1000);
        assertEquals(1, cleanupTask.getRunCount());
    }

    @Test
    public void testNoSyncTokenResource() throws Exception {
        createCleanupTask(0, 1, 100, 86400000);
        createPath(config.getClusterInstancesPath());
        createPath(config.getIdMapPath());
        cleanupTask.handleTopologyEvent(newInitEvent(newView()));
        Thread.sleep(1000);
        assertEquals(1, cleanupTask.getRunCount());
    }

    private void assertConfigs(int expectedInitialDelay, int expectedInterval,
            int expectedBatchSize, int expectedAge) throws NoSuchFieldException {
        int initialDelayMillis = (Integer) PrivateAccessor.getField(cleanupTask,
                "initialDelayMillis");
        assertEquals(expectedInitialDelay, initialDelayMillis);
        int intervalMillis = (Integer) PrivateAccessor.getField(cleanupTask,
                "intervalMillis");
        assertEquals(expectedInterval, intervalMillis);
        int batchSize = (Integer) PrivateAccessor.getField(cleanupTask, "batchSize");
        assertEquals(expectedBatchSize, batchSize);
        long minCreationAgeMillis = (Long) PrivateAccessor.getField(cleanupTask,
                "minCreationAgeMillis");
        assertEquals(expectedAge, minCreationAgeMillis);
    }

    @Test
    public void testPropertiesChanged() throws Exception {
        createCleanupTask(0, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        TopologyView view1 = newView();
        TopologyView view2 = newView();
        TopologyEvent event = newPropertiesChangedEvent(view1, view2);
        cleanupTask.handleTopologyEvent(event);
        assertEquals(0, cleanupTask.getDeleteCount());
        Thread.sleep(500);
        assertEquals(0, cleanupTask.getDeleteCount());
    }

    private void waitForRunCount(SlingIdCleanupTask task, int expectedRunCount,
            int timeoutMillis) throws InterruptedException {
        final long start = System.currentTimeMillis();
        long diff;
        do {
            if (task.getCompletionCount() == expectedRunCount) {
                return;
            }
            Thread.sleep(50);
            diff = (start + timeoutMillis) - System.currentTimeMillis();
        } while (diff > 0);
        assertEquals("did not reach expected runcount within " + timeoutMillis + "ms",
                expectedRunCount, task.getCompletionCount());
    }

    @Test
    public void testInit() throws Exception {
        createCleanupTask(0, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        TopologyEvent event = newInitEvent(view1);
        cleanupTask.handleTopologyEvent(event);
        assertEquals(0, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(10, cleanupTask.getDeleteCount());
    }

    @Test
    public void testInit_smallBatch() throws Exception {
        createCleanupTask(0, 500, 2, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        TopologyEvent event = newInitEvent(view1);
        cleanupTask.handleTopologyEvent(event);
        assertEquals(0, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(2, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 2, 5000);
        assertEquals(4, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 3, 5000);
        assertEquals(6, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 4, 5000);
        assertEquals(8, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 5, 5000);
        assertEquals(10, cleanupTask.getDeleteCount());
    }

    @Test
    public void testChanging() throws Exception {
        createCleanupTask(1000, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        cleanupTask.handleTopologyEvent(newChangingEvent(view1));

        assertEquals(0, cleanupTask.getDeleteCount());
        Thread.sleep(500);
        assertEquals(0, cleanupTask.getDeleteCount());
    }

    @Test
    public void testNoScheduler() throws Exception {
        createCleanupTask(1000, 86400000);
        PrivateAccessor.setField(cleanupTask, "scheduler", null);
        cleanupTask.activate(null, new DummyConf(2, 3, 4, 5));
        cleanupTask.handleTopologyEvent(newChangingEvent(newView()));
        // no asserts, just tests execution without exception
        cleanupTask.handleTopologyEvent(newChangedEvent(newView(), newView()));
        // no asserts, just tests execution without exception
    }

    @Test
    public void testChanged() throws Exception {
        createCleanupTask(1000, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        cleanupTask.handleTopologyEvent(newChangingEvent(view1));
        assertEquals(0, cleanupTask.getDeleteCount());

        TopologyView view2 = newView();
        cleanupTask.handleTopologyEvent(newChangedEvent(view1, view2));
        Thread.sleep(500);
        assertEquals(0, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(10, cleanupTask.getDeleteCount());
    }

    @Test
    public void testTopologyThenPropertiesChanged() throws Exception {
        createCleanupTask(1000, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        cleanupTask.handleTopologyEvent(newChangingEvent(view1));
        assertEquals(0, cleanupTask.getDeleteCount());

        TopologyView view2 = newView();
        cleanupTask.handleTopologyEvent(newChangedEvent(view1, view2));
        // below properties changed event must not stop the cleanup
        cleanupTask.handleTopologyEvent(newPropertiesChangedEvent(view1, view2));
        Thread.sleep(500);
        assertEquals(0, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(10, cleanupTask.getDeleteCount());
    }

    @Test
    public void testRepetitionDelay() throws Exception {
        createCleanupTask(1000, 86400000);
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(10, cleanupTask.getDeleteCount());
        cleanupTask.handleTopologyEvent(newChangingEvent(view1));
        Thread.sleep(1500);
        assertEquals(1, cleanupTask.getRunCount());

        TopologyView view2 = newView();
        cleanupTask.handleTopologyEvent(newChangedEvent(view1, view2));
        Thread.sleep(1500);
        assertEquals(1, cleanupTask.getRunCount());
        assertEquals(10, cleanupTask.getDeleteCount());
    }

    @Test
    public void testDisabled() throws Exception {
        createCleanupTask(1000, 86400000);
        System.setProperty(
                SlingIdCleanupTask.SLINGID_CLEANUP_ENABLED_SYSTEM_PROPERTY_NAME, "false");
        assertEquals(0, cleanupTask.getDeleteCount());
        createSlingIds(5, 10, 0);

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        cleanupTask.handleTopologyEvent(newChangingEvent(view1));
        assertEquals(0, cleanupTask.getDeleteCount());

        TopologyView view2 = newView();
        cleanupTask.handleTopologyEvent(newChangedEvent(view1, view2));
        Thread.sleep(500);
        assertEquals(0, cleanupTask.getDeleteCount());
        Thread.sleep(1000);
        assertEquals(0, cleanupTask.getRunCount());
        assertEquals(0, cleanupTask.getDeleteCount());
    }

    /**
     * This tests the case where there are slingIds under /clusterInstances with no
     * corresponding syncToken
     */
    @Test
    public void testOrphanedClsuterInstances() throws Exception {
        createCleanupTask(1000, 86400000);
        createSlingIds(5, 10, 0, 3);

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(10, cleanupTask.getDeleteCount());
    }

    /**
     * This test the case where there are syncTokens without a corresponding slingId
     * under /clusterInstances
     */
    @Test
    public void testOrphanedSyncTokens() throws Exception {
        createCleanupTask(1000, 86400000);
        createSlingIds(5, 10, 0, 20);

        instance.dumpRepo();

        TopologyView view1 = newView();
        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(15, cleanupTask.getDeleteCount());
    }

    @Test
    public void testLeaderVsNonLeader() throws Exception {
        createCleanupTask(250, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        List<String> slingIds = createSlingIds(3, 7, 2);

        final String clusterId = UUID.randomUUID().toString();
        final DefaultClusterView remoteLeaderCluster = new DefaultClusterView(clusterId);
        final DefaultClusterView localLeaderCluster = new DefaultClusterView(clusterId);
        final DummyTopologyView remoteLeaderView = new DummyTopologyView();
        final DummyTopologyView localLeaderView = new DummyTopologyView();

        Iterator<String> it = slingIds.iterator();
        String leaderSlingId = it.next(); // first is declared leader
        String localSlignId = it.next(); // second is local
        int idx = 0;
        for (String aSlingId : slingIds) {
            remoteLeaderView.addInstance(aSlingId, remoteLeaderCluster,
                    aSlingId.equals(leaderSlingId), aSlingId.equals(localSlignId));
            localLeaderView.addInstance(aSlingId, localLeaderCluster,
                    aSlingId.equals(localSlignId), aSlingId.equals(localSlignId));
            if (++idx >= 2) {
                break;
            }
        }

        assertEquals(0, cleanupTask.getDeleteCount());
        cleanupTask.handleTopologyEvent(newInitEvent(remoteLeaderView));
        assertEquals(0, cleanupTask.getDeleteCount());
        Thread.sleep(1000);
        assertEquals(0, cleanupTask.getDeleteCount());
        cleanupTask.handleTopologyEvent(newChangingEvent(remoteLeaderView));
        assertEquals(0, cleanupTask.getDeleteCount());
        Thread.sleep(1000);
        assertEquals(0, cleanupTask.getDeleteCount());
        cleanupTask
                .handleTopologyEvent(newChangedEvent(remoteLeaderView, localLeaderView));
        assertEquals(0, cleanupTask.getDeleteCount());
        assertEquals(0, cleanupTask.getCompletionCount());
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(1, cleanupTask.getCompletionCount());
        assertEquals(5, cleanupTask.getDeleteCount());
    }

    @Test
    public void testOldSlingIdsButNowActive() throws InterruptedException, Exception {
        doTestOldSlingIdsButActive(4, 9, 7);
    }

    @Test
    public void testOldSlingIdButRecentlyActive() throws Exception, InterruptedException {
        createCleanupTask(1000, 1000, 50, 86400000, -1);
        assertEquals(0, cleanupTask.getDeleteCount());
        int currentIds = 4;
        int oldIds = 9;
        int activeIds = 7;
        int activeOldIds = Math.max(0, activeIds - currentIds);
        List<String> slingIds = createSlingIds(currentIds, oldIds, activeOldIds);

        final String clusterViewId = UUID.randomUUID().toString();
        DefaultClusterView cluster = new DefaultClusterView(clusterViewId);
        DummyTopologyView view1 = new DummyTopologyView();

        Iterator<String> it = slingIds.iterator();
        String leaderSlingId = it.next(); // first is declared leader
        String localSlignId = leaderSlingId; // and is local too
        int idx = 0;
        for (String aSlingId : slingIds) {
            view1.addInstance(aSlingId, cluster, aSlingId.equals(leaderSlingId),
                    aSlingId.equals(localSlignId));
            if (++idx >= activeIds) {
                break;
            }
        }

        cleanupTask.handleTopologyEvent(newInitEvent(view1));
        assertEquals(0, cleanupTask.getDeleteCount());
        assertEquals(0, cleanupTask.getCompletionCount());
        waitForRunCount(cleanupTask, 1, 5000);
        int expectedDeleteCount = oldIds - activeOldIds;
        assertEquals(expectedDeleteCount, cleanupTask.getDeleteCount());
        assertNoGarbageLeft(instance, config, view1, 86400000);
        idx = 0;
        for (String aSlingId : slingIds) {
            logger.info("checking idx=" + idx + ", slingId=" + aSlingId);
            if (idx < 2) {
                // those are currently active and should of course not have been deleted
                assertStatus(instance, config, aSlingId, false);
            } else if (idx < activeIds) {
                // same here, while they are not currently active, they were active during
                // leader's lifetime
                assertStatus(instance, config, aSlingId, false);
            } else {
                // for the rest: those should be deleted
                assertStatus(instance, config, aSlingId, true);
            }
            idx++;
        }

        // in addition to the above, which is the same as doTestOldSlingIdsButActive,
        // we now simulate some activeIds crashing.
        // in particular some old and some current ones.
        // the logic should be that, as long as the leader at some previous
        // time was in a topology with those now crashed active ids, it would
        // not delete them.
        // this is to avoid a race condition that could other wise happen
        // between a crash looping instance and this cleanup mechanism
        cluster = new DefaultClusterView(clusterViewId);
        DummyTopologyView view2 = new DummyTopologyView();

        it = slingIds.iterator();
        leaderSlingId = it.next(); // first is declared leader
        localSlignId = leaderSlingId; // and is local too
        idx = 0;
        for (String aSlingId : slingIds) {
            view2.addInstance(aSlingId, cluster, aSlingId.equals(leaderSlingId),
                    aSlingId.equals(localSlignId));
            if (++idx >= 2) { // let's have only 2 active instances
                break;
            }
        }
        cleanupTask.handleTopologyEvent(newChangingEvent(view1));
        assertEquals(1, cleanupTask.getCompletionCount());
        cleanupTask.handleTopologyEvent(newChangedEvent(view1, view2));
        assertEquals(expectedDeleteCount, cleanupTask.getDeleteCount());
        waitForRunCount(cleanupTask, 2, 5000);
        assertEquals(2, cleanupTask.getCompletionCount());
        // no further cleanup should have happened
        assertEquals(expectedDeleteCount, cleanupTask.getDeleteCount());
        // now check for correct non-/deletion
        assertNoGarbageLeft(instance, config, view1, 86400000);
        idx = 0;
        for (String aSlingId : slingIds) {
            if (idx < 2) {
                // those are currently active and should of course not have been deleted
                assertStatus(instance, config, aSlingId, false);
            } else if (idx < activeIds) {
                // same here, while they are not currently active, they were active during
                // leader's lifetime
                assertStatus(instance, config, aSlingId, false);
            } else {
                // for the rest: those should be deleted
                assertStatus(instance, config, aSlingId, true);
            }
            idx++;
        }
    }

    private void assertStatus(VirtualInstance i, Config c, String slingId,
            boolean deleted) throws Exception {
        ResourceResolverFactory f = i.getResourceResolverFactory();

        ResourceResolver resolver = null;
        resolver = f.getServiceResourceResolver(null);

        final Resource clusterInstances = ResourceHelper.getOrCreateResource(resolver,
                c.getClusterInstancesPath());
        final Resource idMap = ResourceHelper.getOrCreateResource(resolver,
                c.getIdMapPath());
        final Resource syncTokens = ResourceHelper.getOrCreateResource(resolver,
                c.getSyncTokenPath());
        resolver.refresh();

        if (deleted) {
            assertNull(clusterInstances.getChild(slingId));
            assertNull(idMap.getValueMap().get(slingId));
            assertNull(syncTokens.getValueMap().get(slingId));
        } else {
            assertNotNull(clusterInstances.getChild(slingId));
            assertNotNull(idMap.getValueMap().get(slingId));
            assertNotNull(syncTokens.getValueMap().get(slingId));
        }
    }

    @Test
    public void testOldSlingIdButActive_all() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 15);
    }

    @Test
    public void testOldSlingIdButActive_almostall() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 14);
    }

    @Test
    public void testOldSlingIdButActive_some() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 8);
    }

    @Test
    public void testOldSlingIdButActive_one() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 6);
    }

    @Test
    public void testOldSlingIdButActive_none() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 5);
    }

    @Test
    public void testOldSlingIdButActive_oneRecent() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 4);
    }

    @Test
    public void testOldSlingIdButActive_twoRecent() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 3);
    }

    @Test
    public void testOldSlingIdButActive_threeRecent() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 2);
    }

    @Test
    public void testOldSlingIdButActive_allRecent() throws Exception {
        doTestOldSlingIdsButActive(5, 10, 1);
    }

    private void doTestOldSlingIdsButActive(int recentIds, int oldIds, int activeIds)
            throws Exception, InterruptedException {
        logger.info(
                "doTestOldSlingIdsButActive : recentIds={}, oldIds={}, activeIds={} : START",
                recentIds, oldIds, activeIds);
        createCleanupTask(1000, 86400000);
        assertEquals(0, cleanupTask.getDeleteCount());
        List<String> slingIds = createSlingIds(recentIds, oldIds,
                Math.max(0, activeIds - recentIds));

        final DefaultClusterView cluster = new DefaultClusterView(
                UUID.randomUUID().toString());
        final DummyTopologyView view = new DummyTopologyView();

        Iterator<String> it = slingIds.iterator();
        String leaderSlingId = it.next(); // first is declared leader
        String localSlignId = leaderSlingId; // and is local too
        int idx = 0;
        for (String aSlingId : slingIds) {
            view.addInstance(aSlingId, cluster, aSlingId.equals(leaderSlingId),
                    aSlingId.equals(localSlignId));
            if (++idx >= activeIds) {
                break;
            }
        }

        cleanupTask.handleTopologyEvent(newInitEvent(view));
        assertEquals(0, cleanupTask.getDeleteCount());
        assertEquals(0, cleanupTask.getCompletionCount());
        waitForRunCount(cleanupTask, 1, 5000);
        assertEquals(Math.max(0, oldIds - Math.max(0, activeIds - recentIds)),
                cleanupTask.getDeleteCount());
        assertNoGarbageLeft(instance, config, view, 86400000);
    }

    private void assertNoGarbageLeft(VirtualInstance i, Config c,
            TopologyView currentView, long maxAgeMillis) throws Exception {
        List<InstanceDescription> instances = currentView.getLocalInstance()
                .getClusterView().getInstances();
        Set<String> activeIds = new HashSet<>();
        for (InstanceDescription id : instances) {
            activeIds.add(id.getSlingId());
        }

        ResourceResolverFactory f = i.getResourceResolverFactory();

        ResourceResolver resolver = null;
        resolver = f.getServiceResourceResolver(null);

        final Resource clusterInstances = ResourceHelper.getOrCreateResource(resolver,
                c.getClusterInstancesPath());
        final Resource idMap = ResourceHelper.getOrCreateResource(resolver,
                c.getIdMapPath());
        final Resource syncTokens = ResourceHelper.getOrCreateResource(resolver,
                c.getSyncTokenPath());
        resolver.refresh();

        final ValueMap idMapMap = idMap.adaptTo(ValueMap.class);
        final ValueMap syncTokenMap = syncTokens.adaptTo(ValueMap.class);

        for (Resource aChild : clusterInstances.getChildren()) {
            String slingId = aChild.getName();
            if (!isGarbage(aChild, maxAgeMillis)) {
                activeIds.add(slingId);
            }
        }

        Set<String> idMapSlingIds = new HashSet<>();
        for (Entry<String, Object> e : idMapMap.entrySet()) {
            String k = e.getKey();
            if (!k.contains(":") && k.contains("-")) {
                idMapSlingIds.add(k);
            }
        }
        assertCurrentSlingIds(idMapSlingIds, activeIds, "idmap");
        assertEquals("idmap size", activeIds.size(), idMapSlingIds.size());

        Set<String> syncTokenSlingIds = new HashSet<>();
        for (Entry<String, Object> e : syncTokenMap.entrySet()) {
            String k = e.getKey();
            if (!k.contains(":") && k.contains("-")) {
                syncTokenSlingIds.add(k);
            }
        }
        assertCurrentSlingIds(syncTokenSlingIds, activeIds, "syncToken");
        assertEquals("syncToken size", activeIds.size(), syncTokenSlingIds.size());
        resolver.close();
    }

    private boolean isGarbage(Resource aChild, long maxAgeMillis) {
        Calendar now = Calendar.getInstance();
        Object o = aChild.getValueMap().get("leaderElectionIdCreatedAt");
        final long leaderElectionIdCreatedAt = SlingIdCleanupTask.millisOf(o);
        if (leaderElectionIdCreatedAt <= 0) {
            // skip
            return false;
        }
        final long diffMillis = now.getTimeInMillis() - leaderElectionIdCreatedAt;
        return diffMillis > maxAgeMillis;
    }

    private void assertCurrentSlingIds(Set<String> slingIds, Set<String> activeIds,
            String context) {
        for (String aSlingId : slingIds) {
            assertTrue("[" + context + "] stored slingId " + aSlingId
                    + " not in current view", activeIds.contains(aSlingId));
        }
    }

    /** Get or create a ResourceResolver **/
    private ResourceResolver getResourceResolver() throws LoginException {
        return factory.getServiceResourceResolver(null);
    }

    /**
     * Calculate a new leaderElectionId based on the current config and system time
     */
    private String newLeaderElectionId(String slingId) {
        int maxLongLength = String.valueOf(Long.MAX_VALUE).length();
        String currentTimeMillisStr = String.format("%0" + maxLongLength + "d",
                System.currentTimeMillis());

        String prefix = String.valueOf(config.getLeaderElectionPrefix());

        final String newLeaderElectionId = prefix + "_" + currentTimeMillisStr + "_"
                + slingId;
        return newLeaderElectionId;
    }

    private boolean createSyncToken(String slingId, long seqNum) throws Exception {
        final String syncTokenPath = config.getSyncTokenPath();
        ResourceResolver resourceResolver = getResourceResolver();
        if (resourceResolver == null) {
            fail("could not login");
            return false;
        }
        final Resource resource = ResourceHelper.getOrCreateResource(resourceResolver,
                syncTokenPath);
        final ModifiableValueMap resourceMap = resource.adaptTo(ModifiableValueMap.class);
        resourceMap.put(slingId, seqNum);
        logger.info("createSyncToken: storing syncToken: {}, slingId: {}", seqNum,
                slingId);
        resourceResolver.commit();
        return true;
    }

    private void createPath(String path) throws Exception {
        ResourceResolver resourceResolver = getResourceResolver();
        if (resourceResolver == null) {
            fail("could not login");
            return;
        }

        ResourceHelper.getOrCreateResource(resourceResolver, path);
        resourceResolver.commit();
    }

    private boolean createClusterInstance(String uuid, String runtimeId,
            String slingHomePath, String endpointsAsString, Calendar jcrCreated)
            throws Exception {
        final String myClusterNodePath = config.getClusterInstancesPath() + "/" + uuid;
        ResourceResolver resourceResolver = getResourceResolver();
        if (resourceResolver == null) {
            fail("could not login");
            return false;
        }
        String newLeaderElectionId = newLeaderElectionId(uuid);

        final Resource resource = ResourceHelper.getOrCreateResource(resourceResolver,
                myClusterNodePath);
        final ModifiableValueMap resourceMap = resource.adaptTo(ModifiableValueMap.class);

        resourceMap.put(PROPERTY_ID_RUNTIME, runtimeId);
        resourceMap.put(PROPERTY_ID_SLING_HOME_PATH, slingHomePath);
        resourceMap.put(PROPERTY_ID_ENDPOINTS, endpointsAsString);

        resourceMap.put("leaderElectionId", newLeaderElectionId);
        resourceMap.put("leaderElectionIdCreatedAt", jcrCreated);

        resourceMap.put("jcr:created", jcrCreated);

        logger.info(
                "createClusterInstance: storing my runtimeId: {}, endpoints: {}, sling home path: {}, new leaderElectionId: {}, created at: {}",
                new Object[] { runtimeId, endpointsAsString, slingHomePath,
                        newLeaderElectionId, jcrCreated });
        resourceResolver.commit();
        return true;
    }

    private void fillIdMap(Map<String, Long> ids) throws Exception {
        ResourceResolver resourceResolver = getResourceResolver();
        final Resource resource = ResourceHelper.getOrCreateResource(resourceResolver,
                config.getIdMapPath());
        ModifiableValueMap idmap = resource.adaptTo(ModifiableValueMap.class);
        for (Entry<String, Long> e : ids.entrySet()) {
            idmap.put(e.getKey(), e.getValue());
        }
        resourceResolver.commit();
    }

    private List<String> createSlingIds(int currentIds, int oldIds, int activeOldIds)
            throws Exception {
        return createSlingIds(currentIds, oldIds, activeOldIds, currentIds + oldIds);
    }

    private List<String> createSlingIds(int currentIds, int oldIds, int activeOldIds,
            int numSyncTokens) throws Exception {
        final List<String> orderedIds = new LinkedList<>();
        final Map<String, Long> slingIdToClusterNodeIds = new HashMap<>();
        final Map<String, Long> slingIdToSeqNums = new HashMap<>();
        int currentSeqNum = currentIds * 2 + oldIds * 3 + 42;
        for (long i = 0; i < currentIds; i++) {
            final String uuid = UUID.randomUUID().toString();
            orderedIds.add(uuid);
            slingIdToClusterNodeIds.put(uuid, i);
            slingIdToSeqNums.put(uuid, Long.valueOf(currentSeqNum));
            createClusterInstance(uuid, UUID.randomUUID().toString(), "/a/b/c", "n/a",
                    Calendar.getInstance());
        }
        for (long i = 0; i < oldIds; i++) {
            final String uuid = UUID.randomUUID().toString();
            if (i < activeOldIds) {
                slingIdToClusterNodeIds.put(uuid, i + currentIds);
            }
            orderedIds.add(uuid);
            slingIdToSeqNums.put(uuid, Long.valueOf(i));
            final Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_YEAR, -(7 + (int) i));
            createClusterInstance(uuid, UUID.randomUUID().toString(), "/a/b/c", "n/a",
                    cal);
        }
        // idmap is created for all currentIds and active old ids
        fillIdMap(slingIdToClusterNodeIds);
        while (numSyncTokens > slingIdToSeqNums.size()) {
            slingIdToSeqNums.put(UUID.randomUUID().toString(), Long.valueOf(1));
        }
        int c = 0;
        // first make sure as many active slingIds have a syncToken as possible
        for (String activeSlingId : slingIdToClusterNodeIds.keySet()) {
            createSyncToken(activeSlingId, slingIdToSeqNums.get(activeSlingId));
            if (++c >= numSyncTokens) {
                break;
            }
        }
        if (c < numSyncTokens) {
            // only after they have been served, consider the rest, again up to
            // numSynTtokens
            for (Entry<String, Long> e : slingIdToSeqNums.entrySet()) {
                if (slingIdToClusterNodeIds.containsKey(e.getKey())) {
                    // then it was probably already added above
                    continue;
                }
                createSyncToken(e.getKey(), e.getValue());
                if (++c >= numSyncTokens) {
                    break;
                }
            }
        }
        return orderedIds;
    }
}