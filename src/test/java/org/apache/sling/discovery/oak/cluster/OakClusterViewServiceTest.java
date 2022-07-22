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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.base.commons.ClusterViewService;
import org.apache.sling.discovery.base.commons.DefaultTopologyView;
import org.apache.sling.discovery.base.commons.UndefinedClusterViewException;
import org.apache.sling.discovery.base.its.setup.VirtualInstance;
import org.apache.sling.discovery.commons.providers.base.DummyListener;
import org.apache.sling.discovery.commons.providers.spi.LocalClusterView;
import org.apache.sling.discovery.commons.providers.spi.base.IdMapService;
import org.apache.sling.discovery.commons.providers.spi.base.SyncTokenService;
import org.apache.sling.discovery.oak.JoinerDelayTest;
import org.apache.sling.discovery.oak.Log4jErrorCatcher;
import org.apache.sling.discovery.oak.its.setup.OakVirtualInstanceBuilder;
import org.apache.sling.discovery.oak.its.setup.SimulatedLeaseCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OakClusterViewServiceTest {

    enum Speed { SLOW, FAST };

    private static final boolean REUSE_CLUSTERNODEID = true;
    private static final boolean NEW_CLUSTERNODEID = false;
    private static final boolean REUSE_SLINGID = true;
    private static final boolean NEW_SLINGID = false;
    private Log4jErrorCatcher catcher;

    @Before
    public void setup() {
        catcher = Log4jErrorCatcher.installErrorCatcher(OakClusterViewService.class);
    }

    @After
    public void teardown() {
        if (catcher != null) {
            final String errorMsg = catcher.getLastErrorMsg();
            if (errorMsg != null) {
                fail("Unexpected error msg : " + errorMsg);
            }
            catcher.uninstall();
            catcher = null;
        }
    }

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

    private void doTestRejoining(boolean reuseClusterNodeId, boolean reuseSlingId, Speed speed, int expectedInstanceCount, int expectedEventCount) throws Exception {
        OakVirtualInstanceBuilder builder1 =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance1")
                .newRepository("/foo/barrio/foo/", true)
                .setConnectorPingInterval(9970)
                .setConnectorPingTimeout(9971);
        builder1.getConfig().setMinEventDelay(0);
        builder1.getConfig().setJoinerDelaySeconds(0);
        builder1.getConfig().setDiscoveryLiteCheckInterval(9974);
        builder1.getConfig().setSyncTokenEnabled(true);
        builder1.getConfig().setSuppressPartiallyStartedInstance(true);
        final VirtualInstance instance1 = builder1.build();
        ClusterViewService cvs1 = instance1.getClusterViewService();
        DummyListener listener1 = JoinerDelayTest.newDummyListener(instance1);

        // first let instance1 be by its own
        System.out.println("[A] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        Thread.sleep(1000);
        System.out.println("[B] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        Thread.sleep(1000);
        System.out.println("[C] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        assertEquals(1, listener1.countEvents());

        OakVirtualInstanceBuilder builder2 =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance2")
                .useRepositoryOf(instance1)
                .setConnectorPingInterval(9980)
                .setConnectorPingTimeout(9981);
        builder2.getConfig().setMinEventDelay(0);
        builder2.getConfig().setJoinerDelaySeconds(0);
        builder2.getConfig().setDiscoveryLiteCheckInterval(9984);
        builder2.getConfig().setSyncTokenEnabled(true);
        builder2.getConfig().setSuppressPartiallyStartedInstance(true);
        VirtualInstance instance2 = builder2.build();
        builder2.getSimulatedLeaseCollection().incSeqNum();
        ClusterViewService cvs2 = instance2.getClusterViewService();

        // join the 2 nodes
        System.out.println("[D] instance 2 heartbeat, slingId = "+ instance2.getSlingId() + ", id = " + builder2.getLease().getClusterNodeId());
        instance2.heartbeatsAndCheckView();
        System.out.println("[E] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        System.out.println("[E] instance 2 heartbeat, slingId = "+ instance2.getSlingId() + ", id = " + builder2.getLease().getClusterNodeId());
        instance2.heartbeatsAndCheckView();
        Thread.sleep(1000);
        System.out.println("[F] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        System.out.println("[G] instance 2 heartbeat, slingId = "+ instance2.getSlingId() + ", id = " + builder2.getLease().getClusterNodeId());
        instance2.heartbeatsAndCheckView();
        Thread.sleep(1000);
        System.out.println("[I] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        System.out.println("[J] instance 2 heartbeat, slingId = "+ instance2.getSlingId() + ", id = " + builder2.getLease().getClusterNodeId());
        instance2.heartbeatsAndCheckView();
        Thread.sleep(1000);
        System.out.println("[I] instance 1 heartbeat, slingId = "+ instance1.getSlingId() + ", id = " + builder1.getLease().getClusterNodeId());
        instance1.heartbeatsAndCheckView();
        System.out.println("[J] instance 2 heartbeat, slingId = "+ instance2.getSlingId() + ", id = " + builder2.getLease().getClusterNodeId());
        instance2.heartbeatsAndCheckView();
        assertEquals(3, listener1.countEvents());

        // assert the join worked as expected
        assertEquals(2, cvs1.getLocalClusterView().getInstances().size());
        assertEquals(2, cvs2.getLocalClusterView().getInstances().size());

        final OakVirtualInstanceBuilder builder2b =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance2b")
                .useRepositoryOf(instance1)
                .setConnectorPingInterval(9990)
                .setConnectorPingTimeout(9991);
        if (reuseSlingId) {
            builder2b.setSlingId(builder2.getSlingId());
        }
        if (reuseClusterNodeId) {
            final int id2 = builder2.getLease().getClusterNodeId();
            builder2b.getLease().setClusterNodeIdHint(id2);
        } else {
            final int id2 = 42;
            builder2b.getLease().setClusterNodeIdHint(id2);
        }
        builder2b.getConfig().setMinEventDelay(0);
        builder2b.getConfig().setJoinerDelaySeconds(0);
        builder2b.getConfig().setDiscoveryLiteCheckInterval(9993);
        builder2b.getConfig().setSyncTokenEnabled(true);
        builder2b.getConfig().setSuppressPartiallyStartedInstance(true);

        System.out.println();
        System.out.println(" => 2B BUILDING");
        System.out.println();

        builder2.getLease().deactivate();

        // now step 1 is to join through the leases, without yet doing the syncToken thingy
        SimulatedLeaseCollection c = builder2b.getSimulatedLeaseCollection();//new
        c.incSeqNum(2);//new
        System.out.println();
        System.out.println(" UPDATING SEQ NUM TO " + c.getSeqNum());
        System.out.println();
        builder2b.updateLease();
        System.out.println();
        System.out.println("sleep 1s");
        Thread.sleep(1000);

        // now let instance1 go through a heartbeat - without checkView
        builder1.updateLease();
        // that would have updated the lease but also gotten to read the new state
        // we do that last part explicitly here again

        // since rejoined instance2 did not yet syncToken, it should for sure not be in the view
        // but also not cause an exception
        cvs1.getLocalClusterView();

        instance1.heartbeatsAndCheckView();

        AtomicReference<Exception> lastAsyncException = new AtomicReference<>();

        final AtomicReference<VirtualInstance> instance2bRef = new AtomicReference<>();
        if (speed == Speed.FAST) {
            VirtualInstance instance2b = builder2b.build();
            instance2bRef.set(instance2b);
            instance1.heartbeatsAndCheckView();
            System.out.println();
            System.out.println(" => 2B HEARTBEAT ... " + instance2b.getSlingId());
            System.out.println();
            instance2b.heartbeatsAndCheckView();
            instance1.heartbeatsAndCheckView();
            Thread.sleep(1000);
            System.out.println();
            System.out.println(" => 2B HEARTBEAT ...");
            System.out.println();
            instance2b.heartbeatsAndCheckView();
            instance1.heartbeatsAndCheckView();
            Thread.sleep(1000);
            System.out.println();
            System.out.println(" => 2B HEARTBEAT ...");
            System.out.println();
            instance2b.heartbeatsAndCheckView();
            instance1.heartbeatsAndCheckView();
        } else {
            Runnable r2bb = new Runnable() {

                @Override
                public void run() {
                    try {
                        Thread.sleep(4000);
                        VirtualInstance instance2b = builder2b.build();
                        instance2bRef.set(instance2b);
                        instance2b.heartbeatsAndCheckView();
                        instance1.heartbeatsAndCheckView();
                        Thread.sleep(1000);
                        instance2b.heartbeatsAndCheckView();
                        instance1.heartbeatsAndCheckView();
                        Thread.sleep(1000);
                        instance2b.heartbeatsAndCheckView();
                        instance1.heartbeatsAndCheckView();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        lastAsyncException.set(e);
                    } catch (Exception e) {
                        e.printStackTrace();
                        lastAsyncException.set(e);
                    }
                }

            };
            Thread th = new Thread(r2bb);
            th.start();
        }

        instance1.heartbeatsAndCheckView();
        Thread.sleep(1000);
        instance1.heartbeatsAndCheckView();
        Thread.sleep(1000);

        System.out.println("instance 1  is " + instance1.getSlingId() + " - " + builder1.getLease().getClusterNodeId() + " - " + builder1.getLease());
        System.out.println("instance 2  is " + instance2.getSlingId() + " - " + builder2.getLease().getClusterNodeId() + " - " + builder2.getLease());
        if (speed == Speed.FAST) System.out.println("instance 2b is " + instance2bRef.get().getSlingId() + " - " + builder2b.getLease().getClusterNodeId() + " - " + builder2b.getLease());
        final Set<InstanceDescription> instances = listener1.getLastView().getInstances();
        for (InstanceDescription id : instances) {
            System.out.println("id " + id);
        }
        List<TopologyEvent> ev = listener1.getEvents();
        for (TopologyEvent e : ev) {
            System.out.println(" e = " + e);
        }
        assertEquals(expectedEventCount, listener1.countEvents());
        assertEquals(expectedInstanceCount, instances.size());

        if (speed == Speed.SLOW) {
            Thread.sleep(5000);
        }

        // and now for full heartbeating
        instance1.heartbeatsAndCheckView();
        instance2bRef.get().heartbeatsAndCheckView();
        instance1.heartbeatsAndCheckView();
        instance2bRef.get().heartbeatsAndCheckView();
        assertEquals(2, cvs1.getLocalClusterView().getInstances().size());
        assertEquals(2, instance2bRef.get().getClusterViewService().getLocalClusterView().getInstances().size());

        instance1.shutdownRepository();
    }

    // new rules
    //   R1 : new clusterNodeId joining, without all sync data => suppression
    //   R2 : existing clusterNodeId (re)joining
    //      R2.1 : same slingId but not syncToken => no suppression
    //      R2.2 : new slingId => suppression
    //      R2.3 : same slingId, different leaderElectionId => suppression

    /**
     * R2.1 : same slingId but not syncToken => no suppression
     * an AEM reusing the clusterNodeId and slingId - rejoins fast
     */
    @Test
    public void testRejoining_reuseClusterNodeId_reuseSlingId_fast() throws Exception {
        doTestRejoining(REUSE_CLUSTERNODEID, REUSE_SLINGID, Speed.FAST, 2, 5);
    }

    /**
     * R2.2 : new slingId => suppression
     * an AEM reusing the clusterNodeId but not slingId (hence a new slingId) - rejoins fast
     */
    @Test
    public void testRejoining_reuseClusterNodeId_newSlingId_fast() throws Exception {
        doTestRejoining(REUSE_CLUSTERNODEID, NEW_SLINGID, Speed.FAST, 2, 5);
    }

    /**
     * R1 : new clusterNodeId joining, without all sync data => suppression
     * an AEM reusing the slingId but not clusterNoddId  - rejoins fast
     */
    @Test
    public void testRejoining_newClusterNodeId_reuseSlingId_fast() throws Exception {
        doTestRejoining(NEW_CLUSTERNODEID, REUSE_SLINGID, Speed.FAST, 2, 5);
    }

    /**
     * R1 : new clusterNodeId joining, without all sync data => suppression
     * an AEM coming with fresh clusterNodeId and slingId - fast
     */
    @Test
    public void testRejoining_newClusterNodeId_newSlingId_fast() throws Exception {
        doTestRejoining(NEW_CLUSTERNODEID, NEW_SLINGID, Speed.FAST, 2, 5);
    }

    /**
     * R2.1 : same slingId but not syncToken => no suppression
     * an AEM reusing the clusterNodeId and slingId
     */
    @Test
    public void testRejoining_reuseClusterNodeId_reuseSlingId_slow() throws Exception {
        // expecting only 4 events (thus no_topology) due to slowness of rejoining instance
        // combined with discovery noticing this (due to missing syncToken)
        doTestRejoining(REUSE_CLUSTERNODEID, REUSE_SLINGID, Speed.SLOW, 2, 4);
    }

    /**
     * R2.2 : new slingId => suppression
     * an AEM reusing the clusterNodeId but not slingId, a new slingId - rejoins slow
     */
    @Test
    public void testRejoining_reuseClusterNodeId_newSlingId_slow() throws Exception {
        // expecting only 4 events (thus no_topology) due to slowness of rejoining instance
        // which only once it does write its slingId will be considered for suppressing, not before
        // (due to reusing clusterNodeId)
        doTestRejoining(REUSE_CLUSTERNODEID, NEW_SLINGID, Speed.SLOW, 2, 4);
    }

    /**
     * R1 : new clusterNodeId joining, without all sync data => suppression
     * an AEM reusing the slingId but not clusterNoddId but a new clusterNodeId - rejoins slow
     */
    @Test
    public void testRejoining_newClusterNodeId_reuseSlingId_slow() throws Exception {
        doTestRejoining(NEW_CLUSTERNODEID, REUSE_SLINGID, Speed.SLOW, 1/* only instance1 remaining*/,
                5 /* kicks out instance2, no new instance, but a stable topology*/);
    }

    /**
     * R1 : new clusterNodeId joining, without all sync data => suppression
     * an AEM coming with fresh clusterNodeId and slingId - rejoins slow
     */
    @Test
    public void testRejoining_newClusterNodeId_newSlingId_slow() throws Exception {
        doTestRejoining(NEW_CLUSTERNODEID, NEW_SLINGID, Speed.SLOW, 1 /* only instance1 remaining*/,
                5 /* kicks out instance2, no new instance, but a stable topology*/);
    }

    @Test
    public void testSuppression() throws Exception {
        OakVirtualInstanceBuilder builder1 =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance1")
                .newRepository("/foo/barrio/foo/", true)
                .setConnectorPingInterval(999)
                .setConnectorPingTimeout(999);
        builder1.getConfig().setMinEventDelay(99);
        builder1.getConfig().setJoinerDelaySeconds(99);
        builder1.getConfig().setDiscoveryLiteCheckInterval(99);
        builder1.getConfig().setSyncTokenEnabled(true);
        builder1.getConfig().setSuppressPartiallyStartedInstance(true);
        VirtualInstance instance1 = builder1.build();
        ClusterViewService cvs1 = instance1.getClusterViewService();

        OakVirtualInstanceBuilder builder2 =
                (OakVirtualInstanceBuilder) new OakVirtualInstanceBuilder()
                .setDebugName("instance2")
                .useRepositoryOf(instance1)
                .setConnectorPingInterval(999)
                .setConnectorPingTimeout(999);
        builder2.getConfig().setMinEventDelay(99);
        builder2.getConfig().setJoinerDelaySeconds(99);
        builder2.getConfig().setDiscoveryLiteCheckInterval(99);
        builder2.getConfig().setSyncTokenEnabled(true);
        builder2.getConfig().setSuppressPartiallyStartedInstance(true);
        VirtualInstance instance2 = builder2.build();

        instance1.heartbeatsAndCheckView();
        instance2.heartbeatsAndCheckView();
        instance1.heartbeatsAndCheckView();

        LocalClusterView view1 = cvs1.getLocalClusterView();
        assertEquals(2, view1.getInstances().size());

        // now do a switch (seqNum++) but don't update instance2's syncToken
        cvs1.getLocalClusterView();
        builder1.getSimulatedLeaseCollection().incSeqNum();
        builder1.updateLease();
        LocalClusterView view2 = cvs1.getLocalClusterView();

        final DefaultTopologyView topology = new DefaultTopologyView();
        topology.setLocalClusterView(view2);

        SyncTokenService s = builder2.getSyncTokenService();
        final AtomicBoolean syncDone = new AtomicBoolean(false);
        s.sync(topology, new Runnable() {

            @Override
            public void run() {
                syncDone.set(true);
            }

        });
        s.cancelSync();
        assertFalse(syncDone.get());
        Thread.sleep(1000);
        assertFalse(syncDone.get());
        assertFalse(view2.hasPartiallyStartedInstances());
    }
}
