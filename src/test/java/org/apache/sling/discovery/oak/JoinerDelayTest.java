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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.scheduler.impl.QuartzScheduler;
import org.apache.sling.commons.scheduler.impl.SchedulerServiceFactory;
import org.apache.sling.commons.threads.impl.DefaultThreadPoolManager;
import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DummyTopologyView;
import org.apache.sling.discovery.commons.providers.base.DummyScheduler;
import org.apache.sling.discovery.commons.providers.spi.LocalClusterView;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;

import com.google.common.io.Closer;

import junitx.util.PrivateAccessor;

@RunWith(Parameterized.class)
public class JoinerDelayTest {

    private static enum SchedulerType {
            REAL, DUMMY
    }

    private final SchedulerType schedulerType;
    private Closer closer;

    private Scheduler scheduler;
    private BaseTopologyView view;
    private Runnable callback;
    private Semaphore callbackSemaphore;

    private Scheduler createRealScheduler() throws Throwable {
        final BundleContext ctx = Mockito.mock(BundleContext.class);
        final Map<String, Object> props = new HashMap<>();
        final DefaultThreadPoolManager threadPoolManager = new DefaultThreadPoolManager(ctx, new Hashtable<String, Object>());
        final QuartzScheduler qScheduler = new QuartzScheduler();
        Scheduler scheduler = new SchedulerServiceFactory();
        PrivateAccessor.setField(qScheduler, "threadPoolManager", threadPoolManager);
        PrivateAccessor.invoke(qScheduler, "activate", new Class[] {BundleContext.class,  Map.class}, new Object[] {ctx, props});
        PrivateAccessor.setField(scheduler, "scheduler", qScheduler);

        closer.register(new Closeable() {

            @Override
            public void close() throws IOException {
                try {
                    PrivateAccessor.invoke(qScheduler, "deactivate", new Class[] {BundleContext.class}, new Object[] {ctx});
                } catch (Throwable e) {
                    throw new IOException(e);
                }
            }

        });

        return scheduler;
    }

    private Scheduler createDummyScheduler() {
        return new DummyScheduler(true);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<SchedulerType> schedulerTypes() {
        Collection<SchedulerType> result = new ArrayList<>();
        result.add(SchedulerType.REAL);
        result.add(SchedulerType.DUMMY);
        return result;
    }

    public JoinerDelayTest(SchedulerType schedulerType) {
        this.schedulerType = schedulerType;
    }

    @Before
    public void setup() throws Throwable {
        closer = Closer.create();
        switch(schedulerType) {
        case REAL : {
            scheduler = createRealScheduler();
            break;
        }
        case DUMMY : {
            scheduler = createDummyScheduler();
            break;
        }
        default: {
            fail("unknown schedulerType : " + schedulerType);
        }
        }
        DefaultClusterView cluster = new DefaultClusterView(UUID.randomUUID().toString());
        view = new DummyTopologyView()
                .addInstance(UUID.randomUUID().toString(), cluster, true, true)
                .addInstance(UUID.randomUUID().toString(), cluster, false, false);
        callbackSemaphore = new Semaphore(0);
        callback = new Runnable() {

            @Override
            public void run() {
                callbackSemaphore.release();
            }

        };
    }

    @After
    public void teardown() throws Throwable {
        closer.close();
    }

    @Test
    public void testDummyCancelSyncCalls() throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(-1, scheduler);
        joinerDelay.cancelSync();
        joinerDelay.cancelSync();
    }

    @Test
    public void testSync_nulValues() throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(1, scheduler);
        try {
            joinerDelay.sync(null, null);
            fail("should fail");
        } catch(IllegalArgumentException e) {
            // ok
        }
        joinerDelay = new JoinerDelay(1, scheduler);
        try {
            joinerDelay.sync(view, null);
            fail("should fail");
        } catch(IllegalArgumentException e) {
            // ok
        }
        joinerDelay = new JoinerDelay(1, scheduler);
        // this one is fine as it is caught
        try {
            joinerDelay.sync(null, callback);
            fail("should fail");
        } catch(IllegalArgumentException e) {
            // ok
        }

        joinerDelay = new JoinerDelay(-1, scheduler);
        try {
            joinerDelay.sync(null, null);
            fail("should fail");
        } catch(IllegalArgumentException e) {
            // ok
        }
        joinerDelay = new JoinerDelay(-1, scheduler);
        try {
            joinerDelay.sync(view, null);
            fail("should fail");
        } catch(IllegalArgumentException e) {
            // ok
        }
        joinerDelay = new JoinerDelay(-1, scheduler);
        // this one is fine as it is caught
        try {
            joinerDelay.sync(null, callback);
            fail("should fail");
        } catch(IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testSync_withoutDelay() throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(0, scheduler);
        joinerDelay.sync(view, callback);
        assertTrue(callbackSemaphore.tryAcquire());
    }

    @Test
    public void testSync_withDelay() throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(1000, scheduler);
        joinerDelay.sync(view, callback);
        assertFalse(callbackSemaphore.tryAcquire());
        assertTrue(callbackSemaphore.tryAcquire(5, TimeUnit.SECONDS));
    }

    @Test
    public void testSync_withDelayAndReSync() throws Exception {
        doTestSync_withDelayAndReSync(1, false);
    }

    @Test
    public void testSync_withDelayAndReSyncAndCancel() throws Exception {
        doTestSync_withDelayAndReSync(1, true);
    }

    @Test
    public void testSync_withDelayAnd2ReSync() throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(1000, scheduler);
        joinerDelay.sync(view, callback);
        assertFalse(callbackSemaphore.tryAcquire());
        joinerDelay.sync(view, callback);
        assertFalse(callbackSemaphore.tryAcquire());
        assertTrue(callbackSemaphore.tryAcquire(5, TimeUnit.SECONDS));
        // callback should only be invoked once - otherwise the re-sync is not done correctly
        assertFalse(callbackSemaphore.tryAcquire(2, TimeUnit.SECONDS));
    }

    @Test
    public void testSync_withDelayAndManyReSync() throws Exception {
        doTestSync_withDelayAndReSync(50, false);
    }

    @Test
    public void testSync_withDelayAndManyReSyncAndCancel() throws Exception {
        doTestSync_withDelayAndReSync(25, true);
    }

    @Test
    public void testSync_withDelayAndMoreReSyncAndCancel() throws Exception {
        doTestSync_withDelayAndReSync(100, true);
    }

    public void doTestSync_withDelayAndReSync(int repeats, boolean cancels) throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(1000, scheduler);
        for(int i=0; i<repeats; i++) {
            if (cancels) {
                joinerDelay.cancelSync();
            }
            joinerDelay.sync(view, callback);
            assertFalse(callbackSemaphore.tryAcquire());
        }
        assertTrue(callbackSemaphore.tryAcquire(5, TimeUnit.SECONDS));
        // callback should only be invoked once - otherwise the re-sync is not done correctly
        assertFalse(callbackSemaphore.tryAcquire(2, TimeUnit.SECONDS));
    }

    @Test
    public void testSync_withLongerDelay() throws Exception {
        JoinerDelay joinerDelay = new JoinerDelay(2000, scheduler);
        joinerDelay.cancelSync();
        joinerDelay.sync(view, callback);
        joinerDelay.cancelSync();
        assertFalse(callbackSemaphore.tryAcquire());
        Thread.sleep(2500);
        assertFalse(callbackSemaphore.tryAcquire());
        joinerDelay.sync(view, callback);
        assertTrue(callbackSemaphore.tryAcquire());
    }

    @Test
    public void testSingleStartup() throws Exception {
        DefaultClusterView cluster = new DefaultClusterView(UUID.randomUUID().toString());
        view = new DummyTopologyView()
                .addInstance(UUID.randomUUID().toString(), cluster, true, true);
        JoinerDelay joinerDelay = new JoinerDelay(2000, scheduler);
        joinerDelay.sync(view, callback);
        assertTrue(callbackSemaphore.tryAcquire());
    }

    @Test
    public void testPartialJoin() throws Exception {
        final LocalClusterView cluster = new LocalClusterView(UUID.randomUUID().toString(), "any");
        view = new DummyTopologyView()
                .addInstance(UUID.randomUUID().toString(), cluster, true, true);
        cluster.setPartiallyStartedClusterNodeIds(Arrays.asList(2));

        JoinerDelay joinerDelay = new JoinerDelay(2000, scheduler);
        joinerDelay.sync(view, callback);
        assertFalse(callbackSemaphore.tryAcquire());
        Thread.sleep(2500);
        assertTrue(callbackSemaphore.tryAcquire());
    }
}
