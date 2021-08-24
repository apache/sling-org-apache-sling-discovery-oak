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

import java.util.Date;

import org.apache.sling.commons.scheduler.Job;
import org.apache.sling.commons.scheduler.JobContext;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.spi.ClusterSyncService;
import org.apache.sling.discovery.commons.providers.spi.LocalClusterView;
import org.apache.sling.discovery.commons.providers.util.LogSilencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The JoinerDelay is an ClusterSyncService used in the sync chain and got
 * introduced as part of SLING-10489.
 * <p/>
 * With SLING-10489 new-joining instances are ignored/suppressed by existing
 * instances in the cluster as long as they are potentially only partially
 * started up. The definition of partial-vs-full is the when everything
 * is written and the consistencyService (sync) would succeed. In other words
 * it includes: lease update / idMap / leaderElectionId / syncToken.
 * It is undefined how long a startup lasts and to avoid blocking other instances
 * from operating under a well-defined topology, the notion of ignoring/suppressing
 * partially started instances has been introduced.
 * <p/>
 * Generally speaking there are the following different cases wrt changes
 * in the local cluster:
 * <ul>
 *  <li>only properties change -&gt; that is handled separately already and not interesting here</li>
 *  <li>only leaving instances -&gt; not interesting for this problem</li>
 *  <li>only joining instances: in this case, these joining instances get
 *      ignored/suppressed until they are fully started and have written all needed discovery data.
 *      Until that has happened, the existing instances don't do anything discovery-related
 *      yet, ie they don't store syncToken yet neither. Thus, the newly joined instances,
 *      once they are finished with the startup, they would have to wait for the
 *      existing ones to yet take note of their full startup, of them writing their own
 *      sync token, so that the new-joiners can see those sync tokens and finish.
 *      So this case is perfectly fine simply with ignoring/suppressing.</li>
 *  <li>some leaving, some joining instances: it is this case which is a bit more tricky:
 *      with SLING-10489 the joining instances are now ignored/suppressed until they
 *      are fully started, so upon a cluster change they don't trigger any discovery
 *      activity on the existing instances.
 *      However, because there are also some instances leaving, the existing instances
 *      will take note of a cluster change and *therefore* update the syncToken etc.
 *      In that case, we have a new situation: the cluster change has been
 *      announced in the existing instances, the existing instances wrote their new
 *      sync token, but the new-joiners are still partially starting up.
 *      Let's say now the new-joiners finish their startup, so they write down the
 *      sync token. In that very moment the following happens concurrently:
 *      (a) the new-joiners check the topology and notice that everybody else already
 *      wrote the new sync token, so they can immediately go ahead and do a TOPOLOGY_INIT.
 *      (b) the existing instances just now stop ignoring/suppressing the new-joiners
 *      and then go through the consistencyService/syncing - but before they can do that,
 *      they have to inform existing listeners with a TOPOLOGY_CHANGING. Since that
 *      might take a while, it is realistic that the new-joiner already thinks they
 *      are in the new topology *while* the existing ones haven't received
 *      a TOPOLOGY_CHANGING event yet. And voila, we have a sort of short-lived split-brain.
 *      Now usually this should really only be very short-lived, as all that is holding
 *      bak is TopologyEventListeners reacting to TOPOLOGY_CHANGING - plus then some
 *      repository writes. So all of that shouldn't take too long. But it could be a few
 *      seconds. And the aim of discovery is to provide guarantees that there are never
 *      different topologies in the , aehm .., topology.
 *      Now to fix this, we'd have to probably do another synching, which would be
 *      unfeasibly complicated.
 *      But there's a rather simple way out: we can artificially delay the new-joiners
 *      from sending their very first TOPOLOGY_INIT. That way, if that delay is
 *      bigger than the above described race-condition, things would be fine.
 *      And that's what this JoinerDelay is about : delay new-joiner's TOPOLOGY_INIT.</li>
 * </ul>
 */
public class JoinerDelay implements ClusterSyncService {

    private static final String NAME = JoinerDelay.class.getCanonicalName();

    private static enum Phase {
        IDLE,
        DELAYING,
        DONE
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final long timeoutMs;
    private final Scheduler scheduler;

    private Phase phase = Phase.IDLE;
    private Date absoluteTimeout;

    private final LogSilencer logSilencer = new LogSilencer(logger);

    public JoinerDelay(long timeoutMs, Scheduler scheduler) {
        this.timeoutMs = timeoutMs;
        this.scheduler = scheduler;
        if (timeoutMs <= 0) {
            this.phase = Phase.DONE;
            logger.info("<init> : JoinerDelay is disabled (timeoutMs=" + timeoutMs + ")");
        } else {
            logger.info("<init> : JoinerDelay is enabled with timeoutMs=" + timeoutMs);
        }
    }

    /** check whether the given view reflects a 'joining a cluster with existing instances' condition */
    private boolean joinerConditionApplies(BaseTopologyView view) {
        try {
            final ClusterView clusterView = view.getLocalInstance().getClusterView();
            if (clusterView.getInstances().size() > 1) {
                return true;
            }
            if (clusterView instanceof LocalClusterView) {
                final LocalClusterView localClusterView = (LocalClusterView) clusterView;
                if (localClusterView.hasPartiallyStartedInstances()) {
                    logSilencer.infoOrDebug("joinerConditionApplies", "joinerConditionApplies : local cluster has partially started instances - delaying");
                    return true;
                }
            }
            return false;
        } catch(Exception e) {
            logger.error("joinerConditionApplies : got Exception, ignoring JoinerDelay (log level debug shows stacktrace): " + e);
            logger.debug("joinerConditionApplies : got Exception, ignoring JoinerDelay : " + e, e);
            // false disables the JoinerDelay - so lets do that on error
            return false;
        }
    }

    private final void assertCorrectThreadPool() {
        if (!Thread.currentThread().getName().contains("sling-discovery")) {
            logger.warn("assertCorrectThreadPool : not running as part of 'discovery' thread pool."
                    + " Check configuration and ensure 'discovery' is in 'allowedPoolNames' of 'org.apache.sling.commons.scheduler'");
        }
    }

    @Override
    public void sync(BaseTopologyView view, final Runnable callback) {
        if (callback == null) {
            throw new IllegalArgumentException("callback must not be null");
        }
        // here we can be in any phase (IDLE, DELAYING, DONE)
        final boolean isPhaseDone;
        synchronized(this) {
            if (phase == Phase.IDLE) {
                if (joinerConditionApplies(view)) {
                    markDelaying();
                } else {
                    markDone();
                }
            }
            isPhaseDone = phase == Phase.DONE;
        }
        logSilencer.infoOrDebug("sync-status", "sync: isPhaseDone : " + isPhaseDone);
        if (isPhaseDone) {
            // invoke callback (outside synchronization)
            callback.run();
            return;
        }
        // here we are in phase DELAYING and absoluteTimeout is set
        cancelSync();
        final Job doContinue = new Job() {

            @Override
            public void execute(JobContext context) {
                if (context != null) {
                    // context should not be null when invoked by scheduler
                    // (but it is null within the outer sync() )
                    assertCorrectThreadPool();
                }
                markDone();
                // invoke callback (outside synchronization)
                callback.run();
            }

        };
        if (absoluteTimeout.compareTo(new Date()) < 0) {
            // invoke callback (outside synchronization)
            doContinue.execute(null);
        } else if (!scheduler.schedule(doContinue,
                scheduler.AT(absoluteTimeout).name(NAME).threadPoolName("discovery"))) {
            // then let's do it anyway
            logger.error("sync : schedule failed - ignoring JoinerDelay");
            // invoke callback (outside synchronization)
            doContinue.execute(null);
        }
    }

    @Override
    public void cancelSync() {
        try {
            scheduler.unschedule(NAME);
        } catch(Exception e) {
            logger.warn("cancelSync: got Exception (log level debug shows stacktrace): " + e);
            logger.debug("cancelSync: got Exception : " + e, e);
        }
    }

    // called within synchronized(this)
    private final void markDelaying() {
        absoluteTimeout = new Date(System.currentTimeMillis() + timeoutMs);
        phase = Phase.DELAYING;
        logger.info("markDelaying : delaying topology join");
    }

    // might be called within or without synchronzied(this), so we do it ourselves
    private final void markDone() {
        synchronized(this) {
            if (absoluteTimeout == null || phase == Phase.DONE) {
                logger.warn("markDone : already marked done: absoluteTimeout = {}, phase = {}",
                        absoluteTimeout, phase);
            }
            absoluteTimeout = null;
            phase = Phase.DONE;
            logger.info("markDone : no longer delaying topology join");
            logSilencer.reset();
        }
    }
}
