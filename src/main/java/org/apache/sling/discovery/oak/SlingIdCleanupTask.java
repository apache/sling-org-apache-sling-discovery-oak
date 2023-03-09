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

import static org.osgi.util.converter.Converters.standardConverter;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.scheduler.ScheduleOptions;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEvent.Type;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.discovery.TopologyView;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background task that cleans up garbage slingIds after topology changes.
 * <p>
 * A slingId is considered garbage when:
 * <ul>
 * <li>it is not in the current topology</li>
 * <li>it is not in the current idmap (where clusterNodeIds are reused hence
 * that list stays small and does not need cleanup)</li>
 * <li>its leaderElectionId was created more than 7 days ago (the
 * leaderElectionId is created at activate time of the discovery.oak bundle -
 * hence this more or less corresponds to the startup time of that
 * instance)</li>
 * </ul>
 * The garbage accumulates at the following places, where it will thus be
 * cleaned up from:
 * <ul>
 * <li>as child node under /var/discovery/oak/clusterInstances : this is the
 * most performance critical garbage</li>
 * <li>as a property key in /var/discovery/oak/syncTokens</li>
 * </ul>
 * The task by default is executed:
 * <ul>
 * <li>only on the leader</li>
 * <li>10min after a TOPOLOGY_INIT or TOPOLOGY_CHANGED event</li>
 * <li>with a maximum number of delete operations to avoid repository overload -
 * that maximum is called batchSize and is 50 by default</li>
 * <li>in subsequent intervals of 10min after the initial run, if that had to
 * stop at the batchSize of 50 deletions</li>
 * </ul>
 * All parameters mentioned above can be configured.
 */
@Component
@Designate(ocd = SlingIdCleanupTask.Conf.class)
public class SlingIdCleanupTask implements TopologyEventListener, Runnable {

    final static String SLINGID_CLEANUP_ENABLED_SYSTEM_PROPERTY_NAME = "org.apache.sling.discovery.oak.slingidcleanup.enabled";

    /** default minimal cleanup delay at 13h, to intraday load balance */
    final static long MIN_CLEANUP_DELAY_MILLIS = 46800000;

    /**
     * default age is 1 week : an instance that is not in the current topology,
     * started 1 week ago is very unlikely to still be active
     */
    private static final long DEFAULT_MIN_CREATION_AGE_MILLIS = 604800000; // 1 week

    /**
     * initial delay is 10min : after a TOPOLOGY_INIT or TOPOLOGY_CHANGED on the
     * leader, there should be a 10min delay before starting a round of cleanup.
     * This is to not add unnecessary load after a startup/change.
     */
    private static final int DEFAULT_CLEANUP_INITIAL_DELAY = 600000; // 10min

    /**
     * default cleanup interval is 10min - this is together with the batchSize to
     * lower repository load
     */
    private static final int DEFAULT_CLEANUP_INTERVAL = 600000; // 10min

    /**
     * default batch size is 50 deletions : normally there should not be much
     * garbage around anyway, so normally it's just a few, 1-5 perhaps. If there's
     * more than 50, that is probably a one-time cleanup after this feature is first
     * rolled out. That one-time cleanup can actually take a considerable amount of
     * time. So, to not overload the write load on the repository, the deletion is
     * batched into 50 at any time - with 10min delays in between. That results in
     * an average of 1 cleanup every 12 seconds, or 5 per minute, or 8640 per day,
     * for a legacy cleanup.
     */
    private static final int DEFAULT_CLEANUP_BATCH_SIZE = 50;

    /**
     * The sling.commons.scheduler name, so that it can be cancelled upon topology
     * changes.
     */
    private static final String SCHEDULE_NAME = "org.apache.sling.discovery.oak.SlingIdCleanupTask";

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Reference
    protected Scheduler scheduler;

    @Reference
    protected ResourceResolverFactory resourceResolverFactory;

    @Reference
    private Config config;

    /**
     * volatile flag to fast stop any ongoing deletion upon a change in the topology
     */
    private volatile boolean hasTopology = false;

    /**
     * volatile field to keep track of the current topology, shared between topology
     * listener and deletion
     */
    @SuppressWarnings("all")
    private volatile TopologyView currentView;

    private int initialDelayMillis = DEFAULT_CLEANUP_INITIAL_DELAY;

    private int intervalMillis = DEFAULT_CLEANUP_INTERVAL;

    private int batchSize = DEFAULT_CLEANUP_BATCH_SIZE;

    private long minCreationAgeMillis = DEFAULT_MIN_CREATION_AGE_MILLIS;

    /** test counter that increments upon every scheduler invocation */
    private AtomicInteger runCount = new AtomicInteger(0);

    /** test counter that increments upon every batch deletion */
    private AtomicInteger completionCount = new AtomicInteger(0);

    /** test counter that keeps track of actually deleted slingIds */
    private AtomicInteger deleteCount = new AtomicInteger(0);

    /**
     * flag to distinguish first from subsequent runs, as they might have different
     * scheduler delays
     */
    private volatile boolean firstRun = true;

    private long lastSuccessfulRun = -1;

    /**
     * Minimal delay after a successful cleanup round, in millis
     */
    private long minCleanupDelayMillis = MIN_CLEANUP_DELAY_MILLIS;

    /**
     * contains all slingIds ever seen by this instance - should not be a long list
     * so not a memory issue
     */
    private Set<String> seenInstances = new HashSet<>();

    @ObjectClassDefinition(name = "Apache Sling Discovery Oak SlingId Cleanup Task", description = "This task is in charge of cleaning up old SlingIds from the repository.")
    public @interface Conf {

        @AttributeDefinition(name = "Cleanup initial delay milliseconds", description = "Number of milliseconds to initially wait for the first cleanup")
        int org_apache_sling_discovery_oak_slingid_cleanup_initial_delay() default DEFAULT_CLEANUP_INITIAL_DELAY;

        @AttributeDefinition(name = "Cleanup interval milliseconds", description = "Number of milliseconds after which to do another batch of cleaning up (if necessary)")
        int org_apache_sling_discovery_oak_slingid_cleanup_interval() default DEFAULT_CLEANUP_INTERVAL;

        @AttributeDefinition(name = "Cleanup batch size", description = "Maximum number of slingIds to cleanup in one batch.")
        int org_apache_sling_discovery_oak_slingid_cleanup_batchsize() default DEFAULT_CLEANUP_BATCH_SIZE;

        @AttributeDefinition(name = "Cleanup minimum creation age", description = "Minimum number of milliseconds since the slingId was created.")
        long org_apache_sling_discovery_oak_slingid_cleanup_min_creation_age() default DEFAULT_MIN_CREATION_AGE_MILLIS;
    }

    /**
     * Test constructor
     */
    static SlingIdCleanupTask create(Scheduler scheduler, ResourceResolverFactory factory,
            Config config, int initialDelayMillis, int intervalMillis, int batchSize,
            long minCreationAgeMillis, long minCleanupDelayMillis) {
        final SlingIdCleanupTask s = new SlingIdCleanupTask();
        s.scheduler = scheduler;
        s.resourceResolverFactory = factory;
        s.config = config;
        s.minCleanupDelayMillis = minCleanupDelayMillis;
        s.config(initialDelayMillis, intervalMillis, batchSize, minCreationAgeMillis);
        return s;
    }

    @Activate
    protected void activate(final BundleContext bc, final Conf config) {
        this.modified(bc, config);
    }

    @Modified
    protected void modified(final BundleContext bc, final Conf config) {
        if (config == null) {
            return;
        }
        config(config.org_apache_sling_discovery_oak_slingid_cleanup_initial_delay(),
                config.org_apache_sling_discovery_oak_slingid_cleanup_interval(),
                config.org_apache_sling_discovery_oak_slingid_cleanup_batchsize(),
                config.org_apache_sling_discovery_oak_slingid_cleanup_min_creation_age());
    }

    @Deactivate
    protected void deactivate() {
        logger.info("deactivate : deactivated.");
        hasTopology = false;
    }

    private void config(int initialDelayMillis, int intervalMillis, int batchSize,
            long minCreationAgeMillis) {
        this.initialDelayMillis = initialDelayMillis;
        this.intervalMillis = intervalMillis;
        this.batchSize = batchSize;
        this.minCreationAgeMillis = minCreationAgeMillis;
        logger.info(
                "config: enabled = {}, initial delay milliseconds = {}, interval milliseconds = {}, batch size = {}, min creation age milliseconds = {}",
                isEnabled(), initialDelayMillis, intervalMillis, batchSize,
                minCreationAgeMillis);
    }

    @Override
    public void handleTopologyEvent(TopologyEvent event) {
        if (!isEnabled()) {
            hasTopology = false; // stops potentially ongoing deletion
            currentView = null;
            // cancel cleanup schedule
            stop();
            logger.debug("handleTopologyEvent: slingId cleanup is disabled");
            return;
        }
        final TopologyView newView = event.getNewView();
        if (newView == null || event.getType() == Type.PROPERTIES_CHANGED) {
            hasTopology = false; // stops potentially ongoing deletion
            currentView = null;
            // cancel cleanup schedule
            stop();
        } else {
            hasTopology = true;
            currentView = newView;
            for (InstanceDescription id : newView.getLocalInstance().getClusterView()
                    .getInstances()) {
                seenInstances.add(id.getSlingId());
            }
            if (newView.getLocalInstance().isLeader()) {
                // only execute on leader
                recreateSchedule();
            } else {
                // should not be necessary, but lets stop anyway on non-leaders:
                stop();
            }
        }
    }

    /**
     * Cancels a potentially previously registered cleanup schedule.
     */
    private void stop() {
        final Scheduler localScheduler = scheduler;
        if (localScheduler == null) {
            // should not happen
            logger.warn("stop: no scheduler set, giving up.");
            return;
        }
        final boolean unscheduled = localScheduler.unschedule(SCHEDULE_NAME);
        logger.debug("stop: unschedule result={}", unscheduled);
    }

    /**
     * Reads the system property that enables or disabled this tasks
     */
    private static boolean isEnabled() {
        final String systemPropertyValue = System
                .getProperty(SLINGID_CLEANUP_ENABLED_SYSTEM_PROPERTY_NAME);
        return standardConverter().convert(systemPropertyValue).defaultValue(false)
                .to(Boolean.class);
    }

    /**
     * This method can be invoked at any time to reset the schedule to do a fresh
     * round of cleanup.
     * <p>
     * This method is thread-safe : if called concurrently, the fact that
     * scheduler.schedul is synchronized works out that ultimately there will be
     * just 1 schedule active (which is what is the desired outcome).
     */
    private void recreateSchedule() {
        final Scheduler localScheduler = scheduler;
        if (localScheduler == null) {
            // should not happen
            logger.warn("recreateSchedule: no scheduler set, giving up.");
            return;
        }
        final Calendar cal = Calendar.getInstance();
        int delayMillis;
        if (firstRun) {
            delayMillis = initialDelayMillis;
        } else {
            delayMillis = intervalMillis;
        }
        cal.add(Calendar.MILLISECOND, delayMillis);
        final Date scheduledDate = cal.getTime();
        logger.debug(
                "recreateSchedule: scheduling a cleanup in {} milliseconds from now, which is: {}",
                delayMillis, scheduledDate);
        ScheduleOptions options = localScheduler.AT(scheduledDate);
        options.name(SCHEDULE_NAME);
        options.canRunConcurrently(false); // should not concurrently execute
        localScheduler.schedule(this, options);
    }

    /**
     * Invoked via sling.commons.scheduler triggered from resetCleanupSchedule(). By
     * default should get called at max every 5 minutes until cleanup is done or
     * 10min after a topology change.
     */
    @Override
    public void run() {
        if (lastSuccessfulRun > 0 && System.currentTimeMillis()
                - lastSuccessfulRun < minCleanupDelayMillis) {
            logger.debug(
                    "run: last cleanup was {} millis ago, which is less than {} millis, therefore not cleaning up yet.",
                    System.currentTimeMillis() - lastSuccessfulRun,
                    minCleanupDelayMillis);
            recreateSchedule();
            return;
        }
        runCount.incrementAndGet();
        if (!hasTopology) {
            return;
        }
        boolean mightHaveMore = true;
        try {
            mightHaveMore = cleanup();
        } catch (Exception e) {
            // upon exception just log and retry in 10min
            logger.error("run: got Exception while cleaning up slnigIds : " + e, e);
        }
        if (mightHaveMore) {
            // then continue in 10min
            recreateSchedule();
            return;
        }
        // log successful cleanup done, yes, on info
        logger.info(
                "run: slingId cleanup done, run counter = {}, delete counter = {}, completion counter = {}",
                getRunCount(), getDeleteCount(), getCompletionCount());
        lastSuccessfulRun = System.currentTimeMillis();
    }

    /**
     * Do the actual cleanup of garbage slingIds and report back with true if there
     * might be more or false if we're at the end.
     * 
     * @return true if there might be more garbage or false if we're at the end
     */
    private boolean cleanup() {
        logger.debug("cleanup: start");
        if (!isEnabled()) {
            // bit of overkill probably, as this shouldn't happen.
            // but adds to a good night's sleep.
            logger.debug("cleanup: not enabled, stopping.");
            return false;
        }

        final ResourceResolverFactory localFactory = resourceResolverFactory;
        final Config localConfig = config;
        if (localFactory == null || localConfig == null) {
            logger.warn("cleanup: cannot cleanup due to rrf={} or c={}", localFactory,
                    localConfig);
            return true;
        }
        final TopologyView localCurrentView = currentView;
        if (localCurrentView == null || !localCurrentView.isCurrent()) {
            logger.debug("cleanup : cannot cleanup as topology recently changed : {}",
                    localCurrentView);
            return true;
        }
        final Set<String> activeSlingIds = new HashSet<>();
        for (InstanceDescription id : localCurrentView.getLocalInstance().getClusterView()
                .getInstances()) {
            activeSlingIds.add(id.getSlingId());
        }

        try (ResourceResolver resolver = localFactory.getServiceResourceResolver(null)) {

            final Resource clusterInstances = resolver
                    .getResource(localConfig.getClusterInstancesPath());
            if (clusterInstances == null) {
                logger.warn("cleanup: no resource found at {}, stopping.",
                        localConfig.getClusterInstancesPath());
                return false;
            }
            final Resource idMap = resolver.getResource(localConfig.getIdMapPath());
            if (idMap == null) {
                logger.warn("cleanup: no resource found at {}, stopping.",
                        localConfig.getIdMapPath());
                return false;
            }
            final Resource syncTokens = resolver
                    .getResource(localConfig.getSyncTokenPath());
            if (syncTokens == null) {
                logger.warn("cleanup: no resource found at {}, stopping.",
                        localConfig.getSyncTokenPath());
                return false;
            }
            resolver.refresh();

            final ValueMap idMapMap = idMap.adaptTo(ValueMap.class);
            final ModifiableValueMap syncTokenMap = syncTokens
                    .adaptTo(ModifiableValueMap.class);
            final Calendar now = Calendar.getInstance();
            int removed = 0;
            boolean mightHaveMore = false;
            int localBatchSize = batchSize;
            long localMinCreationAgeMillis = minCreationAgeMillis;
            for (Resource resource : clusterInstances.getChildren()) {
                if (!topologyUnchanged(localCurrentView)) {
                    return true;
                }
                final String slingId = resource.getName();
                if (deleteIfOldSlingId(resource, slingId, syncTokenMap, idMapMap,
                        activeSlingIds, now, localMinCreationAgeMillis)) {
                    if (++removed >= localBatchSize) {
                        // we need to stop
                        mightHaveMore = true;
                        break;
                    }
                }
            }
            // if we're not already at the batch limit, check syncTokens too
            if (!mightHaveMore) {
                for (String slingId : syncTokenMap.keySet()) {
                    try {
                        UUID.fromString(slingId);
                    } catch (Exception e) {
                        // not a uuid
                        continue;
                    }
                    if (!topologyUnchanged(localCurrentView)) {
                        return true;
                    }
                    Resource resourceOrNull = clusterInstances.getChild(slingId);
                    if (deleteIfOldSlingId(resourceOrNull, slingId, syncTokenMap,
                            idMapMap, activeSlingIds, now, localMinCreationAgeMillis)) {
                        if (++removed >= localBatchSize) {
                            // we need to stop
                            mightHaveMore = true;
                            break;
                        }
                    }
                }
            }
            if (!topologyUnchanged(localCurrentView)) {
                return true;
            }
            if (removed > 0) {
                // only if we removed something we commit
                resolver.commit();
                logger.info(
                        "cleanup : removed {} old slingIds (batch size : {}), potentially has more: {}",
                        removed, localBatchSize, mightHaveMore);
                deleteCount.addAndGet(removed);
            }
            firstRun = false;
            completionCount.incrementAndGet();
            return mightHaveMore;
        } catch (LoginException e) {
            logger.error("cleanup: could not log in administratively: " + e, e);
            throw new RuntimeException("Could not log in to repository (" + e + ")", e);
        } catch (PersistenceException e) {
            logger.error("cleanup: got a PersistenceException: " + e, e);
            throw new RuntimeException(
                    "Exception while talking to repository (" + e + ")", e);
        } finally {
            logger.debug("cleanup: done.");
        }
    }

    private boolean topologyUnchanged(TopologyView localCurrentView) {
        if (!hasTopology || currentView != localCurrentView
                || !localCurrentView.isCurrent()) {
            // we got interrupted during cleanup
            // let's not commit at all then
            logger.debug(
                    "topologyUnchanged : topology changing during cleanup - not committing this time - stopping for now.");
            return false;
        } else {
            return true;
        }
    }

    static long millisOf(Object leaderElectionIdCreatedAt) {
        if (leaderElectionIdCreatedAt == null) {
            return -1;
        }
        if (leaderElectionIdCreatedAt instanceof Date) {
            final Date d = (Date) leaderElectionIdCreatedAt;
            return d.getTime();
        }
        if (leaderElectionIdCreatedAt instanceof Calendar) {
            final Calendar c = (Calendar) leaderElectionIdCreatedAt;
            return c.getTimeInMillis();
        }
        return -1;
    }

    private boolean deleteIfOldSlingId(Resource resourceOrNull, String slingId,
            ModifiableValueMap syncTokenMap, ValueMap idMapMap,
            Set<String> activeSlingIds, Calendar now, long localMinCreationAgeMillis)
            throws PersistenceException {
        logger.trace("deleteIfOldSlingId : handling slingId = {}", slingId);
        if (activeSlingIds.contains(slingId)) {
            logger.trace("deleteIfOldSlingId : slingId is currently active : {}",
                    slingId);
            return false;
        } else if (seenInstances.contains(slingId)) {
            logger.trace("deleteIfOldSlingId : slingId seen active previously : {}",
                    slingId);
            return false;
        }
        // only check in idmap and for leaderElectionId details if the clusterInstance
        // resource is there
        if (resourceOrNull != null) {
            Object clusterNodeId = idMapMap.get(slingId);
            if (clusterNodeId == null) {
                logger.trace("deleteIfOldSlingId : slingId {} not recently in use : {}",
                        slingId, clusterNodeId);
            } else {
                logger.trace("deleteIfOldSlingId : slingId {} WAS recently in use : {}",
                        slingId, clusterNodeId);
                return false;
            }
            Object o = resourceOrNull.getValueMap().get("leaderElectionIdCreatedAt");
            final long leaderElectionIdCreatedAt = millisOf(o);
            if (leaderElectionIdCreatedAt <= 0) {
                // skip
                logger.trace(
                        "deleteIfOldSlingId: resource ({}) has no or wrongly typed leaderElectionIdCreatedAt : {}",
                        resourceOrNull, o);
                return false;
            }
            final long diffMillis = now.getTimeInMillis() - leaderElectionIdCreatedAt;
            if (diffMillis <= localMinCreationAgeMillis) {
                logger.trace("deleteIfOldSlingId: not old slingId : {}", resourceOrNull);
                return false;
            }
        }
        logger.trace("deleteIfOldSlingId: deleting old slingId : {}", resourceOrNull);
        syncTokenMap.remove(slingId);
        if (resourceOrNull != null) {
            resourceOrNull.getResourceResolver().delete(resourceOrNull);
        }
        return true;
    }

    int getRunCount() {
        return runCount.get();
    }

    int getDeleteCount() {
        return deleteCount.get();
    }

    int getCompletionCount() {
        return completionCount.get();
    }
}