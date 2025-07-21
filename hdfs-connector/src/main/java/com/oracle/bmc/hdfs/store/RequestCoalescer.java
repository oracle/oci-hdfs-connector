package com.oracle.bmc.hdfs.store;

import com.google.common.util.concurrent.Uninterruptibles;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Coalesces requests for the same key. This implementation of RequestCoalescer ensures that only one request per key
 * is executed at any time (except in cases of failure or timeout). Requests for the same key from multiple threads
 * are grouped into sequential request groups based on their arrival time.
 *
 * If the previous group has not yet completed, the current group waits in line. All newly arrived requests will
 * join the current group while it is waiting.
 *
 * Once the previous group’s request completes, the first requester in the current group (the "group owner") sends
 * a single request on behalf of the group. Other threads in the same group act as subscribers and receive the
 * shared result.
 *
 * If a thread waits too long for the result it originally subscribed to, it may either:
 * 1. Form a new group to initiate the next request to the service if no new group has been created, or
 * 2. Join the next waiting group if one has already been created by a later-arriving thread.
 *
 * A waiting thread will subscribe to the results of all groups it has joined, and whichever result returns first
 * will be used as the response to its request.
 *
 * However, if a thread forms a new group and sends a request to the service, it will unsubscribe from all other
 * groups it previously joined and wait only for the response to the request it initiated.
  * @param <K> Key used to identify requests. Requests with the same key, executed at the same time should return the
 *            same response.
 * @param <R> Response type
 */
@Slf4j
public class RequestCoalescer<K, R> {
    private final long priorRequestWaitNanos;
    private final Map<K, RequestGroup<R>> requestMap = new ConcurrentHashMap<>();
    /**
     * @param priorRequestWait Maximum duration to wait for previous request to finish before executing a new request.
     */
    public RequestCoalescer(Duration priorRequestWait) {
        this.priorRequestWaitNanos = priorRequestWait.toNanos();
    }

    /*
     * The following diagram shows an example of the work flow of performRequest method:
     *     T0
     *    ----
     *     |                          |  <---------- req0(group0)
     *     T1     ----------------------------------------------------------------
     *     |                     |    |  <---------- req1(group1)
     *     |               req0  |    |  <---------- req2(group1)
     *     |                     |    |  <---------- req3(group1)
     *     T2     ---------------v---------------resp0----------------------------> resp0 (group0)
     *     |                     |    |  <---------- req4(group2)
     *     |               req1  |    |  <---------- req5(group2)
     *     |                     |    |  <---------- req6(group2)
     *     T3     ---------------v---------------resp1----------------------------> resp1,resp2,resp3 (group1)
     *     |                     |    |
     *     |               req4  |    |  <---------- req7(group3)
     *     |                     |    |  <---------- req8(group3)
     *     |                     |    |  <---------- req9(group3)
     *     |                     |    |
     *     T4     ---------------|-------req5,req6 coalescing_wait_timeout(join next group, group3)------
     *     |                     |    |  req5(group2,group3)
     *     |                     |    |  req6(group2, group3)
     *     |                     |    |  <---------- req10(group3)
     *     |                     |    |
     *     T5     ---------------|-------req7,req8,req9 coalescing_wait_timeout(start group3)-------------
     *     |         req7|       |    |  <---------- req11(group4)
     *     T6     -------|-------v---------------resp4----------------------------> resp4,resp5,resp6(group2)
     *     |             |            |  <---------- req12(group4)
     *     |             |            |
     *     |             |            |  <---------- req13(group4)
     *     |             |            |
     *     T7     -------v------------------------resp7----------------------------> resp7,resp8,resp9,resp10(group3)
     *     |                req11|    |  <---------- req14(group5)
     *     |                     |    |  <---------- req15(group5)
     *     |                     |    |  <---------- req16(group5)
     *     |                     |    |  <---------- req17(group5)
     *     T8     ---------------v----------------resp11----------------------------> resp11,resp12,resp13(group4)
     *     |                req14|    |
     *     |                     |    | (no req)
     *     |                     |    |
     *     |                     |    |
     *    T9      ---------------|----------------req15,req16,req17 coalescing_wait_timeout(req15 forms new group)-----
     *     |          req15 |    |    | req15(group6)
     *     |                |    |    | req16(group5,group6)
     *     |                |    |    | req17(group5,group6)
     *     |                |    |    |  <---------- req18(group7)
     *     T10    ----------|----v----------------resp14---------------------------->resp14,resp16, resp17 (group5)
     *     |                |         |  <---------- req19(group7)
     *     T11    ----------v---------------------resp15----------------------------->resp15(group6)
     *     |                req18|    |
     *     |                     |    |    ......
     *     v                     v    |
     */
    public R performRequest(K key, Supplier<R> request, AtomicBoolean isCoalesced) {
        for (;;) {
            RequestGroup<R> requestGroup = new RequestGroup<>();
            RequestGroup<R> previousGroup = null;
            previousGroup = requestMap.putIfAbsent(key, requestGroup);
            LOG.debug("Performing request with key {}, creating temporary requestGroup {}", key, requestGroup);
            if (previousGroup != null) {
                if (!previousGroup.started) {
                    // Coalesce with this existing request. If the request had started we wouldn't be able to use its
                    // return value because it could be out-of-date.
                    if (isCoalesced != null) {
                        isCoalesced.set(true);
                    }
                    LOG.debug("Previous group {} not started — coalescing current request", previousGroup);
                    return await(key, previousGroup, request, isCoalesced);
                }

                requestGroup.previousGroup = previousGroup;
                // Atomically replace previousGroup with our requestGroup only if no one else has changed it.
                // Ensures only one new requestGroup takes over. If replacement fails, retry the loop:
                // either to join the new group just created by another thread, or to create a new group
                // if the current one has already started (i.e., its "train" has left).
                if (!requestMap.replace(key, previousGroup, requestGroup)) {
                    continue;
                }

                // This is where coalescing starts. When we get here we know that there is an outstanding
                // request (previousGroup is not null) but we cannot use its result (the request has already
                // started). We have put our new group in the map, so that other threads coalesce with us.
                LOG.debug("Created new group {} (previous group: {})", requestGroup, previousGroup);
                waitForPriorRequestCompletion(previousGroup);
            } else {
                LOG.debug("Created new group {} with no previous group", requestGroup);
            }

            return executeRequest(requestGroup, request, key, isCoalesced);
        }
    }

    /**
     * Wait until previousGroup has completed its request, or priorRequestWaitNanos nanoseconds have passed since it
     * started execution
     */
    private void waitForPriorRequestCompletion(RequestGroup previousGroup) {
        long waitTimeNanos = priorRequestWaitNanos - (System.nanoTime() - previousGroup.startedTimeNanos);
        LOG.debug("Waiting {} ms for prior request in previousGroup {}",
                TimeUnit.NANOSECONDS.toMillis(waitTimeNanos), previousGroup);
        if (waitTimeNanos > 0) {
            try {
                Uninterruptibles.getUninterruptibly(previousGroup.responseFuture,
                        waitTimeNanos,
                        TimeUnit.NANOSECONDS);
            } catch (ExecutionException | TimeoutException e) {
                // Ignore
                LOG.debug("Exception while waiting prior request completion for previousGroup {}, exception: ",
                        previousGroup,
                        e);
            }
        }
    }

    private R executeRequest(RequestGroup requestGroup, Supplier<R> request, K key, AtomicBoolean isCoalesced) {
        LOG.debug("Executing request {} for key {} in group {}", request, key, requestGroup);
        if (isCoalesced != null) {
            isCoalesced.set(false);
        }

        try {
            requestGroup.markStarted(); // This train is leaving, don't let others join it

            R response = request.get();

            LOG.debug("Group {} received response for request {}", requestGroup, request);
            // Now that we got a response, we will give that response to all prior waiting groups that haven't
            // received a response yet. If a thread is waiting on a previous group, it must have arrived before we
            // inserted our request group into the map (otherwise it wouldn't have seen the previous group in the
            // map to wait on). Since we inserted our request group into the map before starting our request
            // execution, we are guaranteed that any waiters we give a result to here arrived before we started
            // our request.
            RequestGroup groupToSet = requestGroup;
            while (groupToSet != null) {

                LOG.debug("Group {} assigning future for group {} in executeRequest", requestGroup, groupToSet);
                groupToSet.responseFuture.complete(response);
                RequestGroup nextGroupToSet = groupToSet.previousGroup;
                groupToSet.previousGroup = null;
                groupToSet = nextGroupToSet;
            }

            return response;
        } catch (Throwable t) {
            LOG.debug("Group {} encountered exception during executeRequest ", requestGroup, t);
            requestGroup.responseFuture.completeExceptionally(t);
            // Make a best effort attempt to trim other failed requests out of the linked list of previous groups
            RequestGroup nextUnfinishedGroup = requestGroup.previousGroup;

            while (nextUnfinishedGroup != null && nextUnfinishedGroup.responseFuture.isDone()) {
                nextUnfinishedGroup = nextUnfinishedGroup.previousGroup;
                LOG.debug("Group {} executeRequest trying to set exception for group {} ", requestGroup,
                        nextUnfinishedGroup);
            }
            requestGroup.previousGroup = nextUnfinishedGroup;
            throw t;
        } finally {
            LOG.debug("Removing group {} for key {} ", requestGroup, key);
            requestMap.remove(key, requestGroup);
        }
    }

    /**
     * Wait for the result of a request execution being performed by another thread. Wait until the most recently
     * started request was started more than priorRequestWaitNanos ago, then start a new request group and execute the request.
     */
    R await(K key, RequestGroup<R> group, Supplier<R> request, AtomicBoolean isCoalesced) {
        try {
            for (;;) {
                RequestGroup currentGroup = requestMap.get(key);
                if (currentGroup == null) {
                    // The map entry will only be missing if all outstanding requests have completed, so the future
                    // will already be set
                    LOG.debug("Current group not set; waiting on its own group ({}, previous {}) for key {}",
                            group,
                            group.previousGroup,
                            key);
                    return Uninterruptibles.getUninterruptibly(group.responseFuture);
                }

                LOG.debug("Awaiting — checking group: {}, previous group: {}, currentGroup: {} for key {}", group,
                        group.previousGroup,
                        currentGroup,
                        key);
                long timeToWaitNanos = priorRequestWaitNanos;
                if (currentGroup.started) {
                    timeToWaitNanos -= System.nanoTime() - currentGroup.startedTimeNanos;
                    if (timeToWaitNanos <= 0L) {
                        RequestGroup<R> newGroup = new RequestGroup<>(currentGroup);
                        LOG.debug("Timeout exceeded for key {} — forking new request group (old: {}, new: {})",
                                key, currentGroup, newGroup);
                        if (requestMap.replace(key, currentGroup, newGroup)) {
                           LOG.debug("Forked group (old: {}, new: {}) sending request for {}...", currentGroup,
                                   newGroup,
                                   key);
                           return executeRequest(newGroup, request, key, isCoalesced);
                        }
                        continue;
                    }
                }

                try {
                    LOG.debug("Waiting {} ms for result on key {}",
                            TimeUnit.NANOSECONDS.toMillis(timeToWaitNanos), key);
                    return Uninterruptibles.getUninterruptibly(
                            group.responseFuture,
                            timeToWaitNanos,
                            TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    LOG.debug("Awaiting key {}; encountered exception while retrying group decision", key, e);
                    // Ignore, we will loop around and check if we need to start a new request
                }
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static class RequestGroup<R> {
        private volatile boolean started;
        private volatile long startedTimeNanos;
        private final CompletableFuture<R> responseFuture;
        private RequestGroup previousGroup;

        RequestGroup() {
            responseFuture = new CompletableFuture<>();
        }

        RequestGroup(RequestGroup<R> previousGroup) {
            this();
            this.previousGroup = previousGroup;
        }

        void markStarted() {
            // The ordering is important here. Threads that see started set to true expect that startedTimeNanos has
            // already been set.
            startedTimeNanos = System.nanoTime();
            started = true;
        }
    }
}
