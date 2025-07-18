package com.oracle.bmc.hdfs.store;

import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.model.BmcException;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RequestCoalescerTest {
    @Test
    public void testSingleRequest() {
        RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(Duration.ofMillis(200));
        AtomicBoolean isCoalesced = new AtomicBoolean(false);
        Integer result = coalescer.performRequest("testKey1", () -> 1, isCoalesced);
        Assertions.assertThat(result).isEqualTo(1);
        Assertions.assertThat(isCoalesced.get()).isEqualTo(false);

        isCoalesced.set(false);
        result = coalescer.performRequest("testKey2", () -> 2, isCoalesced);
        Assertions.assertThat(result).isEqualTo(2);
        Assertions.assertThat(isCoalesced.get()).isEqualTo(false);

        isCoalesced.set(false);
        result = coalescer.performRequest("testKey1", () -> 3, isCoalesced);
        Assertions.assertThat(result).isEqualTo(3);
        Assertions.assertThat(isCoalesced.get()).isEqualTo(false);
    }

    @Test
    public void testConcurrentRequestsDifferentKeys() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(Duration.ofMillis(200));

            AtomicBoolean isCoalesced1 = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(2);
            Future<Integer> request1Future = executor.submit(
                    () -> coalescer.performRequest("testKey1", () -> {
                        latch.countDown();
                        Uninterruptibles.awaitUninterruptibly(latch);
                        return 1;
                    }, isCoalesced1));
            AtomicBoolean isCoalesced2 = new AtomicBoolean(false);
            Integer key2Result = coalescer.performRequest("testKey2", () -> {
                latch.countDown();
                Uninterruptibles.awaitUninterruptibly(latch);
                return 2;
            }, isCoalesced2);
            Assertions.assertThat(request1Future.get(1, TimeUnit.MINUTES)).isEqualTo(1);
            Assertions.assertThat(key2Result).isEqualTo(2);
            Assertions.assertThat(isCoalesced1.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced2.get()).isEqualTo(false);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testIfMatchSuffixInKey() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(Duration.ofMillis(200));

            AtomicBoolean isCoalesced1 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced2 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced3 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced4 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced5 = new AtomicBoolean(false);

            CountDownLatch request0StartedLatch = new CountDownLatch(1);
            CountDownLatch other5Submitted = new CountDownLatch(1);
            CountDownLatch latch = new CountDownLatch(5);
            Future<Integer> request0Future = executor.submit(
                    () -> {
                        return coalescer.performRequest("/foo/bar/baz", () -> {
                            request0StartedLatch.countDown();
                            Uninterruptibles.awaitUninterruptibly(other5Submitted);
                            return 1;}, isCoalesced1);});
            request0StartedLatch.await(1, TimeUnit.MINUTES);
            Future<Integer> request1Future = executor.submit(
                    () -> {
                        latch.countDown();
                        Uninterruptibles.awaitUninterruptibly(latch);
                        return coalescer.performRequest("/foo/", () -> 1, isCoalesced1);});

            Future<Integer> request2Future = executor.submit(
                    () -> {
                        latch.countDown();
                        Uninterruptibles.awaitUninterruptibly(latch);
                        return coalescer.performRequest("/foo/non-matching-etag", () -> 2, isCoalesced2);});
            Future<Integer> request3Future = executor.submit(
                    () -> {
                        latch.countDown();
                        Uninterruptibles.awaitUninterruptibly(latch);
                        return coalescer.performRequest("/foo/bar/baz", () -> 3, isCoalesced3);});
            Future<Integer> request4Future = executor.submit(
                    () -> {
                        latch.countDown();
                        Uninterruptibles.awaitUninterruptibly(latch);
                        return coalescer.performRequest("/foo/bar/baznon-matching-etag", () -> 4, isCoalesced4);});

            Future<Integer> request5Future = executor.submit(
                    () -> {
                        latch.countDown();
                        Uninterruptibles.awaitUninterruptibly(latch);
                        return coalescer.performRequest("/foo/bar/baz", () -> 5, isCoalesced5);});

            Thread.sleep(400);
            other5Submitted.countDown();

            Assertions.assertThat(isCoalesced1.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced2.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced4.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced3.get() || isCoalesced5.get()).isEqualTo(true);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testConcurrentRequestsSameKey() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(Duration.ofMillis(200));

            AtomicBoolean request1Completed = new AtomicBoolean();
            AtomicBoolean isCoalesced1 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced2 = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(1);
            Future<Integer> request2Future = executor.submit(
                    () -> {
                        latch.await(); // Wait for request 1 to start
                        return coalescer.performRequest("testKey", () -> {
                            Assertions.assertThat(request1Completed.get()).isTrue();
                            return 2;
                        }, isCoalesced2);
                    });
            Integer request1Result = coalescer.performRequest("testKey", () -> {
                latch.countDown();
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                request1Completed.set(true);
                return 1;
            }, isCoalesced1);
            Assertions.assertThat(request2Future.get(1, TimeUnit.MINUTES)).isEqualTo(2);
            Assertions.assertThat(request1Result).isEqualTo(1);
            Assertions.assertThat(isCoalesced1.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced2.get()).isEqualTo(false);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testMultipleWaiters() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(Duration.ofMillis(200));

            AtomicInteger request2Invocations = new AtomicInteger();
            CountDownLatch request1StartedLatch = new CountDownLatch(1);
            CountDownLatch request2Submitted = new CountDownLatch(1);

            AtomicBoolean isCoalesced = new AtomicBoolean(false);
            List<AtomicBoolean> coalescedList = java.util.stream.Stream
                    .generate(() -> new AtomicBoolean(false))
                    .limit(4)
                    .collect(Collectors.toList());
            Future<Integer> request1Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        request1StartedLatch.countDown();
                        Uninterruptibles.awaitUninterruptibly(request2Submitted);
                        return 1;
                    }, isCoalesced));
            request1StartedLatch.await(1, TimeUnit.MINUTES);
            List<Future> group2Futures = new ArrayList<>();
            for (int i = 0; i < 4; ++i) {
                final int idx = i;
                group2Futures.add(executor.submit(
                        () -> coalescer.performRequest("testKey",
                                () -> {
                                    request2Invocations.incrementAndGet();
                                    return 2;
                                }, coalescedList.get(idx))));
            }

            Thread.sleep(100);
            Assertions.assertThat(request1Future.isDone()).isFalse();
            request2Submitted.countDown();
            Assertions.assertThat(request1Future.get(1, TimeUnit.MINUTES)).isEqualTo(1);
            for (Future<Integer> future : group2Futures) {
                Assertions.assertThat(future.get(1, TimeUnit.MINUTES)).isEqualTo(2);
            }
            long trueCount = coalescedList.stream()
                    .filter(AtomicBoolean::get)
                    .count();
            Assertions.assertThat(isCoalesced.get()).isEqualTo(false);
            Assertions.assertThat(trueCount).isEqualTo(4-1);
            Assertions.assertThat(request2Invocations.get()).isEqualTo(1);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testSlowPriorRequest() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch request1StartedLatch = new CountDownLatch(1);
        CountDownLatch request1CompleteLatch = new CountDownLatch(1);
        AtomicBoolean isCoalesced1 = new AtomicBoolean(false);
        AtomicBoolean isCoalesced2 = new AtomicBoolean(false);
        try {
            Duration priorRequestWaitMillis = Duration.ofMillis(100);
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(priorRequestWaitMillis);

            Future<Integer> request1Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        request1StartedLatch.countDown();
                        // Hang the first request until the end of the test
                        Uninterruptibles.awaitUninterruptibly(request1CompleteLatch);
                        return 1;
                    }, isCoalesced1));
            request1StartedLatch.await(1, TimeUnit.MINUTES);

            long startTime = System.nanoTime();
            Future<Integer> request2Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        return 2;
                    }, isCoalesced2));

            Assertions.assertThat(request2Future.get(1, TimeUnit.MINUTES)).isEqualTo(2);
            long requestTimeNanos = System.nanoTime() - startTime;
            Assertions.assertThat(requestTimeNanos).isGreaterThanOrEqualTo(priorRequestWaitMillis.toNanos());
            Assertions.assertThat(isCoalesced1.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced2.get()).isEqualTo(false);
        } finally {
            executor.shutdown();
            request1CompleteLatch.countDown();
        }
    }

    @Test
    public void testSlowCurrentRequest() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(6);
        CountDownLatch request1StartedLatch = new CountDownLatch(1);
        CountDownLatch request1CompleteLatch = new CountDownLatch(1);
        CountDownLatch request2CompleteLatch = new CountDownLatch(1);
        try {
            AtomicInteger request3Invocations = new AtomicInteger();
            Duration priorRequestWait= Duration.ofMillis(500);
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(priorRequestWait);
            AtomicBoolean isCoalesced1 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced2 = new AtomicBoolean(false);
            List<AtomicBoolean> coalescedList = java.util.stream.Stream
                    .generate(() -> new AtomicBoolean(false))
                    .limit(3)
                    .collect(Collectors.toList());
            Future<Integer> request1Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        request1StartedLatch.countDown();
                        // Wait until the second requst is enqueued
                        Uninterruptibles.awaitUninterruptibly(request1CompleteLatch);
                        return 1;
                    }, isCoalesced1));
            request1StartedLatch.await();

            Future<Integer> request2Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        Uninterruptibles.awaitUninterruptibly(request2CompleteLatch);
                        return 2;
                    }, isCoalesced2));

            // Give request 2 time to enqueue
            Thread.sleep(100);

            long startTime = System.nanoTime();
            List<Future<Integer>> waiterFutures = new ArrayList<>();
            for (int i = 0; i < 3; ++i) {
                final int idx = i;
                waiterFutures.add(executor.submit(
                        ()-> coalescer.performRequest("testKey", () -> {
                            request3Invocations.incrementAndGet();
                            return 3;
                        }, coalescedList.get(idx))));
            }

            // Allow all the waiter to join request group 2
            Thread.sleep(100);
            // Allow request 1 to complete so that request 2 can start
            request1CompleteLatch.countDown();

            Assertions.assertThat(isCoalesced1.get()).isEqualTo(false);
            // The waiters should give up on request group two and form their own third group
            for (Future<Integer> waiterFuture : waiterFutures) {
                Assertions.assertThat(waiterFuture.get(1, TimeUnit.MINUTES)).isEqualTo(3);
            }

            long trueCount = coalescedList.stream()
                    .filter(AtomicBoolean::get)
                    .count();
            Assertions.assertThat(request3Invocations.get()).isEqualTo(1);

            // first all 3 waiters join group 2 before forming new group, but after they form the new group. one
            // waiter will exist coalescing
            Assertions.assertThat(trueCount).isEqualTo(3-1);
            // group

            long requestTimeNanos = System.nanoTime() - startTime;
            Assertions.assertThat(requestTimeNanos).isGreaterThanOrEqualTo(priorRequestWait.toNanos());

            Assertions.assertThat(request2Future.isDone()).isFalse();
            Assertions.assertThat(request1Future.isDone()).isTrue();

            request2CompleteLatch.countDown();
            Assertions.assertThat(request2Future.get(1, TimeUnit.MINUTES)).isEqualTo(2);
            Assertions.assertThat(isCoalesced2.get()).isEqualTo(false);
        } finally {
            executor.shutdown();
            request1CompleteLatch.countDown();
            request2CompleteLatch.countDown();
        }
    }

    @Test
   public void testRequestHedging() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(6);
        CountDownLatch request1StartedLatch = new CountDownLatch(1);
        CountDownLatch request1CompleteLatch = new CountDownLatch(1);
        CountDownLatch request2CompleteLatch = new CountDownLatch(1);
        CountDownLatch request3StartedLatch = new CountDownLatch(1);
        CountDownLatch request3CompleteLatch = new CountDownLatch(1);
        try {
            AtomicInteger request3Invocations = new AtomicInteger();
            Duration priorRequestWait= Duration.ofMillis(500);
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(priorRequestWait);

            AtomicBoolean isCoalesced1 = new AtomicBoolean(false);
            AtomicBoolean isCoalesced2 = new AtomicBoolean(false);
            List<AtomicBoolean> coalescedList = java.util.stream.Stream
                    .generate(() -> new AtomicBoolean(false))
                    .limit(3)
                    .collect(Collectors.toList());
            Future<Integer> request1Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        request1StartedLatch.countDown();
                        // Wait until the second requst is enqueued
                        Uninterruptibles.awaitUninterruptibly(request1CompleteLatch);
                        return 1;
                    }, isCoalesced1));
            request1StartedLatch.await();

            Future<Integer> request2Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        Uninterruptibles.awaitUninterruptibly(request2CompleteLatch);
                        return 2;
                    }, isCoalesced2));

            // Give request 2 time to enqueue
            Thread.sleep(100);

            long startTime = System.nanoTime();
            List<Future<Integer>> waiterFutures = new ArrayList<>();
            for (int i = 0; i < 3; ++i) {
                final int idx = i;
                waiterFutures.add(executor.submit(
                        ()-> coalescer.performRequest("testKey", () -> {
                            request3StartedLatch.countDown();
                            Uninterruptibles.awaitUninterruptibly(request3CompleteLatch);
                            request3Invocations.incrementAndGet();
                            return 3;
                        }, coalescedList.get(idx))));
            }

            // Allow all the waiter to join request group 2
            Thread.sleep(100);
            // Allow request 1 to complete so that request 2 can start
            request1CompleteLatch.countDown();

            // Wait for one of the threads to give up on request 2 and start a new group
            request3StartedLatch.await(1, TimeUnit.MINUTES);

            Thread.sleep(100);

            // Now let request 2 complete.
            request2CompleteLatch.countDown();
            long trueCount = coalescedList.stream()
                    .filter(AtomicBoolean::get)
                    .count();
            // first all 3 waiters join group 2 before forming new group, but after they form the new group. one
            // waiter will exist coalescing
            Assertions.assertThat(trueCount).isEqualTo(3-1);

            // Only the waiter that tried to execute group 3 should be stuck
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .until(() -> waiterFutures.stream().filter(Future::isDone).count() == waiterFutures.size() - 1);

            List<Future<Integer>> doneWaiters = waiterFutures.stream().filter(Future::isDone).collect(Collectors.toList());
            List<Future<Integer>> stuckWaiters = waiterFutures.stream().filter(f -> !f.isDone()).collect(Collectors.toList());
            Assertions.assertThat(stuckWaiters.size()).isEqualTo(1);
            for (Future<Integer> future :doneWaiters) {
                Assertions.assertThat(future.get()).isEqualTo(2);
            }

            request3CompleteLatch.countDown();
            Assertions.assertThat(stuckWaiters.get(0).get(1, TimeUnit.MINUTES)).isEqualTo(3);


            Assertions.assertThat(isCoalesced1.get()).isEqualTo(false);
            Assertions.assertThat(isCoalesced2.get()).isEqualTo(false);

            // give request 3 some time to complete
            Thread.sleep(100);
            Assertions.assertThat(request3Invocations.get()).isEqualTo(1);
        } finally {
            executor.shutdown();
            request1CompleteLatch.countDown();
            request2CompleteLatch.countDown();
            request3CompleteLatch.countDown();
        }
    }
    @Test
    public void stressTest() throws Exception {
        Map<Integer, AtomicLong> data = new HashMap<>();
        AtomicBoolean testComplete = new AtomicBoolean(false);
        int nThreads = 50;
        int nKeys = 10;
        int sleepProbabilityPercent = 10;
        int errorProbabilityPercent = 1;
        for (int i = 0; i < nKeys; ++i) {
            data.put(i, new AtomicLong(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE / 2)));
        }
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        try {
            RequestCoalescer<Integer, Long> coalescer = new RequestCoalescer<>(Duration.ofMillis(10));
            List<Future<Void>> futures = new ArrayList<>();
            for (int i = 0; i < nThreads; ++i) {
                futures.add(executor.submit(() -> {
                    Random r = new Random();
                    while (!testComplete.get()) {
                        int key = r.nextInt(nKeys);
                        AtomicLong counter = data.get(key);
                        long startValue = counter.incrementAndGet();
                        long responseValue;
                        AtomicBoolean isCoalesced = new AtomicBoolean(false);
                        try {
                            responseValue = coalescer.performRequest(key, () -> {
                                long localVal = data.get(key).get();
                                if (ThreadLocalRandom.current().nextInt(100) < errorProbabilityPercent) {
                                    throw new RuntimeException();
                                }
                                if (ThreadLocalRandom.current().nextInt(100) < sleepProbabilityPercent) {
                                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(50),
                                            TimeUnit.MILLISECONDS);
                                }
                                return localVal;
                            }, isCoalesced);
                        } catch (RuntimeException e) {
                            continue;
                        }
                        long endValue = counter.get();
                        Assertions.assertThat(responseValue).isGreaterThanOrEqualTo(startValue);
                        // This check is mostly to verify that we read the correct counter
                        Assertions.assertThat(responseValue).isLessThanOrEqualTo(endValue);
                    }
                    return null;
                }));
            }
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            testComplete.set(true);
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            testComplete.set(true);
            executor.shutdown();
        }
    }

    @Test
    public void testMultipleWaitersWithException() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            RequestCoalescer<String, Integer> coalescer = new RequestCoalescer<>(Duration.ofMillis(200));

            AtomicInteger request2Invocations = new AtomicInteger();
            CountDownLatch request1StartedLatch = new CountDownLatch(1);
            CountDownLatch request2Submitted = new CountDownLatch(1);

            AtomicBoolean isCoalesced = new AtomicBoolean(false);
            List<AtomicBoolean> coalescedList = java.util.stream.Stream
                    .generate(() -> new AtomicBoolean(false))
                    .limit(4)
                    .collect(Collectors.toList());
            Future<Integer> request1Future = executor.submit(
                    () -> coalescer.performRequest("testKey", () -> {
                        request1StartedLatch.countDown();
                        Uninterruptibles.awaitUninterruptibly(request2Submitted);
                        return 1;
                    }, isCoalesced));
            request1StartedLatch.await(1, TimeUnit.MINUTES);
            List<Future> group2Futures = new ArrayList<>();
            for (int i = 0; i < 4; ++i) {
                final int idx = i;
                group2Futures.add(executor.submit(
                        () -> coalescer.performRequest("testKey",
                                () -> {
                                    request2Invocations.incrementAndGet();
                                    throw new BmcException(503, "Service Unavailable", null, null);
                                }, coalescedList.get(idx))));
            }

            Thread.sleep(100);
            Assertions.assertThat(request1Future.isDone()).isFalse();
            request2Submitted.countDown();
            Assertions.assertThat(request1Future.get(1, TimeUnit.MINUTES)).isEqualTo(1);
            for (Future<Integer> future : group2Futures) {
                Assertions.assertThat(Assertions.catchThrowable(() -> future.get(1, TimeUnit.MINUTES)).getCause())
                        .isInstanceOf(BmcException.class);
            }
            long trueCount = coalescedList.stream()
                    .filter(AtomicBoolean::get)
                    .count();
            Assertions.assertThat(isCoalesced.get()).isEqualTo(false);
            Assertions.assertThat(trueCount).isEqualTo(4-1);
            Assertions.assertThat(request2Invocations.get()).isEqualTo(1);
        } finally {
            executor.shutdown();
        }
    }
}
