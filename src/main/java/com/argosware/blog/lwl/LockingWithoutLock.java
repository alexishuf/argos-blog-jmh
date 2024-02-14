/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.Control;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 3)
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LockingWithoutLock {
    public enum Implementation {
        LOCK,
        SPIN,
        SPSC,
        PADDED_SPIN,
        PADDED_SPSC;
        public Queue create(int capacity) {
            return switch (this) {
                case LOCK        -> new LockQueue(capacity);
                case SPIN        -> new SpinQueue(capacity);
                case SPSC        -> new SPSCQueue(capacity);
                case PADDED_SPIN -> new PaddedSpinQueue(capacity);
                case PADDED_SPSC -> new PaddedSPSCQueue(capacity);
            };
        }
    }

    @Param public Implementation implementation;
    @Param({"1", "4", "16", "256"}) public int capacity;
    private final AtomicInteger nextPairId = new AtomicInteger();
    private final List<Queue> queues = new ArrayList<>();
    private final ExecutorService counterpartExecutor
            = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger nextThreadId = new AtomicInteger();
        private final ThreadGroup group
                = new ThreadGroup(Thread.currentThread().getThreadGroup(), "counterparts");
        @Override public Thread newThread(@NonNull Runnable r) {
            var name = "counterpart-" + nextThreadId.getAndIncrement();
            var thread = new Thread(group, r, name);
            if (thread.getPriority() != Thread.NORM_PRIORITY)
                thread.setPriority(Thread.NORM_PRIORITY);
            return thread;
        }
    });
    private long lastIterationStart = System.nanoTime();

    @Setup(Level.Trial)
    public void trialSetup() {
        queues.clear(); // sanity: should be empty already
        nextPairId.setRelease(0);
        try {
            Thread.sleep(1_000); //CPU cooldown
        } catch (InterruptedException ignored) {}
    }

    @Setup(Level.Iteration)
    public void setup(BenchmarkParams params) {
        int threads = params.getThreads();
        // close leftover queues
        queues.forEach(Queue::close);
        queues.clear();
        nextPairId.setRelease(0); // restart numbering for PairState instances
        // create queues all from the same thread
        for (int i = 0, max = threads+4; i < max; i++)
            queues.add(implementation.create(capacity));
        // CPU cooldown, avoid later benchmarks being penalized by thermal throttling
        long now    = System.nanoTime();
        try {
            double ms = (now-lastIterationStart) / 1_000_000.0;
            if (threads > 1)
                ms *= 2; // if multithreading, sleep 66%, else 50%
            Thread.sleep((int)Math.min(2_000, ms));
        } catch (InterruptedException ignored) { }
        lastIterationStart = System.nanoTime();
    }

    @TearDown(Level.Iteration) public void tearDown() {
        queues.forEach(Queue::close);
        queues.clear();
    }

    @Override public String toString() {
        return getClass().getSimpleName();
    }

    @State(Scope.Thread)
    public static class PairState implements Runnable {
        protected Blackhole bh;
        private @MonotonicNonNull Future<?> counterpartFuture;
        public Queue queue;

        @Setup(Level.Iteration) public void setup(LockingWithoutLock outer, Blackhole bh) {
            this.queue = outer.queues.get(outer.nextPairId.getAndIncrement());
            this.bh = bh;
            this.counterpartFuture = outer.counterpartExecutor.submit(this);
        }

        @TearDown(Level.Iteration) public void tearDown() {
            queue.close();
            try {
                counterpartFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Unexpected", e);
            }
        }

        @Override public void run() {
            for (int i = 0; true; i++) {
                try {
                    counterpart(i);
                } catch (Queue.ClosedException e) {
                    break;
                }
            }
        }

        protected void counterpart(int i) throws Queue.ClosedException {}
    }

    @State(Scope.Thread)
    public static class ConsumerState extends PairState {
        @Override public void counterpart(int i) throws Queue.ClosedException {
            queue.put(i);
        }
    }

    @State(Scope.Thread)
    public static class ProducerState extends PairState {
        public int counter;
        @Override public void counterpart(int i) throws Queue.ClosedException {
            bh.consume(queue.take());
        }
    }

    @Fork(value = 1)
    @Measurement(iterations = 3, time = 100, timeUnit = TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
    @Group("baseline")
    @Benchmark
    public int baseline(ProducerState s) { return s.counter++; }

    @Group("queue") @Benchmark public void put(ProducerState s) {
        try {
            s.queue.put(s.counter++);
        } catch (Queue.ClosedException ignored) {}
    }

    @Group("queue") @Benchmark public int take(ConsumerState s) {
        try {
            return s.queue.take();
        } catch (Queue.ClosedException ignored) { return 0; }
    }

    @Group("queue") @Benchmark public int poll(ConsumerState s) {
        try {
            return s.queue.poll(0);
        } catch (Queue.ClosedException ignored) { return 0; }
    }

    @Group("queue") @Benchmark public boolean offer(ProducerState s) {
        try {
            return s.queue.offer(s.counter++);
        } catch (Queue.ClosedException ignored) { return false; }
    }

}
