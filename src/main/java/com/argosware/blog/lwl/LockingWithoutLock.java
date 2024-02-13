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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Control;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static java.lang.Thread.currentThread;

@State(Scope.Benchmark)
@Threads(Threads.MAX)
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
        SPSC_UNCACHED_THREAD,
        PADDED_SPIN,
        PADDED_SPSC;
        public Queue create(int capacity) {
            return switch (this) {
                case LOCK                       -> new LockQueue(capacity);
                case SPIN                       -> new SpinQueue(capacity);
                case SPSC, SPSC_UNCACHED_THREAD -> new SPSCQueue(capacity);
                case PADDED_SPIN                -> new PaddedSpinQueue(capacity);
                case PADDED_SPSC                -> new PaddedSPSCQueue(capacity);
            };
        }
    }

    public enum Capacity {
        UNIT,
        THREADS,
        HALF_THREADS;
        public int capacityForThreads(int threads) {
            int producers = threads/2;
            return switch (this) {
                case UNIT         -> 1;
                case THREADS      -> Math.max(1, producers);
                case HALF_THREADS -> Math.max(1, producers/2);
            };
        }
    }

    @Param public Implementation implementation;
    @Param public Capacity capacity;
    private boolean cacheThread;
    private final AtomicInteger nextProducerId = new AtomicInteger();
    private final AtomicInteger nextConsumerId = new AtomicInteger();
    private final ReentrantLock lock = new ReentrantLock();
    private @MonotonicNonNull Supplier<Queue> queueFactory;
    private final List<Queue> queues = new ArrayList<>();
    private long lastIterationStart = System.nanoTime();

    private Queue queue(int id) {
        lock.lock();
        try {
            while (queues.size() <= id)
                queues.add(queueFactory.get());
            return queues.get(id);
        } finally { lock.unlock(); }
    }

    @Setup(Level.Iteration)
    public void setup(BenchmarkParams params) {
        // CPU cooldown, avoid later benchmarks being penalized by thermal throttling
        long now = System.nanoTime();
        int threads = params.getThreads();
        try {
            double ms = (now - lastIterationStart) / 1_000_000.0;
            if (threads > 1)
                ms *= 1.5; // if multithreading, CPU gets hotter
            Thread.sleep((int)Math.min(1_000, ms));
        } catch (InterruptedException ignored) { }
        cacheThread = implementation != Implementation.SPSC_UNCACHED_THREAD;
        queueFactory = () -> implementation.create(capacity.capacityForThreads(threads));
        lastIterationStart = System.nanoTime();
    }

    @Setup(Level.Trial)
    public void trialSetup() {
        try {
            Thread.sleep(1_000); //CPU cooldown
        } catch (InterruptedException ignored) {}
    }

    @State(Scope.Thread)
    public static class ProducerConsumerState {
        protected boolean cacheCurrentThread;
        protected @MonotonicNonNull Thread thread;

        public @Nullable Thread cachedCurrentThread() {
            if (cacheCurrentThread) thread = currentThread();
            return thread;
        }

        @Setup(Level.Trial) public void trialSetup(LockingWithoutLock outer) {
            cacheCurrentThread = outer.cacheThread;
        }
    }

    @State(Scope.Thread)
    public static class ProducerState extends ProducerConsumerState {
        public Queue queue;
        public int counter;

        @Setup(Level.Iteration) public void setup(LockingWithoutLock outer) {
            queue = outer.queue(outer.nextProducerId.getAndIncrement());
        }
    }

    @State(Scope.Thread)
    public static class ConsumerState extends ProducerConsumerState {
        public Queue queue;

        @Setup(Level.Iteration) public void setup(LockingWithoutLock outer) {
            queue = outer.queue(outer.nextConsumerId.getAndIncrement());
        }
    }

    @Group("balanced") @Benchmark public void produce(ProducerState s, Control jmhControl) {
        if (jmhControl.stopMeasurement)
            return; //there may be no more consumer to unblock this thread if queue is full
        s.queue.offer(s.counter++, s.cachedCurrentThread());
    }

    @Group("balanced") @Benchmark public int consume(ConsumerState s, Control jmhControl) {
        if (jmhControl.stopMeasurement)
            return 0;  // there may be no producer to feed this consumer if queue is empty
        return s.queue.take(s.cachedCurrentThread());
    }

}
