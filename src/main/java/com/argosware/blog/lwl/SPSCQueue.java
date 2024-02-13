package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.currentThread;

class SPSCQueue implements Queue {
    private static final VarHandle LOCK;

    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(SPSCQueue.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private int plainLock;
    private final int[] data;
    private Thread consumer, producer;
    private int readIdx, size;

    public SPSCQueue(int capacity) {
        this.data = new int[capacity];
    }

    @Override public void offer(int value, @Nullable Thread currentThread) {
        while (true) {
            Thread unpark = null;
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            boolean locked = true;
            try {
                if (this.size >= data.length) {
                    if (producer == null) {
                        if (currentThread == null)
                            currentThread = currentThread();
                        producer = currentThread;
                        LOCK.setRelease(this, 0);
                        locked = false;
                        LockSupport.park();
                    }
                } else {
                    data[(readIdx+size)%data.length] = value;
                    ++size;
                    unpark = consumer;
                    consumer = null;
                    break;
                }
            } finally {
                if (locked)
                    LOCK.setRelease(this, 0);
                LockSupport.unpark(unpark);
            }
        }
    }

    @Override public int take(@Nullable Thread currentThread) {
        while (true) {
            Thread unpark = null;
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            boolean locked = true;
            try {
                if (size == 0) {
                    if (consumer == null) {
                        if (currentThread == null)
                            currentThread = currentThread();
                        consumer = currentThread;
                        LOCK.setRelease(this, 0);
                        locked = false;
                        LockSupport.park();
                    }
                } else {
                    int readIdx = this.readIdx, item = data[readIdx];
                    this.readIdx = (readIdx+1)%data.length;
                    --size;
                    unpark = producer;
                    producer = null;
                    return item;
                }
            } finally {
                if (locked)
                    LOCK.setRelease(this, 0);
                LockSupport.unpark(unpark);
            }
        }
    }
}
