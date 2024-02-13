package com.argosware.blog.lwl;

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
    private boolean closed;

    public SPSCQueue(int capacity) {
        this.data = new int[capacity];
    }

    @Override public void close() {
        Thread consumer = null, producer = null;
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.onSpinWait();
        try {
            closed = true;
            consumer = this.consumer;
            producer = this.producer;
            this.consumer = null;
            this.producer = null;
        } finally {
            LOCK.setRelease(this, 0);
            LockSupport.unpark(consumer);
            LockSupport.unpark(producer);
        }
    }

    @Override public boolean offer(int value) throws ClosedException {
        Thread unpark = null;
        while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.onSpinWait();
        try {
            if (closed) {
                throw ClosedException.INSTANCE;
            } else if (this.size >= data.length) {
                return false;
            } else {
                data[(readIdx+size)%data.length] = value;
                ++size;
                unpark = consumer;
                consumer = null;
                return true;
            }
        } finally {
            LOCK.setRelease(this, 0);
            LockSupport.unpark(unpark);
        }
    }

    @Override public void put(int value) throws ClosedException {
        while (true) {
            Thread unpark = null;
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            boolean locked = true;
            try {
                if (closed) {
                    throw ClosedException.INSTANCE;
                } else if (this.size >= data.length) {
                    if (producer == null) {
                        producer = currentThread();
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

    @Override public int poll(int fallback) throws ClosedException {
        Thread unpark = null;
        while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.onSpinWait();
        try {
            if (closed) {
                throw ClosedException.INSTANCE;
            } else if (size == 0) {
                return fallback;
            } else {
                int readIdx = this.readIdx, item = data[readIdx];
                this.readIdx = (readIdx+1)%data.length;
                --size;
                unpark = producer;
                producer = null;
                return item;
            }
        } finally {
            LOCK.setRelease(this, 0);
            LockSupport.unpark(unpark);
        }
    }

    @Override public int take() throws ClosedException {
        while (true) {
            Thread unpark = null;
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            boolean locked = true;
            try {
                if (closed) {
                    throw ClosedException.INSTANCE;
                } else if (size == 0) {
                    if (consumer == null) {
                        consumer = currentThread();
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
