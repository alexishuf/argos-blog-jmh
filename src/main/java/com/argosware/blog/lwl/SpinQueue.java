package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class SpinQueue implements Queue {
    private static final VarHandle LOCK;

    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(SpinQueue.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private int plainLock;
    private final int[] data;
    private int readIdx, size;

    public SpinQueue(int capacity) {
        this.data = new int[capacity];
    }

    @Override public void offer(int value, @Nullable Thread currentThread) {
        while (true) {
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            try {
                if (size < data.length) {
                    data[(readIdx+size)%data.length] = value;
                    ++size;
                    break;
                }
            } finally {
                LOCK.setRelease(this, 0);
            }
        }
    }

    @Override public int take(@Nullable Thread currentThread) {
        while (true) {
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            try {
                if (size > 0) {
                    int readIdx = this.readIdx, item = data[readIdx];
                    this.readIdx = (readIdx+1)%data.length;
                    --size;
                    return item;
                }
            } finally {
                LOCK.setRelease(this, 0);
            }
        }
    }
}
