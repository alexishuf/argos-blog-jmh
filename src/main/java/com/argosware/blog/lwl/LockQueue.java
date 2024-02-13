package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class LockQueue implements Queue {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasSpace = lock.newCondition();
    private final Condition hasItems = lock.newCondition();
    private final int[] data;
    private int readIdx, size;

    public LockQueue(int capacity) {
        this.data = new int[capacity];
    }

    @Override public void offer(int value, @Nullable Thread currentThread) {
        lock.lock();
        try {
            while (size == data.length)
                hasSpace.awaitUninterruptibly();
            data[(readIdx+size)%data.length] = value;
            ++size;
            hasItems.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override public int take(@Nullable Thread currentThread) {
        lock.lock();
        try {
            while (size == 0)
                hasItems.awaitUninterruptibly();
            int readIdx = this.readIdx, item = data[readIdx];
            this.readIdx = (readIdx+1)%data.length;
            --size;
            hasSpace.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }
}
