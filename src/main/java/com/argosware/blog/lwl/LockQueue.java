package com.argosware.blog.lwl;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class LockQueue implements Queue {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasSpace = lock.newCondition();
    private final Condition hasItems = lock.newCondition();
    private final int[] data;
    private int readIdx, size;
    private boolean closed;

    public LockQueue(int capacity) {
        this.data = new int[capacity];
    }

    @Override public void close() {
        lock.lock();
        try {
            closed = true;
            hasSpace.signalAll();
            hasItems.signalAll();
        } finally { lock.unlock(); }
    }

    @Override public boolean offer(int value) throws ClosedException {
        lock.lock();
        try {
            if (closed) {
                throw ClosedException.INSTANCE;
            } else if (size >= data.length) {
                return false;
            } else {
                data[(readIdx+size)%data.length] = value;
                ++size;
                hasItems.signal();
                return true;
            }
        } finally { lock.unlock(); }
    }

    @Override public void put(int value) throws ClosedException {
        lock.lock();
        try {
            while (size == data.length && !closed)
                hasSpace.awaitUninterruptibly();
            if (closed)
                throw ClosedException.INSTANCE;
            data[(readIdx+size)%data.length] = value;
            ++size;
            hasItems.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override public int poll(int fallback) throws ClosedException {
        lock.lock();
        try {
            if (size == 0 && !closed) {
                return fallback;
            } else if (closed) {
                throw ClosedException.INSTANCE;
            } else {
                int readIdx = this.readIdx, item = data[readIdx];
                this.readIdx = (readIdx+1)%data.length;
                --size;
                hasSpace.signal();
                return item;
            }
        } finally { lock.unlock(); }
    }

    @Override public int take() throws ClosedException {
        lock.lock();
        try {
            while (size == 0 && !closed)
                hasItems.awaitUninterruptibly();
            if (closed)
                throw ClosedException.INSTANCE;
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
