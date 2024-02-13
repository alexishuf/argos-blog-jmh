package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface Queue {
    final class ClosedException extends Exception {
        public static final ClosedException INSTANCE = new ClosedException();
    }

    void offer(int value, @Nullable Thread currentThread) throws ClosedException;

    int take(@Nullable Thread currentThread) throws ClosedException;

    void close();
}
