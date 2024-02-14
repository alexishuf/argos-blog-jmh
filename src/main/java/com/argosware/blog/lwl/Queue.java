package com.argosware.blog.lwl;

public interface Queue {
    final class ClosedException extends RuntimeException {
        public static final ClosedException INSTANCE = new ClosedException();
    }

    boolean offer(int value) throws ClosedException;

    int poll(int fallback) throws ClosedException;

    void put(int value) throws ClosedException;

    int take() throws ClosedException;

    void close();
}
