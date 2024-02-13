package com.argosware.blog.lwl;

public interface Queue {
    final class ClosedException extends Exception {
        public static final ClosedException INSTANCE = new ClosedException();
    }

    void offer(int value) throws ClosedException;

    int take() throws ClosedException;

    void close();
}
