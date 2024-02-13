package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface Queue {
    void offer(int value, @Nullable Thread currentThread);

    int take(@Nullable Thread currentThread);
}
