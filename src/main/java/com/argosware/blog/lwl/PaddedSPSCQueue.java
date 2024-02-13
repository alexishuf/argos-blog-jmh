package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.currentThread;

public class PaddedSPSCQueue extends PaddedSPSCQueueL3 implements Queue {
    public PaddedSPSCQueue(int capacity) { super(capacity); }

    @Override public void offer(int value, @Nullable Thread currentThread) {
        while (true) {
            Thread unpark = null;
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            boolean locked = true;
            try {
                if (this.size >= capacity) {
                    if (producer == null) {
                        if (currentThread == null)
                            currentThread = currentThread();
                        producer = currentThread;
                        LOCK.setRelease(this, 0);
                        locked = false;
                        LockSupport.park();
                    }
                } else {
                    data[DATA_OFF+(readIdx+size)%capacity] = value;
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
                    int readIdx = this.readIdx, item = data[DATA_OFF+readIdx];
                    this.readIdx = (readIdx+1)%capacity;
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

@SuppressWarnings("unused")
abstract class PaddedSPSCQueueL0 {
    private   static final int DATA_PADD = 128/4;
    protected static final int DATA_OFF = DATA_PADD-(16/4); // use array object header
    protected final int[] data;
    protected final int capacity;

    public PaddedSPSCQueueL0(int capacity) {
        data = new int[DATA_OFF+capacity+DATA_PADD];
        this.capacity = capacity;
    }
}
@SuppressWarnings("unused") abstract class PaddedSPSCQueueL1 extends PaddedSPSCQueueL0 {
    private byte b00_0, b00_1, b00_2, b00_3, b00_4, b00_5, b00_6, b00_7; // 8  bytes
    private byte b01_0, b01_1, b01_2, b01_3, b01_4, b01_5, b01_6, b01_7; // 16 bytes
    private byte b02_0, b02_1, b02_2, b02_3, b02_4, b02_5, b02_6, b02_7; // 24 bytes
    private byte b03_0, b03_1, b03_2, b03_3, b03_4, b03_5, b03_6, b03_7; // 32 bytes
    private byte b04_0, b04_1, b04_2, b04_3, b04_4, b04_5, b04_6, b04_7; // 48 bytes
    private byte b05_0, b05_1, b05_2, b05_3, b05_4, b05_5, b05_6, b05_7; // 64 bytes
    private byte b06_0, b06_1, b06_2, b06_3, b06_4, b06_5, b06_6, b06_7; // 72 bytes
    private byte b07_0, b07_1, b07_2, b07_3, b07_4, b07_5, b07_6, b07_7; // 80 bytes
    private byte b08_0, b08_1, b08_2, b08_3, b08_4, b08_5, b08_6, b08_7; // 88 bytes
    private byte b09_0, b09_1, b09_2, b09_3, b09_4, b09_5, b09_6, b09_7; // 96 bytes
    private byte b10_0, b10_1, b10_2, b10_3, b10_4, b10_5, b10_6, b10_7; // 104 bytes
    private byte b11_0, b11_1, b11_2, b11_3, b11_4, b11_5, b11_6, b11_7; // 112 bytes
    private byte b12_0, b12_1, b12_2, b12_3, b12_4, b12_5, b12_6, b12_7; // 120 bytes
    private byte b13_0, b13_1, b13_2, b13_3, b13_4, b13_5, b13_6, b13_7; // 128 bytes

    public PaddedSPSCQueueL1(int capacity) {super(capacity);}
}
abstract class PaddedSPSCQueueL2 extends PaddedSPSCQueueL1 {
    protected static final VarHandle LOCK;
    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(PaddedSPSCQueueL2.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused") protected int plainLock;
    protected Thread consumer, producer;
    protected int readIdx, size;

    public PaddedSPSCQueueL2(int capacity) {super(capacity);}
}
@SuppressWarnings("unused") abstract class PaddedSPSCQueueL3 extends PaddedSPSCQueueL2 {
    private byte b00_0, b00_1, b00_2, b00_3, b00_4, b00_5, b00_6, b00_7; // 8  bytes
    private byte b01_0, b01_1, b01_2, b01_3, b01_4, b01_5, b01_6, b01_7; // 16 bytes
    private byte b02_0, b02_1, b02_2, b02_3, b02_4, b02_5, b02_6, b02_7; // 24 bytes
    private byte b03_0, b03_1, b03_2, b03_3, b03_4, b03_5, b03_6, b03_7; // 32 bytes
    private byte b04_0, b04_1, b04_2, b04_3, b04_4, b04_5, b04_6, b04_7; // 48 bytes
    private byte b05_0, b05_1, b05_2, b05_3, b05_4, b05_5, b05_6, b05_7; // 64 bytes
    private byte b06_0, b06_1, b06_2, b06_3, b06_4, b06_5, b06_6, b06_7; // 72 bytes
    private byte b07_0, b07_1, b07_2, b07_3, b07_4, b07_5, b07_6, b07_7; // 80 bytes
    private byte b08_0, b08_1, b08_2, b08_3, b08_4, b08_5, b08_6, b08_7; // 88 bytes
    private byte b09_0, b09_1, b09_2, b09_3, b09_4, b09_5, b09_6, b09_7; // 96 bytes
    private byte b10_0, b10_1, b10_2, b10_3, b10_4, b10_5, b10_6, b10_7; // 104 bytes
    private byte b11_0, b11_1, b11_2, b11_3, b11_4, b11_5, b11_6, b11_7; // 112 bytes
    private byte b12_0, b12_1, b12_2, b12_3, b12_4, b12_5, b12_6, b12_7; // 120 bytes
    private byte b13_0, b13_1, b13_2, b13_3, b13_4, b13_5, b13_6, b13_7; // 128 bytes

    public PaddedSPSCQueueL3(int capacity) {super(capacity);}
}
