package com.argosware.blog.lwl;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class PaddedSpinQueue extends PaddedQueueL3 implements Queue {
    public PaddedSpinQueue(int capacity) {
        super(capacity);
    }

    @Override public void close() {
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.onSpinWait();
        try {
            closed = true;
        } finally { LOCK.setRelease(this, 0); }
    }

    @Override public void offer(int value, @Nullable Thread currentThread) throws ClosedException {
        while (true) {
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            try {
                if (size < capacity) {
                    data[DATA_OFF+(readIdx+size)%capacity] = value;
                    ++size;
                    break;
                } else if (closed) {
                    throw ClosedException.INSTANCE;
                }
            } finally {
                LOCK.setRelease(this, 0);
            }
        }
    }

    @Override public int take(@Nullable Thread currentThread) throws ClosedException {
        while (true) {
            while ((int) LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                Thread.onSpinWait();
            try {
                if (size > 0) {
                    int readIdx = this.readIdx, item = data[DATA_OFF+readIdx];
                    this.readIdx = (readIdx+1)%capacity;
                    --size;
                    return item;
                } else if (closed) {
                    throw ClosedException.INSTANCE;
                }
            } finally {
                LOCK.setRelease(this, 0);
            }
        }
    }
}

@SuppressWarnings("unused") abstract class PaddedQueueL0 {
    private   static final int DATA_PADDING = 128/4;
    protected static final int DATA_OFF = DATA_PADDING-(16/4); // use array object header
    protected final int[] data;
    protected final int capacity;

    public PaddedQueueL0(int capacity) {
        this.capacity = capacity;
        this.data = new int[capacity];
    }
}
@SuppressWarnings("unused") abstract class PaddedQueueL1 extends PaddedQueueL0 {
    private boolean p001, p002, p003, p004, p005, p006, p007, p008;
    private boolean p011, p012, p013, p014, p015, p016, p017, p018;
    private boolean p021, p022, p023, p024, p025, p026, p027, p028;
    private boolean p031, p032, p033, p034, p035, p036, p037, p038;
    private boolean p041, p042, p043, p044, p045, p046, p047, p048;
    private boolean p051, p052, p053, p054, p055, p056, p057, p058;
    private boolean p061, p062, p063, p064, p065, p066, p067, p068;
    private boolean p071, p072, p073, p074, p075, p076, p077, p078;
    private boolean p101, p102, p103, p104, p105, p106, p107, p108;
    private boolean p111, p112, p113, p114, p115, p116, p117, p118;
    private boolean p121, p122, p123, p124, p125, p126, p127, p128;
    private boolean p131, p132, p133, p134, p135, p136, p137, p138;
    private boolean p141, p142, p143, p144, p145, p146, p147, p148;
    private boolean p151, p152, p153, p154, p155, p156, p157, p158;
    private boolean p161, p162, p163, p164, p165, p166, p167, p168;
    private boolean p171, p172, p173, p174, p175, p176, p177, p178;

    public PaddedQueueL1(int capacity) {super(capacity);}
}
abstract class PaddedQueueL2 extends PaddedQueueL1 {
    protected static final int DATA_OFF = (128-16)/4;
    protected static final VarHandle LOCK;
    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(PaddedQueueL2.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected int plainLock;
    protected int readIdx, size;
    protected boolean closed;
    PaddedQueueL2(int capacity) {super(capacity);}
}
@SuppressWarnings("unused") abstract class PaddedQueueL3 extends PaddedQueueL2 {
    private boolean p001, p002, p003, p004, p005, p006, p007, p008;
    private boolean p011, p012, p013, p014, p015, p016, p017, p018;
    private boolean p021, p022, p023, p024, p025, p026, p027, p028;
    private boolean p031, p032, p033, p034, p035, p036, p037, p038;
    private boolean p041, p042, p043, p044, p045, p046, p047, p048;
    private boolean p051, p052, p053, p054, p055, p056, p057, p058;
    private boolean p061, p062, p063, p064, p065, p066, p067, p068;
    private boolean p071, p072, p073, p074, p075, p076, p077, p078;
    private boolean p101, p102, p103, p104, p105, p106, p107, p108;
    private boolean p111, p112, p113, p114, p115, p116, p117, p118;
    private boolean p121, p122, p123, p124, p125, p126, p127, p128;
    private boolean p131, p132, p133, p134, p135, p136, p137, p138;
    private boolean p141, p142, p143, p144, p145, p146, p147, p148;
    private boolean p151, p152, p153, p154, p155, p156, p157, p158;
    private boolean p161, p162, p163, p164, p165, p166, p167, p168;
    private boolean p171, p172, p173, p174, p175, p176, p177, p178;

    PaddedQueueL3(int capacity) {super(capacity);}
}
