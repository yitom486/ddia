package io.ddia.disruptor.lab.multiproducer;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 多生产者 Sequencer 配套的 Sequence。
 *
 * 布局与 singleproducer.Sequence 保持一致：value 独占一条 64 字节 cache line。
 * 这里用独立的副本而不是 import singleproducer.Sequence，是为了：
 *   1. 演示两个目录互不依赖、可单独 import；
 *   2. 让 multiproducer/MiniMultiProducerDemo 跑时不需要 sourcepath 倒引。
 *
 * 多生产者 publish 需要 CAS 推进 cursor（取 max、禁止倒退），因此提供 compareAndSet。
 */
public class Sequence {

    public static final long INITIAL = -1L;

    private static final AtomicLongFieldUpdater<Sequence> VALUE =
            AtomicLongFieldUpdater.newUpdater(Sequence.class, "value");

    long p0, p1, p2, p3, p4, p5, p6;
    volatile long value = INITIAL;
    long q0, q1, q2, q3, q4, q5, q6;

    public Sequence() {}

    public Sequence(long initial) {
        this.value = initial;
    }

    public long get() { return value; }
    public void set(long v) { value = v; }

    public boolean compareAndSet(long expect, long update) {
        return VALUE.compareAndSet(this, expect, update);
    }
}