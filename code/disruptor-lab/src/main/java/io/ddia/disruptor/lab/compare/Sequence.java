package io.ddia.disruptor.lab.compare;

/**
 * 共享 Sequence 定义。简化版（保留 cache line padding 字段名，便于和 singleproducer 版对照）。
 * 这里的值域与 singleproducer.Sequence 保持一致，因此 Sequencer 的对照实验可以共用同一份语义。
 */
public class Sequence {
    public static final long INITIAL = -1L;
    long p0, p1, p2, p3, p4, p5, p6;
    volatile long value = INITIAL;
    long q0, q1, q2, q3, q4, q5, q6;
    public Sequence() {}
    public Sequence(long initial) { this.value = initial; }
    public long get() { return value; }
    public void set(long v) { value = v; }
}