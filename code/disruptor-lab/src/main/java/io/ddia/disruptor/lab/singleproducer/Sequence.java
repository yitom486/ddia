package io.ddia.disruptor.lab.singleproducer;

/**
 * 一个带 cache line padding 的 volatile long，模仿 LMAX Disruptor 的 Sequence。
 *
 * 布局：对象头 + [左 7 个 long][value][右 7 个 long]
 *      = header + 56 + 8 + 56 字节
 * 让 value 独占一条 64 字节的 cache line，避免和相邻字段伪共享。
 *
 * 现代 JVM 上等价于给 value 加 @Contended，这里手写是为了让 JOL 能"看见"。
 * （LMAX 用继承链 LhsPad->Value->RhsPad 排 padding，这里用字段等价，效果一致。）
 */
public class Sequence {

    public static final long INITIAL = -1L;

    /** 左 padding：7 个 long = 56 字节，把 value 推离对象头。 */
    long p0, p1, p2, p3, p4, p5, p6;

    /** 真正的值：volatile，保证跨线程可见性。 */
    volatile long value = INITIAL;

    /** 右 padding：7 个 long = 56 字节，确保下一个字段不和 value 共享 cache line。 */
    long q0, q1, q2, q3, q4, q5, q6;

    public Sequence() {}

    public Sequence(long initial) {
        this.value = initial;
    }

    public long get() {
        return value;
    }

    /** 唯一的写入口。如果这个 Sequence 只有一个写者（如生产者的 cursor），就不需要 CAS。 */
    public void set(long v) {
        value = v;
    }
}
