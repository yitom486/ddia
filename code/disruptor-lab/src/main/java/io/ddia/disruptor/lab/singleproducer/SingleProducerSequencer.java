package io.ddia.disruptor.lab.singleproducer;

import java.util.concurrent.locks.LockSupport;

/**
 * 单生产者序号分配器：Disruptor 单生产者模式的核心，简化自 LMAX 源码。
 *
 * 关键点：全程零 CAS。原因在"单写者原则"——
 *   - nextValue / cachedGating 只有生产者线程读写     -> 普通 long，连 volatile 都不用
 *   - cursor 只有生产者写、消费者读                  -> volatile 写一次就够，无需 CAS
 *   - gatingSequences（消费者进度）只有消费者写、生产者读 -> volatile 读
 *
 * 对照多生产者：多线程要抢同一个"下一个序号"，必须 CAS；
 * 单生产者只有一个线程在推进序号，所以"抢"这件事不存在，只需保证可见性。
 *
 * 为了用计数器把"零 CAS"这件事可视化，这里加了两个 long 计数：
 *   volatileWriteCount : publish() 时自增（整条生产路径唯一的跨线程写）
 *   casAttemptCount    : 永远为 0（这里压根没有 CAS 调用，留作对照锚点）
 */
public class SingleProducerSequencer {

    private final int bufferSize;
    private final long mask;
    private final Sequence cursor;              // 已发布的最大序号（生产者唯一写）
    private final Sequence[] gatingSequences;   // 消费者进度（生产者只读）

    // ===== 单写者字段：只有生产者线程会碰，普通 long 即可 =====
    private long nextValue = Sequence.INITIAL;
    private long cachedGating = Sequence.INITIAL;

    // ===== 可视化用计数器（非线程安全，仅在生产者线程里自增，主线程最后读取） =====
    public long volatileWriteCount;
    public final long casAttemptCount = 0L;     // 永远为 0，证明没有 CAS

    public SingleProducerSequencer(int bufferSize, Sequence cursor, Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize 必须是 2 的幂: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1L;
        this.cursor = cursor;
        this.gatingSequences = gatingSequences;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public long mask() {
        return mask;
    }

    /**
     * 申请下一个可写槽位的序号。无 CAS，只有可能的一次 volatile 读。
     * 返回的序号用于 entries[seq & mask] 定位。
     */
    public long next() {
        long next = nextValue + 1;               // 本地算术，无竞争
        long wrapPoint = next - bufferSize;      // 一旦超过它就会覆盖消费者还没读的槽
        long cached = cachedGating;

        // 只有"可能要绕环"时才做一次 volatile 读消费者进度；平时直接用本地缓存
        if (wrapPoint > cached || cached > nextValue) {
            long min = minGating();
            while (wrapPoint > min) {
                LockSupport.parkNanos(1L);       // 消费者没跟上，自旋等待
                min = minGating();
            }
            cachedGating = min;                  // 普通写，仅生产者可见
        }

        nextValue = next;                        // 普通写，仅生产者可见
        return next;
    }

    /** 把已发布指针推到 sequence。这是整条生产路径唯一的"对其他线程可见"的写。 */
    public void publish(long sequence) {
        cursor.set(sequence);                    // 一次 volatile 写
        volatileWriteCount++;
        // 这里没有 compareAndSet —— casAttemptCount 永远是 0
    }

    public long cursor() {
        return cursor.get();
    }

    private long minGating() {
        long min = Long.MAX_VALUE;
        for (Sequence s : gatingSequences) {
            long v = s.get();                    // volatile 读
            if (v < min) min = v;
        }
        return min == Long.MAX_VALUE ? Sequence.INITIAL : min;
    }
}
