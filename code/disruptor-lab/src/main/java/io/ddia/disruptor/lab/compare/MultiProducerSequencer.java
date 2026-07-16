package io.ddia.disruptor.lab.compare;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多生产者 Sequencer 的 compare/ 版本。
 *
 * 核心算法来自 LMAX Disruptor 多生产者模式：
 *   - cursor          用 CAS 抢序号，表示最大已申请序号（claimed cursor）
 *   - availableBuffer 按“槽位 + 环次”记录具体 sequence 是否已发布
 *   - publish()       只标记具体 sequence 已发布，不再改写 cursor
 *   - 消费者以 cursor 为扫描上界，以 isAvailable(sequence) 处理发布空洞
 *
 * 关键不变量:
 *   - cursor 只增不减，不会被乱序 publish 写回较小值
 *   - cursor=N 只表示 [0..N] 已被申请，不表示它们全部已发布
 *   - isAvailable(N)=true 才表示 N 对应的 Entry 已完整写入
 *   - 单生产者的 cursor 是 Sequence(单写者,volatile 写即可);
 *     多生产者的 cursor 是 AtomicLong(多写者在 next() 中 CAS 抢号)
 */
public class MultiProducerSequencer implements Sequencer {

    private final int bufferSize;
    private final long mask;
    private final int indexShift;
    /** 最大已申请序号。多个生产者在 next() 中 CAS 推进。 */
    private final AtomicLong cursor;
    private final Sequence[] gatingSequences;

    /** cached gating sequence: 减少 minGating() 的 volatile 读。 */
    private final AtomicLong cachedGating = new AtomicLong(Sequence.INITIAL);

    /**
     * 每个槽位最近一次已发布的环次。
     *
     * 不能只用 boolean：同一个槽会依次承载 seq=i、i+bufferSize、...
     * 如果第一圈留下 true，第二圈在真正 publish 前就会被误判为可用。
     * AtomicIntegerArray 同时提供跨线程可见性，以及 value 写入到发布标记之间的
     * release/acquire 顺序。
     */
    private final AtomicIntegerArray availableBuffer;

    /**
     * 每个生产者线程独占一个普通计数器，结束后再汇总。
     * 如果在热路径上用 AtomicLong/LongAdder 统计 CAS，会额外制造共享写入，污染被测结果。
     */
    private final ConcurrentLinkedQueue<Counter> counterRegistry = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<Counter> threadCounter = ThreadLocal.withInitial(() -> {
        Counter counter = new Counter();
        counterRegistry.add(counter);
        return counter;
    });

    public MultiProducerSequencer(int bufferSize, AtomicLong cursor, Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize 必须是 2 的幂: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1L;
        this.indexShift = Integer.numberOfTrailingZeros(bufferSize);
        this.cursor = cursor;
        this.gatingSequences = gatingSequences;
        this.availableBuffer = new AtomicIntegerArray(bufferSize);
        for (int i = 0; i < bufferSize; i++) {
            availableBuffer.set(i, -1);
        }
    }

    @Override
    public long next() {
        Counter counter = threadCounter.get();
        while (true) {
            long current = cursor.get();
            long next = current + 1;
            long wrapPoint = next - bufferSize;
            long cached = cachedGating.get();

            if (wrapPoint > cached || cached > current) {
                long min = minGating();
                if (wrapPoint > min) {
                    Thread.onSpinWait();
                    // 关键：容量不足时回到循环顶部，绝不能执行下面的 CAS。
                    continue;
                }
                cachedGating.set(min);
            }

            counter.casAttempts++;
            if (cursor.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    @Override
    public void publish(long sequence) {
        int index = (int) (sequence & mask);
        int availabilityFlag = availabilityFlag(sequence);
        // AtomicIntegerArray.set 是发布动作：Entry.value/sequence 的写入先于该标记可见。
        availableBuffer.set(index, availabilityFlag);
        threadCounter.get().publishes++;
    }

    /** 消费者用:检查 index 槽位是否已发布。 */
    @Override
    public boolean isAvailable(long sequence) {
        int index = (int) (sequence & mask);
        return availableBuffer.get(index) == availabilityFlag(sequence);
    }

    @Override public long cursor() { return cursor.get(); }
    @Override public int bufferSize() { return bufferSize; }
    @Override public long mask() { return mask; }
    @Override public long volatileWriteCount() {
        long total = 0;
        for (Counter counter : counterRegistry) total += counter.publishes;
        return total;
    }
    @Override public long casAttemptCount() {
        long total = 0;
        for (Counter counter : counterRegistry) total += counter.casAttempts;
        return total;
    }

    private int availabilityFlag(long sequence) {
        return (int) (sequence >>> indexShift);
    }

    private static final class Counter {
        long casAttempts;
        long publishes;
    }

    private long minGating() {
        long min = Long.MAX_VALUE;
        for (Sequence s : gatingSequences) {
            long v = s.get();
            if (v < min) min = v;
        }
        return min == Long.MAX_VALUE ? Sequence.INITIAL : min;
    }
}
