package io.ddia.disruptor.lab.compare;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 多生产者 Sequencer 的 compare/ 版本。
 *
 * 核心算法来自 LMAX Disruptor 多生产者模式：
 *   - nextValue   用 CAS 抢序号
 *   - available[] 每个槽位"是否已发布"位图
 *   - cursor      用 AtomicLong,publish 时 CAS 推进
 *                 (因为多线程 publish 都要推 cursor,所以必须 CAS)
 *   - 消费者按 entry.sequence (== isAvailable) 处理,遇到空洞就 park
 *
 * 关键不变量:
 *   - cursor.get() 始终 >= 已发布序号的最大值
 *   - 单生产者的 cursor 是 Sequence(单写者,volatile 写即可);
 *     多生产者的 cursor 必须是 AtomicLong(多写者,必须 CAS)
 */
public class MultiProducerSequencer implements Sequencer {

    private final int bufferSize;
    private final long mask;
    /** 多生产者下 cursor 由多线程 publish 同时推,必须用 AtomicLong。 */
    private final AtomicLong cursor;
    private final Sequence[] gatingSequences;

    /** 抢序号的 CAS 计数器。 */
    private final AtomicLong nextValue = new AtomicLong(Sequence.INITIAL);

    /** cached gating sequence: 减少 minGating() 的 volatile 读。 */
    private final AtomicLong cachedGating = new AtomicLong(Sequence.INITIAL);

    /**
     * 每个槽位是否已发布。
     * 索引 i 表示 entry[i] 是否已发布 seq (i+1, i+1+bufferSize, ...)。
     */
    private final boolean[] available;

    /** 可视化计数器。 */
    public long casAttemptCount;     // next() 里 CAS 尝试 + publish() 里推进 cursor 的 CAS 尝试
    public long volatileWriteCount;  // publish() 次数

    public MultiProducerSequencer(int bufferSize, AtomicLong cursor, Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize 必须是 2 的幂: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1L;
        this.cursor = cursor;
        this.gatingSequences = gatingSequences;
        this.available = new boolean[bufferSize];
    }

    @Override
    public long next() {
        long current;
        long next;
        long wrapPoint;
        long cached = cachedGating.get();

        do {
            current = nextValue.get();
            next = current + 1;

            wrapPoint = next - bufferSize;
            if (wrapPoint > cached) {
                long min = minGating();
                if (wrapPoint > min) {
                    Thread.onSpinWait();
                    continue;
                }
                cachedGating.set(min);
                cached = min;
            }
        } while (!nextValue.compareAndSet(current, next));

        casAttemptCount++;
        return next;
    }

    @Override
    public void publish(long sequence) {
        // 1. 标记槽位已发布 —— 消费者靠这个跳过空洞
        int index = (int) (sequence & mask);
        available[index] = true;
        volatileWriteCount++;

        // 2. 推 cursor:CAS 把 cursor 推到 max(已 publish 的所有 seq)。
        //    因为 sequence 单调增,推到 sequence 即可。
        long current = cursor.get();
        while (current < sequence) {
            if (cursor.compareAndSet(current, sequence)) {
                casAttemptCount++;
                return;
            }
            current = cursor.get();
            casAttemptCount++;
        }
    }

    /** 消费者用:检查 index 槽位是否已发布。 */
    @Override
    public boolean isAvailable(long sequence) {
        return available[(int) (sequence & mask)];
    }

    @Override public long cursor() { return cursor.get(); }
    @Override public int bufferSize() { return bufferSize; }
    @Override public long mask() { return mask; }
    @Override public long volatileWriteCount() { return volatileWriteCount; }
    @Override public long casAttemptCount() { return casAttemptCount; }

    private long minGating() {
        long min = Long.MAX_VALUE;
        for (Sequence s : gatingSequences) {
            long v = s.get();
            if (v < min) min = v;
        }
        return min == Long.MAX_VALUE ? Sequence.INITIAL : min;
    }
}