package io.ddia.disruptor.lab.multiproducer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 多生产者序号分配器：Disruptor 多生产者模式的核心，简化自 LMAX 源码。
 *
 * 与单生产者的根本差异：
 *   - 多个线程同时调 next() 抢序号，必须 CAS
 *   - 抢到的序号和"发布到的序号"是两件事，中间会留空洞
 *   - 消费者要看每个 Entry 的 sequence 标记来跳过空洞
 *
 * 关键点：
 *   nextValue         : AtomicLong.compareAndSet 抢序号（每条消息可能多次 CAS）
 *   cachedGating      : 仍然只是普通 long，但只在"单写者窗口"里写
 *   cursor            : AtomicLong，最后一步发布用 set()（单写者窗口结束）
 *
 * "单写者窗口"是 LMAX 多生产者 Sequencer 的精髓：
 *   一次 next() 调用里，cachedGating 是本地变量，不会有别的线程碰它。
 *   出了 next() 之后才把它"提交"回去（用 putOrderedInt 到一个 padded int[]，
 *   或用 AtomicLongFieldUpdater 等价方案）。这里为了简洁，直接用 AtomicLong。
 */
public class MultiProducerSequencer {

    private final int bufferSize;
    private final long mask;
    private final Sequence cursor;
    private final Sequence[] gatingSequences;

    // ===== 关键差异 1：nextValue 必须是原子的 =====
    private final AtomicLong nextValue = new AtomicLong(Sequence.INITIAL);

    // ===== 关键差异 2：cachedGating 也得是原子的 =====
    // 多生产者下，线程 A 进入 next() 之前可能看到的是线程 B 留下的 cachedGating
    // 但线程 B 正在算自己的 wrapPoint、可能要等 —— 所以这个值也得线程安全
    private final AtomicLong cachedGating = new AtomicLong(Sequence.INITIAL);

    // ===== 可视化计数器 =====
    public long casAttemptCount;   // next() 里 CAS 失败重试次数
    public long volatileWriteCount;

    public MultiProducerSequencer(int bufferSize, Sequence cursor, Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize 必须是 2 的幂: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1L;
        this.cursor = cursor;
        this.gatingSequences = gatingSequences;
    }

    public int bufferSize() { return bufferSize; }

    public long mask() { return mask; }

    /**
     * 申请一个可写槽位的序号。零 CAS 是不可能的：必须靠 CAS 把 nextValue 推上去。
     *
     * 与单生产者的对照：
     *   单：nextValue 是普通 long，nextValue++ 即可
     *   多：nextValue 是 AtomicLong，必须 compareAndSet；失败说明别人抢先了，重试
     */
    public long next() {
        long current;
        long next;
        long wrapPoint;
        long cached = cachedGating.get();

        do {
            current = nextValue.get();                // volatile 读
            next = current + 1;

            wrapPoint = next - bufferSize;
            if (wrapPoint > cached) {                  // 真的要绕环了
                long min = minGating();                // volatile 读消费者进度
                if (wrapPoint > min) {
                    // 消费者没跟上，让出 CPU，但这里没 park，跟单生产者也不同：
                    // 多生产者下 park 完醒来还得重新读 nextValue，因为别人可能改过了
                    Thread.onSpinWait();
                    continue;                          // 重新进 do-while 循环
                }
                cachedGating.set(min);                 // 缓存回去，给别的线程用
                cached = min;
            }
            // CAS 抢 nextValue：expected=current, new=next
            // 失败说明别的线程抢先 +1 了，current 已过期，重新读
        } while (!nextValue.compareAndSet(current, next));

        casAttemptCount++;
        return next;
    }

    /**
     * 发布。与单生产者最大的不同：这里 cursor.set() 之前，
     * 调用方必须先把 Entry 自己的 sequence 标记写好。
     *
     * 为什么不能用 cachedValue/单写者窗口？
     *   单生产者：cursor 的"新值"和"旧值"是同一个线程算的，能用 set()
     *   多生产者：cursor.set() 也仍然是 volatile 写（不是 CAS），因为发布时刻
     *            只受"我自己的 next()"结果影响，set 仍然是原子的"覆盖"语义。
     */
    public void publish(long sequence) {
        cursor.set(sequence);                          // 一次 volatile 写（不是 CAS）
        volatileWriteCount++;
    }

    public long cursor() { return cursor.get(); }

    private long minGating() {
        long min = Long.MAX_VALUE;
        for (Sequence s : gatingSequences) {
            long v = s.get();
            if (v < min) min = v;
        }
        return min == Long.MAX_VALUE ? Sequence.INITIAL : min;
    }
}