package io.ddia.disruptor.lab.multiproducer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

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
 *   cachedGating      : AtomicLong，缓存 gating 最小值，减少 volatile 读
 *   cursor            : Sequence，publish 时 CAS 取 max，保证扫描上界单调不回退
 *
 * 多生产者下申请顺序可以乱于发布顺序；cursor 只表示"消费者最多该扫到哪里"，
 * 具体序号是否已发布由 AvailableBuffer 判断。
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

    // ===== 可视化计数器（多线程自增，必须原子） =====
    public final AtomicLong casAttemptCount = new AtomicLong();
    public final AtomicLong volatileWriteCount = new AtomicLong();

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
     *
     * 关键修复（对比错误版本）：
     *   错误版本用 do-while + continue：continue 跳到 while 条件，current 不重新读，
     *   spin 后 CAS 成功就直接退出，把本不该拿到的序号返回去了。
     *   正确版本用 while(true) + continue：continue 跳回循环顶部，current 重新读，
     *   spin 后 CAS 失败就重试，CAS 成功才退出——永远不会在绕环保护失效时拿到序号。
     */
    public long next() {
        while (true) {
            long current = nextValue.get();
            long next = current + 1;
            long wrapPoint = next - bufferSize;

            if (wrapPoint > cachedGating.get()) {
                long min = minGating();
                cachedGating.set(min);
                if (wrapPoint > cachedGating.get()) {
                    LockSupport.parkNanos(1L);
                    continue;
                }
            }

            if (nextValue.compareAndSet(current, next)) {
                casAttemptCount.incrementAndGet();
                return next;
            }
        }
    }

    /**
     * 发布：把 cursor 单调推进到至少 sequence。
     *
     * 多生产者申请顺序 ≠ 发布顺序。若直接 cursor.set(sequence)，后发布的小序号
     * 会把 cursor 从较大值写回较小值（倒退），消费者以 cursor 为扫描上界时会永久漏掉
     * 已经 available 的后续序号。因此这里必须 CAS 取 max，禁止回退。
     *
     * 调用方须先写好 Entry / availableBuffer，再调本方法。
     */
    public void publish(long sequence) {
        volatileWriteCount.incrementAndGet(); // 每次 publish 调用计一次（含未抬高 cursor 的小序号）
        long cur;
        do {
            cur = cursor.get();
            if (sequence <= cur) {
                return; // 后发布的小序号：上界已够大，不回写
            }
            casAttemptCount.incrementAndGet();
        } while (!cursor.compareAndSet(cur, sequence));
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