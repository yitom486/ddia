package io.ddia.disruptor.lab.compare;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 单生产者 vs 多生产者 对比 demo。
 *
 * 控制变量（两边完全相同）：
 *   - bufferSize      = 1024
 *   - 默认消息总数     = 40,000,000（单: 4000 万；多: 4 生产者 × 1000 万）
 *   - 消费者线程数     = 1
 *   - 消息体           = 装箱 Long
 *
 * 自变量：
 *   - 生产者线程数     = 1 (单) vs 4 (多)
 *   - Sequencer 实现   = SingleProducerSequencer vs MultiProducerSequencer
 *
 * 因变量（每轮打印）：
 *   - 端到端耗时、吞吐
 *   - volatile 写次数、 CAS 次数
 *   - CAS / 消息比值（单生产者期望 0，多生产者期望 >= 1）
 *
 * 运行（必须先 compile 再 exec，否则新加的类不会被发现）：
 *   mvn -f code/disruptor-lab/pom.xml compile exec:java \
 *       -Dexec.mainClass=io.ddia.disruptor.lab.compare.SingleVsMultiProducerDemo
 */
public class SingleVsMultiProducerDemo {

    private static final long LOG_EVERY = 1_000_000L;  // 消费者每处理 100 万条打一次 log

    private static void log(String tag, String fmt, Object... args) {
        // 注意：不能直接用 printf("[%s] " + fmt, tag, args)，
        // 那样 varargs 的 args 会被当成 printf 的 Object[]，再被 tag 这个 String 抢走 fmt 位置，
        // 报 IllegalFormatConversionException: d != [Ljava.lang.Object;
        System.out.printf("[%s] %s%n", tag, String.format(fmt, args));
        System.out.flush();
    }

    /** 槽位。sequence 用于校验消费者读到的确实是当前环次的数据。 */
    static final class Entry<T> {
        volatile T value;
        volatile long sequence = Sequence.INITIAL;
    }

    /** 复用的环形数组 + 发布入口。 */
    static final class RingBuffer<T> {
        final Entry<T>[] entries;
        final Sequencer sequencer;
        final boolean multiProducer;

        @SuppressWarnings("unchecked")
        RingBuffer(int bufferSize, Sequencer sequencer, boolean multiProducer) {
            this.sequencer = sequencer;
            this.multiProducer = multiProducer;
            this.entries = new Entry[bufferSize];
            for (int i = 0; i < bufferSize; i++) entries[i] = new Entry<>();
        }

        public void publish(T value) {
            long seq = sequencer.next();
            Entry<T> e = entries[(int) (seq & sequencer.mask())];
            e.value = value;
            e.sequence = seq;
            sequencer.publish(seq);
        }
    }

    public interface EventHandler<T> {
        void onEvent(T event, long sequence, boolean endOfBatch);
    }

    /**
     * 批量消费者。
     *
     * 单生产者：cursor 推到 N 意味着 [0..N] 全部已发布，所以靠 `seq.cursor()` 一次性扫完一批。
     * 多生产者：cursor 推到 N 只表示 [0..N] 已被申请，中间可能有尚未 publish 的空洞，
     *          因此必须对每个 next 检查 isAvailable(next)。发布标记可用后，再用
     *          Entry.sequence 校验没有读到错误环次的数据。
     */
    static final class BatchEventProcessor<T> implements Runnable {
        private final RingBuffer<T> rb;
        private final Sequencer seq;
        private final Sequence cursor;
        private final EventHandler<T> handler;
        private final String tag;
        private final long stopAt;
        private volatile boolean running = true;
        private volatile Throwable failure;

        BatchEventProcessor(RingBuffer<T> rb, Sequencer seq, Sequence cursor,
                            EventHandler<T> handler, String tag, long stopAt) {
            this.rb = rb;
            this.seq = seq;
            this.cursor = cursor;
            this.handler = handler;
            this.tag = tag;
            this.stopAt = stopAt;
        }

        @Override
        public void run() {
            long next = cursor.get() + 1;
            long lastLog = 0;
            int spinInRow = 0;
            long startedNs = System.nanoTime();
            try {
                while (running && next <= stopAt) {
                    long available = seq.cursor();
                    if (next > available) {
                        Thread.onSpinWait();
                        if (++spinInRow % 1_000_000 == 0) {
                            long stuck = System.nanoTime() - startedNs;
                            log(tag, "  消费者空闲中 next=%d available=%d 已等 %d ms",
                                    next, available, stuck / 1_000_000);
                        }
                        continue;
                    }
                    spinInRow = 0;
                    while (next <= available && next <= stopAt) {
                        if (rb.multiProducer) {
                            // cursor 是 claimed 上界；具体 next 必须等发布标记完成。
                            if (!seq.isAvailable(next)) {
                                Thread.onSpinWait();
                                break;
                            }
                        }
                        Entry<T> e = rb.entries[(int) (next & seq.mask())];
                        if (e.sequence != next) {
                            throw new IllegalStateException(
                                    "槽位环次错误: expected=" + next + ", actual=" + e.sequence);
                        }
                        handler.onEvent(e.value, next, next == available);
                        cursor.set(next);
                        next++;

                        if (next - lastLog >= LOG_EVERY) {
                            lastLog = next;
                            log(tag, "  消费者进度: %,d / next=%,d", next, next);
                        }
                    }
                }
                log(tag, "  消费者退出循环 (running=%s, next=%,d, stopAt=%,d)", running, next, stopAt);
            } catch (Throwable t) {
                failure = t;
                running = false;
                log(tag, "!! 消费者异常 next=%d processed=%d : %s",
                        next, cursor.get(), t);
                t.printStackTrace(System.out);
            }
        }

        public void stop() { running = false; }
        public long processed() { return cursor.get(); }
        public Throwable failure() { return failure; }
    }

    /** 单次跑测试的结果。 */
    static final class Result {
        final String label;
        final int producerCount;
        final long totalMessages;
        final long elapsedNs;
        final long volatileWrites;
        final long casAttempts;

        Result(String label, int producerCount, long totalMessages, long elapsedNs,
               long volatileWrites, long casAttempts) {
            this.label = label;
            this.producerCount = producerCount;
            this.totalMessages = totalMessages;
            this.elapsedNs = elapsedNs;
            this.volatileWrites = volatileWrites;
            this.casAttempts = casAttempts;
        }

        double throughputMps() {
            return totalMessages / (elapsedNs / 1_000_000_000.0) / 1_000_000.0;
        }
        double casPerMessage() {
            return totalMessages == 0 ? 0.0 : (double) casAttempts / totalMessages;
        }
    }

    /** 跑一轮：固定 producerCount，perProducer 条消息，返回结果。 */
    static Result runOnce(String label, int producerCount, long perProducer,
                          int bufferSize, boolean multiProducer) throws InterruptedException {
        long total = producerCount * perProducer;

        log(label, "开始：生产者数=%d, perProducer=%,d, total=%,d, multi=%s",
                producerCount, perProducer, total, multiProducer);

        Sequence producerCursor = new Sequence(Sequence.INITIAL);
        Sequence consumerCursor = new Sequence(Sequence.INITIAL);
        AtomicLong atomicCursor = new AtomicLong(Sequence.INITIAL);

        Sequencer sequencer = multiProducer
                ? new MultiProducerSequencer(bufferSize, atomicCursor, consumerCursor)
                : new SingleProducerSequencer(bufferSize, producerCursor, consumerCursor);

        RingBuffer<Long> ring = new RingBuffer<>(bufferSize, sequencer, multiProducer);

        BatchEventProcessor<Long> consumer = new BatchEventProcessor<>(
                ring, sequencer, consumerCursor,
                (event, sequence, endOfBatch) -> {
                    if (event == null) {
                        log(label, "!! handler 收到 null @ seq=%,d", sequence);
                        throw new IllegalStateException("消费到 null @ " + sequence);
                    }
                },
                label, total - 1);

        Thread consumerThread = new Thread(consumer, label + ".consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        CountDownLatch producersDone = new CountDownLatch(producerCount);
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        Thread[] producers = new Thread[producerCount];
        for (int i = 0; i < producerCount; i++) {
            final int idx = i;
            producers[i] = new Thread(() -> {
                try {
                    long base = (long) idx * perProducer;
                    long lastLog = 0;
                    for (long j = 0; j < perProducer; j++) {
                        ring.publish(base + j);
                        if (j - lastLog >= LOG_EVERY) {
                            lastLog = j;
                            log(label, "  producer-%d 进度: %,d", idx, j);
                        }
                    }
                } catch (Throwable t) {
                    producerFailure.compareAndSet(null, t);
                } finally {
                    producersDone.countDown();
                }
            }, label + ".producer-" + i);
            producers[i].setDaemon(true);
        }

        long start = System.nanoTime();
        for (Thread producer : producers) {
            producer.start();
        }

        // 监控必须覆盖生产者运行阶段，不能先无限 join 再开始计时。
        long timeoutSeconds = Long.getLong("disruptor.timeoutSeconds", 120L);
        long deadlineNs = start + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        long lastProgress = -1;
        long lastProgressNs = start;
        while (producersDone.getCount() > 0 || consumer.processed() < total - 1) {
            Throwable producerError = producerFailure.get();
            if (producerError != null) {
                consumer.stop();
                throw new IllegalStateException("生产者异常", producerError);
            }
            Throwable consumerError = consumer.failure();
            if (consumerError != null) {
                throw new IllegalStateException("消费者异常", consumerError);
            }

            long now = System.nanoTime();
            if (now > deadlineNs) {
                consumer.stop();
                log(label, "!! %d 秒内未完成: producersLeft=%d, processed=%,d / total=%,d, cursor=%,d",
                        timeoutSeconds, producersDone.getCount(), consumer.processed(), total,
                        sequencer.cursor());
                throw new IllegalStateException("生产/消费流程超时");
            }

            long p = consumer.processed();
            if (p != lastProgress) {
                lastProgress = p;
                lastProgressNs = now;
            } else if (now - lastProgressNs > 5_000_000_000L) {
                consumer.stop();
                log(label, "!! 5 秒无消费进度: producersLeft=%d, processed=%,d / total=%,d, cursor=%,d",
                        producersDone.getCount(), p, total, sequencer.cursor());
                throw new IllegalStateException("消费者停滞");
            }
            LockSupport.parkNanos(1_000_000L);
        }

        for (Thread producer : producers) {
            producer.join(1_000L);
        }
        long elapsedNs = System.nanoTime() - start;
        consumer.stop();
        consumerThread.join(1000);
        if (consumerThread.isAlive()) {
            throw new IllegalStateException("消费者线程未退出");
        }

        log(label, "  %d 个生产者与消费者均已完成", producerCount);

        double throughput = total / (elapsedNs / 1_000_000_000.0) / 1_000_000.0;
        log(label, "结束：耗时 %d ms, 吞吐 %.2f Mops/s, volatileWrites=%,d, cas=%,d",
                elapsedNs / 1_000_000, throughput,
                sequencer.volatileWriteCount(), sequencer.casAttemptCount());

        return new Result(label, producerCount, total, elapsedNs,
                sequencer.volatileWriteCount(), sequencer.casAttemptCount());
    }

    public static void main(String[] args) throws InterruptedException {
        log("main", "入口到达 classpath=%s", System.getProperty("java.class.path"));
        final int bufferSize = Integer.getInteger("disruptor.bufferSize", 1024);
        final long perProducer = Long.getLong("disruptor.perProducer", 10_000_000L);
        if (perProducer <= 0) {
            throw new IllegalArgumentException("disruptor.perProducer 必须 > 0");
        }
        final long totalMessages = 4L * perProducer;
        final long warmupPerProducer = Math.max(10_000L, perProducer / 10L);

        System.out.println("===== 单生产者 vs 多生产者 对比实验 =====");
        System.out.printf("bufferSize     = %d%n", bufferSize);
        System.out.printf("消息总数       = %,d (多生产者 = %d × %,d)%n",
                totalMessages, 4, perProducer);
        System.out.println();

        // 预热也保持单/多两边总消息数相同。
        log("main", "[warmup] 跑 1 轮预热，让 JIT 编译...");
        runOnce("warmup-single", 1, 4L * warmupPerProducer, bufferSize, false);
        runOnce("warmup-multi", 4, warmupPerProducer, bufferSize, true);
        log("main", "[warmup] 完成。");
        System.out.println();

        Result single = runOnce("单生产者", 1, totalMessages, bufferSize, false);
        Result multi  = runOnce("多生产者", 4, perProducer, bufferSize, true);

        printTable(single, multi);
    }

    private static void printTable(Result s, Result m) {
        System.out.println("┌─────────────┬──────────────┬──────────────┐");
        System.out.println("│     指标    │  单生产者     │  多生产者(4)  │");
        System.out.println("├─────────────┼──────────────┼──────────────┤");
        System.out.printf ("│ 生产者数    │ %12d │ %12d │%n", s.producerCount, m.producerCount);
        System.out.printf ("│ 消息总数    │ %,12d │ %,12d │%n", s.totalMessages, m.totalMessages);
        System.out.printf ("│ 耗时 (ms)   │ %,12d │ %,12d │%n",
                s.elapsedNs / 1_000_000, m.elapsedNs / 1_000_000);
        System.out.printf ("│ 吞吐 Mops/s │ %12.2f │ %12.2f │%n",
                s.throughputMps(), m.throughputMps());
        System.out.printf ("│ volatile 写 │ %,12d │ %,12d │%n", s.volatileWrites, m.volatileWrites);
        System.out.printf ("│ CAS 次数    │ %,12d │ %,12d │%n", s.casAttempts, m.casAttempts);
        System.out.printf ("│ CAS/消息    │ %12.3f │ %12.3f │%n", s.casPerMessage(), m.casPerMessage());
        System.out.println("└─────────────┴──────────────┴──────────────┘");

        double speedup = m.throughputMps() > 0 ? s.throughputMps() / m.throughputMps() : 0.0;
        System.out.println();
        System.out.printf("吞吐量比值：单 / 多 = %.2fx%n", speedup);
        System.out.println();
        System.out.println("看什么：");
        System.out.println("  1. 多生产者的 CAS 次数 > 0，单生产者 == 0");
        System.out.println("  2. 多生产者的 CAS/消息 比值（>=1 表示每次 next 至少 1 次 CAS）");
        System.out.println("  3. 单生产者吞吐通常高 1.x ~ 数 x，倍数取决于 CPU 核数与是否被消费者拖住");
    }
}
