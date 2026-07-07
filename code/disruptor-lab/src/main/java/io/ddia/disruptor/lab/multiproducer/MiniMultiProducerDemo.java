package io.ddia.disruptor.lab.multiproducer;

import java.util.concurrent.locks.LockSupport;

/**
 * 多生产者 Disruptor 最小 demo：3 个生产者线程 -> RingBuffer -> 1 个消费者线程。
 *
 * 与单生产者 demo 的关键差异：
 *   1. RingBuffer 里每个 Entry 必须带自己的 sequence 标记
 *   2. 消费者必须按 sequence 顺序处理，遇到空洞就跳过（等生产者补上）
 *   3. 会打印 CAS 次数，证明多生产者路径上 CAS 不是 0
 *
 * 运行：
 *   mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.multiproducer.MiniMultiProducerDemo
 */
public class MiniMultiProducerDemo {

    /** 关键差异 1：每个槽位带自己的 sequence 标记，用来识别"是否已发布" */
    static final class Entry<T> {
        volatile T value;
        volatile long sequence = Sequence.INITIAL;    // -1 = 未发布；>=0 = 发布时的序号
    }

    static final class RingBuffer<T> {
        final Entry<T>[] entries;
        final MultiProducerSequencer sequencer;

        @SuppressWarnings("unchecked")
        RingBuffer(int bufferSize, MultiProducerSequencer sequencer) {
            this.sequencer = sequencer;
            this.entries = new Entry[bufferSize];
            for (int i = 0; i < bufferSize; i++) entries[i] = new Entry<>();
        }

        public void publish(T value) {
            long seq = sequencer.next();                            // CAS 抢序号
            Entry<T> e = entries[(int) (seq & sequencer.mask())];
            e.value = value;
            e.sequence = seq;                                       // 标记这个槽已发布
            sequencer.publish(seq);                                 // 推 cursor
        }
    }

    public interface EventHandler<T> {
        void onEvent(T event, long sequence, boolean endOfBatch);
    }

    /**
     * 关键差异 2：消费者要按 sequence 顺序消费，遇到空洞就 park 等。
     *
     * 为什么单生产者不需要这一步？
     *   单生产者 next() 永远按顺序返回（先 next=10 才能 next=11），publish 也按顺序，
     *   所以 cursor=N 意味着 [0..N] 全部已发布。
     * 多生产者为什么需要？
     *   线程 A 抢到 10、线程 B 抢到 11、线程 C 抢到 12；
     *   假如 C 先写完先 publish(12)，cursor=12，但槽 10 还没写。
     *   消费者看到 cursor=12 不能直接处理 10/11/12，
     *   必须看 entry.sequence == 我期望的序号 才处理。
     */
    static final class BatchEventProcessor<T> implements Runnable {
        private final RingBuffer<T> rb;
        private final MultiProducerSequencer seq;
        private final EventHandler<T> handler;
        private final Sequence cursor;
        private volatile boolean running = true;

        BatchEventProcessor(RingBuffer<T> rb, MultiProducerSequencer seq,
                            Sequence cursor, EventHandler<T> handler) {
            this.rb = rb;
            this.seq = seq;
            this.cursor = cursor;
            this.handler = handler;
        }

        @Override
        public void run() {
            long next = cursor.get() + 1;
            while (running) {
                long available = seq.cursor();                       // volatile 读 cursor
                while (next <= available) {
                    Entry<T> e = rb.entries[(int) (next & seq.mask())];
                    long published = e.sequence;                    // 关键：读槽的发布标记
                    if (published != next) {
                        // 还没发布到这一条，等一下（避免 busy-spin）
                        LockSupport.parkNanos(1L);
                        // 重新读 cursor，可能又推进了
                        available = seq.cursor();
                        continue;
                    }
                    handler.onEvent(e.value, next, next == available);
                    cursor.set(next);
                    next++;
                }
                if (next > available) {
                    LockSupport.parkNanos(1L);
                }
            }
        }

        public void stop() { running = false; }
        public long processed() { return cursor.get(); }
    }

    public static void main(String[] args) throws InterruptedException {
        final int bufferSize = 1024;
        final Sequence producerCursor = new Sequence(Sequence.INITIAL);
        final Sequence consumerCursor = new Sequence(Sequence.INITIAL);
        final MultiProducerSequencer sequencer =
                new MultiProducerSequencer(bufferSize, producerCursor, consumerCursor);
        final RingBuffer<Long> ring = new RingBuffer<>(bufferSize, sequencer);

        final int producerCount = 3;
        final long perProducer = 5_000_000L;
        final long total = producerCount * perProducer;

        BatchEventProcessor<Long> consumer = new BatchEventProcessor<>(
                ring, sequencer, consumerCursor,
                (event, sequence, endOfBatch) -> {
                    if (event == null) {
                        throw new IllegalStateException("消费到 null @ " + sequence);
                    }
                });

        Thread consumerThread = new Thread(consumer, "consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        Thread[] producers = new Thread[producerCount];
        long start = System.nanoTime();
        for (int i = 0; i < producerCount; i++) {
            final int idx = i;
            producers[i] = new Thread(() -> {
                for (long j = 0; j < perProducer; j++) {
                    ring.publish((long) (idx * perProducer + j));
                }
            }, "producer-" + i);
            producers[i].setDaemon(true);
            producers[i].start();
        }
        for (Thread t : producers) t.join();

        while (consumer.processed() < total - 1) {
            LockSupport.parkNanos(1_000L);
        }
        long elapsedNs = System.nanoTime() - start;
        consumer.stop();

        long elapsedMs = elapsedNs / 1_000_000;
        double throughputMps = total / (elapsedNs / 1_000_000_000.0) / 1_000_000.0;

        System.out.println();
        System.out.println("===== 多生产者 Disruptor 运行结果 =====");
        System.out.printf("生产者线程数    : %d%n", producerCount);
        System.out.printf("消息总数        : %,d 条%n", total);
        System.out.printf("端到端耗时      : %,d ms%n", elapsedMs);
        System.out.printf("吞吐量          : %.2f M ops/s%n", throughputMps);
        System.out.println();
        System.out.println("===== 对照单生产者 =====");
        System.out.printf("publish() 次数 (volatile 写) : %,d%n", sequencer.volatileWriteCount);
        System.out.printf("CAS 尝试次数                 : %,d  <-- 这里不为 0%n", sequencer.casAttemptCount);
        System.out.printf("CAS / 消息                   : %.2f%n",
                (double) sequencer.casAttemptCount / sequencer.volatileWriteCount);
        System.out.println();
        System.out.println("结论：多生产者路径上 CAS != 0，这是单生产者比多生产者快一个数量级的根本原因。");
    }
}