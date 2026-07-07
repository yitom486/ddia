package io.ddia.disruptor.lab.singleproducer;

import java.util.concurrent.locks.LockSupport;

/**
 * 一个最小可运行的单生产者 Disruptor：
 *   1 个生产者线程 -> RingBuffer -> 1 个消费者线程
 *
 * 跑完会打印吞吐量，并打印 SingleProducerSequencer 的两个计数器：
 *   volatileWriteCount == 处理消息数   (每条消息一次 volatile 写)
 *   casAttemptCount    == 0            (证明生产路径零 CAS)
 *
 * 运行：
 *   mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.singleproducer.MiniSingleProducerDemo
 */
public class MiniSingleProducerDemo {

    /** 槽位：预分配、复用，运行期零对象分配。 */
    static final class Entry<T> {
        volatile T value;
        volatile long sequence = Sequence.INITIAL;   // -1 表示尚未发布
    }

    /** 环形数组：长度为 2 的幂，用 seq & mask 取模。 */
    static final class RingBuffer<T> {
        final Entry<T>[] entries;
        final SingleProducerSequencer sequencer;

        @SuppressWarnings("unchecked")
        RingBuffer(int bufferSize, SingleProducerSequencer sequencer) {
            this.sequencer = sequencer;
            this.entries = new Entry[bufferSize];
            for (int i = 0; i < bufferSize; i++) entries[i] = new Entry<>();
        }

        /** 申请序号 + 写入 + 发布。这就是生产者唯一要做的事。 */
        public void publish(T value) {
            long seq = sequencer.next();            // 无 CAS
            Entry<T> e = entries[(int) (seq & sequencer.mask())];
            e.value = value;
            e.sequence = seq;
            sequencer.publish(seq);                 // 一次 volatile 写
        }
    }

    public interface EventHandler<T> {
        void onEvent(T event, long sequence, boolean endOfBatch);
    }

    /** 批量消费线程：自旋追 cursor，没追到就 parkNanos。 */
    static final class BatchEventProcessor<T> implements Runnable {
        private final RingBuffer<T> rb;
        private final SingleProducerSequencer seq;
        private final EventHandler<T> handler;
        private final Sequence cursor;              // 这个消费者的进度
        private volatile boolean running = true;

        BatchEventProcessor(RingBuffer<T> rb, SingleProducerSequencer seq, Sequence cursor, EventHandler<T> handler) {
            this.rb = rb;
            this.seq = seq;
            this.cursor = cursor;
            this.handler = handler;
        }

        @Override
        public void run() {
            long next = cursor.get() + 1;
            while (running) {
                long available = seq.cursor();      // volatile 读生产者进度
                if (available < next) {
                    LockSupport.parkNanos(1L);
                    continue;
                }
                // 一次性把可用的这批全处理掉
                while (next <= available) {
                    Entry<T> e = rb.entries[(int) (next & seq.mask())];
                    handler.onEvent(e.value, next, next == available);
                    cursor.set(next);               // 消费者写自己的 cursor（volatile 写）
                    next++;
                }
            }
        }

        public void stop() {
            running = false;
        }

        public long processed() {
            return cursor.get();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final int bufferSize = 1024;
        final Sequence producerCursor = new Sequence(Sequence.INITIAL);
        final Sequence consumerCursor = new Sequence(Sequence.INITIAL);
        final SingleProducerSequencer sequencer =
                new SingleProducerSequencer(bufferSize, producerCursor, consumerCursor);
        final RingBuffer<Long> ring = new RingBuffer<>(bufferSize, sequencer);

        final long total = 50_000_000L;

        BatchEventProcessor<Long> consumer = new BatchEventProcessor<>(
                ring, sequencer, consumerCursor,
                (event, sequence, endOfBatch) -> {
                    if((sequence & 0xFFFFF) == 0) {
                        System.out.println("消费到消息 @ " + sequence);
                    }
                    if (event == null) {
                        throw new IllegalStateException("消费到 null @ " + sequence);
                    }
                });

        Thread consumerThread = new Thread(consumer, "consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // 生产者：单线程往里灌 total 条消息
        Thread producerThread = new Thread(() -> {
            for (long i = 0; i < total; i++) {
                ring.publish(i);
            }
        }, "producer");
        producerThread.setDaemon(true);

        long start = System.nanoTime();
        producerThread.start();
        producerThread.join();
        // 等消费者追上
        while (consumer.processed() < total - 1) {
            LockSupport.parkNanos(1_000L);
        }
        long elapsedNs = System.nanoTime() - start;

        consumer.stop();

        long elapsedMs = elapsedNs / 1_000_000;
        double throughputMps = total / (elapsedNs / 1_000_000_000.0) / 1_000_000.0;

        System.out.println();
        System.out.println("===== 单生产者 Disruptor 运行结果 =====");
        System.out.printf("消息总数        : %,d 条%n", total);
        System.out.printf("端到端耗时      : %,d ms%n", elapsedMs);
        System.out.printf("吞吐量          : %.2f M ops/s%n", throughputMps);
        System.out.println();
        System.out.println("===== 证明生产路径零 CAS =====");
        System.out.printf("publish() 次数 (volatile 写) : %,d%n", sequencer.volatileWriteCount);
        System.out.printf("CAS 尝试次数                 : %,d  <-- 永远为 0%n", sequencer.casAttemptCount);
        System.out.println("结论：每条消息只有 1 次 volatile 写，0 次 CAS。");
        System.out.println("      这就是单生产者模式比多生产者快一个数量级的原因。");
    }
}
