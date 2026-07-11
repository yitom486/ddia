package io.ddia.disruptor.lab.multiproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 多生产者修复验证：顺序发号、乱序发布不倒退、真实消费者连续推进。
 *
 * 运行：
 *   mvn -q -f code/disruptor-lab/pom.xml exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.ddia.disruptor.lab.multiproducer.VerifyMultiProducerFix
 */
public class VerifyMultiProducerFix {

    public static void main(String[] args) throws Exception {
        System.out.println("=== MultiProducer 修复验证 ===\n");

        AtomicBoolean error = new AtomicBoolean(false);

        testSequentialIssuance(error);
        testOutOfOrderPublishDoesNotRetreatCursor(error);
        testConsumerAdvancesThroughOutOfOrderPublish(error);
        testEventEqualsSequenceUnderConcurrency(error);

        if (error.get()) {
            System.out.println("\nVERIFICATION FAILED");
            System.exit(1);
        } else {
            System.out.println("\nALL CHECKS PASSED");
        }
    }

    /** Test 1：顺序 next/publish，序号连续。 */
    private static void testSequentialIssuance(AtomicBoolean error) {
        System.out.println("Test 1: 顺序发放（不触发绕环）");

        int bufferSize = 8;
        Sequence cursor = new Sequence(-1);
        Sequence consumer = new Sequence(Long.MAX_VALUE);
        MultiProducerSequencer seq = new MultiProducerSequencer(bufferSize, cursor, cursor, consumer);

        long prev = -1;
        for (int i = 0; i < bufferSize * 2; i++) {
            long result = seq.next();
            seq.publish(result);
            if (result != prev + 1) {
                System.out.println("  [ERROR] 序号不连续: " + prev + " -> " + result);
                error.set(true);
            }
            prev = result;
        }
        System.out.println("  OK: " + (bufferSize * 2) + " 次 next/publish，序号连续 [0.." + prev + "]");
    }

    /**
     * Test 2：确定性乱序发布。
     * 先申请 0、1，先 publish(1) 再 publish(0)，断言 cursor 不得从 1 回退到 0。
     */
    private static void testOutOfOrderPublishDoesNotRetreatCursor(AtomicBoolean error) {
        System.out.println("Test 2: 乱序发布不倒退 cursor");

        int bufferSize = 8;
        Sequence cursor = new Sequence(-1);
        Sequence gating = new Sequence(Long.MAX_VALUE);
        MultiProducerSequencer seq = new MultiProducerSequencer(bufferSize, cursor, gating);

        long s0 = seq.next();
        long s1 = seq.next();
        if (s0 != 0 || s1 != 1) {
            System.out.println("  [ERROR] 期望申请到 0/1，实际 " + s0 + "/" + s1);
            error.set(true);
            return;
        }

        seq.publish(s1);
        long afterFirst = seq.cursor();
        if (afterFirst != 1) {
            System.out.println("  [ERROR] publish(1) 后 cursor 应为 1，实际 " + afterFirst);
            error.set(true);
            return;
        }

        seq.publish(s0);
        long afterSecond = seq.cursor();
        if (afterSecond != 1) {
            System.out.println("  [ERROR] publish(0) 后 cursor 倒退为 " + afterSecond + "（应为 1）");
            error.set(true);
            return;
        }

        System.out.println("  OK: publish(1)→cursor=1，再 publish(0)→cursor 仍为 1（不倒退）");
    }

    /**
     * Test 3：乱序发布后，真实消费者仍能连续消费 0 和 1。
     */
    private static void testConsumerAdvancesThroughOutOfOrderPublish(AtomicBoolean error)
            throws InterruptedException {
        System.out.println("Test 3: 乱序发布后消费者连续推进");

        int bufferSize = 8;
        Sequence producerCursor = new Sequence(-1);
        Sequence consumerCursor = new Sequence(-1);
        MultiProducerSequencer sequencer =
                new MultiProducerSequencer(bufferSize, producerCursor, consumerCursor);
        MiniMultiProducerDemo.RingBuffer<Long> ring =
                new MiniMultiProducerDemo.RingBuffer<>(bufferSize, sequencer);

        AtomicReference<AssertionError> fail = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        MiniMultiProducerDemo.BatchEventProcessor<Long> consumer =
                new MiniMultiProducerDemo.BatchEventProcessor<>(
                        ring, sequencer, consumerCursor,
                        (event, sequence, endOfBatch) -> {
                            if (event == null || event.longValue() != sequence) {
                                fail.set(new AssertionError(
                                        "wrong event: sequence=" + sequence + ", event=" + event));
                                done.countDown();
                                return;
                            }
                            if (sequence >= 1) {
                                done.countDown();
                            }
                        });

        Thread consumerThread = new Thread(consumer, "verify-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        long s0 = sequencer.next();
        long s1 = sequencer.next();
        writeAndMark(ring, sequencer, s1);
        writeAndMark(ring, sequencer, s0);

        if (!done.await(3, TimeUnit.SECONDS)) {
            System.out.println("  [ERROR] 消费者 3s 内未推进到 sequence=1；"
                    + " processed=" + consumer.processed()
                    + " producerCursor=" + sequencer.cursor()
                    + " isAvailable(1)=" + ring.available.isAvailable(1));
            error.set(true);
        } else if (fail.get() != null) {
            System.out.println("  [ERROR] " + fail.get().getMessage());
            error.set(true);
        } else if (consumer.processed() < 1) {
            System.out.println("  [ERROR] processed=" + consumer.processed() + "，期望 >= 1");
            error.set(true);
        } else {
            System.out.println("  OK: 先发 1 再发 0 后，消费者连续消费到 processed="
                    + consumer.processed());
        }

        consumer.stop();
        consumerThread.join(1000);
    }

    /**
     * Test 4：多线程并发生产，消费者严格校验 event == sequence，并跑完一小批。
     */
    private static void testEventEqualsSequenceUnderConcurrency(AtomicBoolean error)
            throws InterruptedException {
        System.out.println("Test 4: 并发生产 + event==sequence 校验");

        int bufferSize = 64;
        int producerCount = 3;
        long perProducer = 2_000L;
        long total = producerCount * perProducer;

        Sequence producerCursor = new Sequence(-1);
        Sequence consumerCursor = new Sequence(-1);
        MultiProducerSequencer sequencer =
                new MultiProducerSequencer(bufferSize, producerCursor, consumerCursor);
        MiniMultiProducerDemo.RingBuffer<Long> ring =
                new MiniMultiProducerDemo.RingBuffer<>(bufferSize, sequencer);

        AtomicReference<AssertionError> fail = new AtomicReference<>();
        MiniMultiProducerDemo.BatchEventProcessor<Long> consumer =
                new MiniMultiProducerDemo.BatchEventProcessor<>(
                        ring, sequencer, consumerCursor,
                        (event, sequence, endOfBatch) -> {
                            if (event == null || event.longValue() != sequence) {
                                fail.compareAndSet(null, new AssertionError(
                                        "wrong event: sequence=" + sequence + ", event=" + event));
                            }
                        });

        Thread consumerThread = new Thread(consumer, "verify-consumer-concurrent");
        consumerThread.setDaemon(true);
        consumerThread.start();

        Thread[] producers = new Thread[producerCount];
        for (int i = 0; i < producerCount; i++) {
            producers[i] = new Thread(() -> {
                for (long j = 0; j < perProducer; j++) {
                    long seq = sequencer.next();
                    writeAndMark(ring, sequencer, seq);
                }
            }, "verify-producer-" + i);
            producers[i].start();
        }
        for (Thread t : producers) {
            t.join(10_000);
            if (t.isAlive()) {
                System.out.println("  [ERROR] 生产者 join 超时");
                error.set(true);
                consumer.stop();
                return;
            }
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (consumer.processed() < total - 1 && System.nanoTime() < deadline) {
            if (fail.get() != null) {
                break;
            }
            LockSupport.parkNanos(100_000L);
        }

        if (fail.get() != null) {
            System.out.println("  [ERROR] " + fail.get().getMessage());
            error.set(true);
        } else if (consumer.processed() < total - 1) {
            long next = consumer.processed() + 1;
            System.out.println("  [ERROR] 未消费完: processed=" + consumer.processed()
                    + " total-1=" + (total - 1)
                    + " producerCursor=" + sequencer.cursor()
                    + " isAvailable(next)=" + ring.available.isAvailable(next));
            error.set(true);
        } else {
            System.out.println("  OK: " + total + " 条全部消费且 event==sequence；"
                    + " CAS attempts=" + sequencer.casAttemptCount.get());
        }

        consumer.stop();
        consumerThread.join(1000);
    }

    private static void writeAndMark(MiniMultiProducerDemo.RingBuffer<Long> ring,
                                     MultiProducerSequencer sequencer,
                                     long seq) {
        MiniMultiProducerDemo.Entry<Long> e =
                ring.entries[(int) (seq & sequencer.mask())];
        e.value = seq;
        e.sequence = seq;
        ring.available.set(seq);
        sequencer.publish(seq);
    }
}
