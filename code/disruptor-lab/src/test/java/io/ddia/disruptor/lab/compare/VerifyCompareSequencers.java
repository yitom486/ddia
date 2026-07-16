package io.ddia.disruptor.lab.compare;

import java.util.concurrent.atomic.AtomicLong;

/** 确定性验证 compare/ 多生产者协议的关键不变量。 */
public final class VerifyCompareSequencers {

    private VerifyCompareSequencers() {}

    public static void main(String[] args) throws Exception {
        verifyOutOfOrderPublishDoesNotMoveCursorBackward();
        verifyAvailabilityDistinguishesRingGenerations();
        verifyConcurrentEndToEndRun();
        System.out.println("ALL COMPARE SEQUENCER CHECKS PASSED");
    }

    private static void verifyOutOfOrderPublishDoesNotMoveCursorBackward() {
        Sequence consumer = new Sequence(Sequence.INITIAL);
        MultiProducerSequencer sequencer =
                new MultiProducerSequencer(8, new AtomicLong(Sequence.INITIAL), consumer);

        long sequence0 = sequencer.next();
        long sequence1 = sequencer.next();
        require(sequence0 == 0 && sequence1 == 1, "申请序号必须连续");

        sequencer.publish(sequence1);
        sequencer.publish(sequence0);

        require(sequencer.cursor() == 1, "乱序发布不能让 claimed cursor 倒退");
        require(sequencer.isAvailable(0), "sequence 0 应已发布");
        require(sequencer.isAvailable(1), "sequence 1 应已发布");
    }

    private static void verifyAvailabilityDistinguishesRingGenerations() {
        Sequence consumer = new Sequence(Sequence.INITIAL);
        MultiProducerSequencer sequencer =
                new MultiProducerSequencer(4, new AtomicLong(Sequence.INITIAL), consumer);

        for (long expected = 0; expected < 4; expected++) {
            long sequence = sequencer.next();
            require(sequence == expected, "第一圈序号错误: " + sequence);
            sequencer.publish(sequence);
        }

        consumer.set(0);
        long secondGeneration = sequencer.next();
        require(secondGeneration == 4, "第二圈第一个序号应为 4");
        require(!sequencer.isAvailable(4), "sequence 4 尚未发布，不能继承 sequence 0 的标记");

        sequencer.publish(4);
        require(sequencer.isAvailable(4), "sequence 4 发布后必须可见");
        require(!sequencer.isAvailable(0), "槽位进入第二圈后，旧环次不能继续显示可用");
    }

    private static void verifyConcurrentEndToEndRun() throws Exception {
        long perProducer = 25_000L;
        SingleVsMultiProducerDemo.Result result = SingleVsMultiProducerDemo.runOnce(
                "verify-multi", 4, perProducer, 64, true);

        long total = 4L * perProducer;
        require(result.totalMessages == total, "消息总数错误");
        require(result.volatileWrites == total, "每条消息必须恰好发布一次");
        require(result.casAttempts >= total, "每次申请至少执行一次 CAS");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
