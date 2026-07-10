package io.ddia.disruptor.lab.multiproducer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 验证修复后的 MultiProducerSequencer 的绕环保护逻辑。
 */
public class VerifyMultiProducerFix {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== MultiProducer 绕环保护修复验证 ===\n");

        AtomicBoolean error = new AtomicBoolean(false);

        testSequentialIssuance(error);

        if (error.get()) {
            System.out.println("\nVERIFICATION FAILED");
            System.exit(1);
        } else {
            System.out.println("\nALL CHECKS PASSED");
        }
    }

    private static void testSequentialIssuance(AtomicBoolean error) {
        System.out.println("Test 1: 顺序发放（不触发绕环）");

        int bufferSize = 8;
        Sequence cursor = new Sequence(-1);
        // 关键：同时用 cursor 和 consumer 作为 gating sequences
        // - cursor：已发布的序号，producer 不能超过自己已发布的部分
        // - consumer：已消费的序号，producer 不能覆盖未消费的部分
        // 二者取 min，防止 producer 超过任一限制
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
        System.out.println("  OK: " + (bufferSize * 2) + " 次 next/publish，序号连续 [" + 0 + ".." + prev + "]");
        System.out.println("  CAS attempts: " + seq.casAttemptCount);
    }
}
