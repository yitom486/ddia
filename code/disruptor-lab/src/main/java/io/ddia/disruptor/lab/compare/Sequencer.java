package io.ddia.disruptor.lab.compare;

/**
 * 抽象出"序号分配器"的最小契约，对比 demo 用。
 *
 * 实现：
 *   - SingleProducerSequencer : zero-CAS 路径
 *   - MultiProducerSequencer  : CAS 抢序号路径
 */
    public interface Sequencer {
        /** 申请一个可写槽位的序号。 */
        long next();

        /** 告知消费者：序号 sequence 已经发布。 */
        void publish(long sequence);

        /** 当前已发布到的最大序号。消费者用来探测"有没有新东西"。 */
        long cursor();

        /** RingBuffer 取模掩码 = bufferSize - 1。 */
        long mask();

        /** RingBuffer 容量。 */
        default int bufferSize() { return (int) (mask() + 1L); }

        /** publish() 被调用次数（= 写入 volatile 的次数）。 */
        long volatileWriteCount();

        /** CAS 尝试次数（= next() 里 CAS 调用的次数）。 */
        long casAttemptCount();

        /**
         * 多生产者专用：检查序号 sequence 对应的槽位是否已发布。
         * 单生产者可视为总是 true（但本方法可能仍抛 UnsupportedOperationException）。
         */
        default boolean isAvailable(long sequence) {
            // 单生产者路径不需要这个
            throw new UnsupportedOperationException("isAvailable 仅多生产者支持");
        }
    }