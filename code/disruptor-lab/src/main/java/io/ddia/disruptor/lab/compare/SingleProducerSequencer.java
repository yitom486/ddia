package io.ddia.disruptor.lab.compare;

/**
 * 单生产者实现：实现 Sequencer 接口，暴露计数器。
 *
 * 与 singleproducer/SingleProducerSequencer 完全等价，但作为接口实现以便复用。
 */
public class SingleProducerSequencer implements Sequencer {

    private final int bufferSize;
    private final long mask;
    private final Sequence cursor;
    private final Sequence[] gatingSequences;

    private long nextValue = Sequence.INITIAL;
    private long cachedGating = Sequence.INITIAL;

    public long volatileWriteCount;
    public long casAttemptCount;

    public SingleProducerSequencer(int bufferSize, Sequence cursor, Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize 必须是 2 的幂: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1L;
        this.cursor = cursor;
        this.gatingSequences = gatingSequences;
    }

    @Override
    public long next() {
        long next = nextValue + 1;
        long wrapPoint = next - bufferSize;
        long cached = cachedGating;

        if (wrapPoint > cached || cached > nextValue) {
            long min = minGating();
            while (wrapPoint > min) {
                Thread.onSpinWait();
                min = minGating();
            }
            cachedGating = min;
        }

        nextValue = next;
        return next;
    }

    @Override
    public void publish(long sequence) {
        cursor.set(sequence);
        volatileWriteCount++;
    }

    @Override
    public long cursor() { return cursor.get(); }
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
