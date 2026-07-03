package io.ddia.disruptor.lab.falsesharing;

import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;

/**
 * 伪共享（False Sharing）微基准。
 *
 * 4 个线程各自疯狂递增一个计数器，分两种内存布局对比：
 *   - Plain  : 4 个 volatile long 紧挨着，挤在同一条 64B cache line -> 互相伪共享
 *   - Padded : 每个计数器前后各 7 个 long (7*8=56B)，独占一条 cache line -> 无伪共享
 *
 * 预期：Padded 比 Plain 快约 3-7 倍（依核数和缓存架构而定）。
 *
 * 运行：
 *   mvn -q package
 *   java -jar target/disruptor-lab-1.0.0-SNAPSHOT.jar FalseSharingBench
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
public class FalseSharingBench {

    /** 不填充：4 个 long 挤在一条 cache line 里，互相伪共享。 */
    static class Plain {
        volatile long v0;
        volatile long v1;
        volatile long v2;
        volatile long v3;
    }

    /** 每个值前后各 7 个 long，让它独占一条 64B cache line。 */
    static class Padded {
        long p00, p01, p02, p03, p04, p05, p06;
        volatile long v0;
        long p10, p11, p12, p13, p14, p15, p16;
        volatile long v1;
        long p20, p21, p22, p23, p24, p25, p26;
        volatile long v2;
        long p30, p31, p32, p33, p34, p35, p36;
        volatile long v3;
        long p40, p41, p42, p43, p44, p45, p46;
    }

    private final Plain plain = new Plain();
    private final Padded padded = new Padded();

    // ---- Plain 组：4 个线程各写各的，但 cache line 互相顶 ----
    @Benchmark @Group("plain") @GroupThreads(1)
    public long plain0() { return plain.v0++; }

    @Benchmark @Group("plain") @GroupThreads(1)
    public long plain1() { return plain.v1++; }

    @Benchmark @Group("plain") @GroupThreads(1)
    public long plain2() { return plain.v2++; }

    @Benchmark @Group("plain") @GroupThreads(1)
    public long plain3() { return plain.v3++; }

    // ---- Padded 组：4 个线程各写各的，cache line 互不打扰 ----
    @Benchmark @Group("padded") @GroupThreads(1)
    public long padded0() { return padded.v0++; }

    @Benchmark @Group("padded") @GroupThreads(1)
    public long padded1() { return padded.v1++; }

    @Benchmark @Group("padded") @GroupThreads(1)
    public long padded2() { return padded.v2++; }

    @Benchmark @Group("padded") @GroupThreads(1)
    public long padded3() { return padded.v3++; }
}
