package io.ddia.disruptor.lab.falsesharing;

import io.ddia.disruptor.lab.singleproducer.Sequence;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

/**
 * 用 JOL 把对象字段偏移量打出来，肉眼确认 cache line 归属。
 *
 * 预期输出片段：
 *   Plain  对象里 v0/v1/v2/v3 偏移 16/24/32/40  -> 全在一条 64B line 内（伪共享）
 *   Padded 对象里 v0..v3 偏移隔 64 字节         -> 各占一条 line（无伪共享）
 *   Sequence.value 偏移 56                      -> 被 padding 推到独占一条 line
 *
 * 运行：
 *   mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.falsesharing.ObjectLayoutDemo
 *
 * 注意：要看到压缩指针关闭下的真实偏移，可加 -XX:-UseCompressedOops。
 */
public class ObjectLayoutDemo {

    static class Plain {
        volatile long v0;
        volatile long v1;
        volatile long v2;
        volatile long v3;
    }

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

    public static void main(String[] args) {
        System.out.println("================ Plain（不填充）================");
        System.out.println(ClassLayout.parseInstance(new Plain()).toPrintable());

        System.out.println("================ Padded（每值独占 cache line）================");
        System.out.println(ClassLayout.parseInstance(new Padded()).toPrintable());

        System.out.println("================ Disruptor 风格 Sequence ==================");
        System.out.println(ClassLayout.parseInstance(new Sequence()).toPrintable());

        System.out.println("================ 整图大小 ==================");
        System.out.println(GraphLayout.parseInstance(new Padded()).toFootprint());
    }
}
