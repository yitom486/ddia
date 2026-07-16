# disruptor-lab

`docs/mq-learning/Disruptor详解.md` 的配套可运行 demo。三个实验，分别验证文档里三个最关键的论断：

1. **单生产者模式完全不用 CAS** — `singleproducer/`
2. **伪共享会带来 6-7 倍性能差距** — `falsesharing/`
3. **单生产者 vs 多生产者 直接对比** — `compare/`

## 环境

- JDK 17+
- Maven 3.6+

## 实验 1：单生产者零 CAS

一个最小可运行的单生产者 Disruptor：1 个生产者线程 → RingBuffer → 1 个消费者线程，跑 5000 万条消息后打印吞吐量，并打印两个计数器证明生产路径**零 CAS**：

```
mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.singleproducer.MiniSingleProducerDemo
```

预期输出形如：

```
===== 单生产者 Disruptor 运行结果 =====
消息总数        : 50,000,000 条
端到端耗时      : 4,933 ms
吞吐量          : 10.13 M ops/s

===== 证明生产路径零 CAS =====
publish() 次数 (volatile 写) : 50,000,000
CAS 尝试次数                 : 0  <-- 永远为 0
结论：每条消息只有 1 次 volatile 写，0 次 CAS。
```

> 吞吐数值取决于机器核数与 WSL2/裸机差异，本 demo 的重点不是绝对吞吐，
> 而是 **`publish() 次数 == 消息数` 且 `CAS 尝试次数 == 0`**。

**看点**：`SingleProducerSequencer.next()` 里
- `nextValue` / `cachedGating` 是**普通 long**（连 volatile 都不是），因为只有生产者线程会读写它们；
- `publish()` 里只有一次 `cursor.set(seq)` —— 这是一条 volatile 写，不是 CAS。

对照多生产者必须 `compareAndSet` 抢序号，单生产者把"原子化"从"每条消息 CAS"降到"每条消息一次 volatile 写"。

## 实验 2：伪共享

### 2a. 跑 JMH 基准，看吞吐差距

4 个线程各自递增一个计数器，分 `Plain`（4 个 long 挤一条 cache line）和 `Padded`（每个 long 独占一条 cache line）两种布局：

```
mvn -q package
java -jar target/disruptor-lab-1.0.0-SNAPSHOT.jar FalseSharingBench
```

预期 `padded` 吞吐约为 `plain` 的 3-7 倍（依 CPU 核数与缓存架构而定）。

**实测（WSL2, JDK 17, 2 forks × 5 次测量）**：

| Benchmark | Throughput (ops/ms) |
|-----------|--------------------|
| `padded` 合计 | 2,334,979 ± 101,219 |
| `plain`  合计 |   134,702 ±   8,421 |
| **差距** | **~17×** |

WSL2 上因缓存/调度特性，差距往往比裸机（通常 6-7×）更夸张，但方向稳定：**有 padding 的版本把 4 个值分散到 4 条 cache line，互不打扰；没 padding 的版本 4 个值互相把对方的 cache line 顶失效，每次写都触发跨核同步。**

### 2b. 用 JOL 看 cache line 归属

把对象的字段偏移量打出来，肉眼确认 4 个值是否落在同一条 64 字节 cache line：

```
mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.falsesharing.ObjectLayoutDemo
```

预期看到：
- `Plain`：`v0/v1/v2/v3` 偏移 16/24/32/40 → 全在一条 64B line 内
- `Padded`：`v0..v3` 偏移 72/136/200/264 → 间隔 64 字节，各占一条 line
- `Sequence.value` 偏移 72 → 被前后 padding 推到独占一条 line（line 内其余字段都是冷 padding）

> 想看更"真实"的偏移可加 `-XX:-UseCompressedOops` 关闭压缩指针。

## 实验 3：单生产者 vs 多生产者 直接对比

控制变量（两边完全相同）：bufferSize、消息总数、消费者线程数、消息体、warm-up 策略。
自变量：生产者线程数（1 vs 4）+ 对应 Sequencer 实现。

> 注意：下面的命令默认你**已经在 `code/disruptor-lab/` 目录里**。如果从仓库根 `/home/tomy/projects/javaProjects/ddia` 跑，要在前面加 `-f code/disruptor-lab/pom.xml`，否则 Maven 会报 *"there is no POM in this directory"*。
>
> 如果是**新加的类**（比如对比 demo 这种），先跑一遍 `mvn compile` 把新类编译进 `target/classes`，否则 exec 会报 `ClassNotFoundException`：

```
# 方式 A（推荐，仓库根可直接跑）
mvn -q -f code/disruptor-lab/pom.xml compile exec:java -Dexec.mainClass=io.ddia.disruptor.lab.compare.SingleVsMultiProducerDemo

# 方式 B（先 cd 再跑）
cd code/disruptor-lab
mvn -q compile exec:java -Dexec.mainClass=io.ddia.disruptor.lab.compare.SingleVsMultiProducerDemo
```

快速跑 400 万条（单生产者 400 万；多生产者 4 × 100 万）：

```
mvn -q -f code/disruptor-lab/pom.xml compile exec:java \
  -Dexec.mainClass=io.ddia.disruptor.lab.compare.SingleVsMultiProducerDemo \
  -Ddisruptor.perProducer=1000000
```

多生产者实现使用两阶段协议：`next()` CAS 推进单调的 claimed cursor，`publish()`
按“槽位 + 环次”写入原子 availability flag。消费者以 cursor 为扫描上界，再逐条检查
availability，既支持乱序发布形成的空洞，也不会把上一圈残留状态误认为下一圈已经发布。

预期看到（实际数字依机器而异，关键看**比值**）：

```
┌─────────────┬──────────────┬──────────────┐
│     指标    │  单生产者     │  多生产者(4)  │
├─────────────┼──────────────┼──────────────┤
│ 吞吐 Mops/s │        10.77 │         5.43 │
│ CAS 次数    │             0 │    68,054,749 │
│ CAS/消息    │         0.000 │         1.701 │
└─────────────┴──────────────┴──────────────┘
吞吐量比值：单 / 多 ≈ 1.98x
```

**怎么看这个表**：
- `CAS 次数`：单生产者永远 0；多生产者 ≥ 消息总数（每次 `next()` 至少 1 次 CAS，冲突时会更多）
- `CAS/消息`：多生产者的核心成本指标。比值 ≈ 1 是"完全无冲突"，比值 > 1 是"有 CAS 重试"
- 吞吐倍数差：单生产者的 publisher 路径只有 1 次 volatile 写，多生产者要 1 次 CAS（`lock cmpxchg`），后者在 x86 上多一道 cache 同步成本

## 目录结构

```
disruptor-lab/
├── pom.xml
└── src/main/java/io/ddia/disruptor/lab/
    ├── singleproducer/
    │   ├── Sequence.java                 # 带 cache line padding 的 volatile long
    │   ├── SingleProducerSequencer.java  # 零 CAS 序号分配器（核心）
    │   └── MiniSingleProducerDemo.java   # 可运行 demo + 计数器证明
    ├── multiproducer/                    # 早期写出的多生产者参考实现（独立目录）
    │   ├── MultiProducerSequencer.java
    │   └── MiniMultiProducerDemo.java
    ├── compare/                          # 单 vs 多 直接对比
    │   ├── Sequencer.java                # 抽象接口
    │   ├── Sequence.java
    │   ├── SingleProducerSequencer.java  # implements Sequencer
    │   ├── MultiProducerSequencer.java   # implements Sequencer
    │   └── SingleVsMultiProducerDemo.java
    └── falsesharing/
        ├── FalseSharingBench.java        # JMH 基准：Plain vs Padded
        └── ObjectLayoutDemo.java         # JOL 打印字段偏移
```
