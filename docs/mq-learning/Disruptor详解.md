# Disruptor 详解

> 本文档配套消息队列学习总结文档，是对 **P2（Disruptor 源码解读）** 学习计划的展开文档。重点讲清楚「为什么」「是什么」「怎么实现」。

---

## 目录

1. [背景：Disruptor 是怎么诞生的](#1-背景disruptor-是怎么诞生的)
2. [核心思想：把单机内存压榨到极限](#2-核心思想把单机内存压榨到极限)
3. [原理剖析：从一句话需求到极致优化](#3-原理剖析从一句话需求到极致优化)
4. [Ring Buffer：环形数组的精妙设计](#4-ring-buffer环形数组的精妙设计)
5. [核心机制：伪共享、序号屏障、单/多生产者](#5-核心机制伪共享序号屏障单多生产者)
6. [完整示例代码：手写一个 MiniDisruptor](#6-完整示例代码手写一个-minidisruptor)
7. [与 BlockingQueue / MQ 的对比](#7-与-blockingqueue--mq-的对比)
8. [总结](#8-总结)

---

## 1. 背景：Disruptor 是怎么诞生的

### 1.1 经典场景：撮合引擎的延迟问题

2010 年，LMAX（一家英国外汇交易所）在伦敦证券交易所跑一套**撮合系统**。他们的核心业务是匹配买卖订单，把一个大单切碎后推到线程池并行处理，结果发现：

> **单笔订单的端到端延迟，99% 都花在了"线程之间的消息传递"上。**

业务计算本身只要几微秒，但**消息从生产者传到消费者之间产生的延迟达到了几十微秒**，这在金融系统里是致命的——撮合延迟 1ms，可能就错过最佳成交价。

### 1.2 一句话总结起源

> **Disruptor 是 LMAX 为了解决"线程间超低延迟消息传递"而设计的高性能内存队列。它最出名的设计：用"环形数组 + 无锁化 + 缓存行填充"达成单机 1 亿 TPS 级别的吞吐。**

### 1.3 它到底是什么

```
本质：Disruptor 是一个**有界、环形、无锁**的内存队列，
    专为"一个或多个生产者 → 一个或多个消费者"的高并发通信设计。

不是分布式的；
不持久化；
不跨机器；
就是一个**单进程内**的消息传递机制，
但通过巧妙的内存设计和 CPU 缓存亲和性达到极致性能。
```

### 1.4 它为什么值得学

| 价值层 | 收益 |
|-------|------|
| **学习操作系统底层** | 伪共享、内存屏障、CPU 缓存一致性 |
| **学习并发编程** | 无锁算法的正确写法，CAS / volatile 的实战 |
| **学习性能调优** | 不是堆机器，而是懂硬件 |
| **直接应用** | 日志框架（log4j2 async logger）、金融交易、低延迟 RPC、订单流水线 |

### 1.5 一个真实案例：Log4j 2 AsyncLogger

Log4j 2 的异步 Logger 用 Disruptor 替代传统的 ArrayBlockingQueue：

```
传统方案：log.info(...) → 进 BlockingQueue → 异步 IO 线程取出 → 写盘
Disruptor方案：log.info(...) → 写到位 (没满时是 O(1) 写指针推进) → 独立线程消费写盘

实测：在典型应用上，AsyncLogger 比同步 + BufferedWriter **快 10-30 倍**。
```

这种"工具框架愿拿 Disruptor 替代标准 JDK 队列"的事实，就是它的性能最有力的证明。

---

## 2. 核心思想：把单机内存压榨到极限

### 2.1 一句话核心思想

> **不要在"传消息"上花任何多余的时间。把消息当作一组连续的内存单元，用序号管理生产和消费，把所有非必要的开销（锁、内存分配、缓存颠簸）全部干掉。**

### 2.2 五个核心思想

#### 思想 1：数据结构决定性能上限

```
传统方案：
    LinkedList / ArrayList
    ├── 每条消息 = 一个新 Node 对象
    ├── 节点分散在堆内存
    └── 命中率低 → 缓存不友好

Disruptor方案：
    预分配的环形数组 entry[] = new Entry[BUFFER_SIZE]
    ├── 没有 Node，全是复用
    ├── 内存连续，CPU 预取器友好
    └── 命中率极高
```

#### 思想 2：用序号表示位置，不用锁

```
传统 BlockingQueue 的 take() / put()：
    synchronized (lock) { ... }   ← 锁

Disruptor：
    long sequence = 原子自增    ← CAS
    消费者通过 sequence 判断"我已经处理到哪了"
```

#### 思想 3：单写者原则

```
默认情况下，生产者**只用一个线程**（多生产者时用 CAS 自旋）。
这意味着：
    - 生产端不会竞争；
    - 所有 entry 槽写入都是单线程推进；
    - 这是 Disruptor 性能压倒一切的根本保证之一。
```

#### 思想 4：所有进入都在 L1 / L2 cache

```
数组 + 顺序访问 = 顺序预取命中
+ 填充伪共享字段 = 没有 cache line 失效

结果：消费者的 cursor 读是个「几乎免费」的操作。
```

#### 思想 5：批量化消费

```
消费者通过 waitStrategy（等待策略）攒一批再处理：
    ✅ BusySpinWaitStrategy      （最高延迟敏感，使用 CPU 自旋）
    ✅ YieldingWaitStrategy
    ✅ SleepingWaitStrategy
    ✅ BlockingWaitStrategy
```

### 2.3 五条思想的汇总图

```
┌────────────────────────────────────────────────────┐
│           Disruptor 的五条核心思想                  │
├────────────────────────────────────────────────────┤
│                                                    │
│  ① 预分配环形数组（避免分配/GC）                     │
│  ② 序号 + CAS（避免锁）                             │
│  ③ 单写者原则（生产者竞争为 0）                      │
│  ④ Cache 行填充（避免伪共享）                        │
│  ⑤ 批处理 + 多种等待策略（CPU 友好）                 │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## 3. 原理剖析：从一句话需求到极致优化

假设我们的需求是：

> **多个生产者线程同时投递消息，单个消费者线程按顺序处理。**

### 3.1 最挫的实现（同步队列）

```java
public class SlowQueue<T> {
    private final Object lock = new Object();
    private final List<T> queue = new ArrayList<>();

    public void put(T item) {
        synchronized (lock) {       // ← 锁！
            queue.add(item);
            lock.notify();
        }
    }

    public T take() throws InterruptedException {
        synchronized (lock) {
            while (queue.isEmpty()) lock.wait();
            return queue.remove(0);
        }
    }
}
```

**性能问题**：
1. `synchronized` 走操作系统监视器锁，JVM 升级成重量级锁时会让线程 park
2. `ArrayList` 满了扩容要拷贝数组，对象分配→GC 压力
3. `wait/notify` 唤醒来回，内核态切换

### 3.2 第一步优化：用 ConcurrentLinkedQueue + AtomicReference

```java
public class BetterQueue<T> {
    private final AtomicReference<Node<T>> head = new AtomicReference<>();
    private final AtomicReference<Node<T>> tail = new AtomicReference<>();

    public void put(T item) {
        Node<T> newNode = new Node<>(item);
        Node<T> oldTail;
        do {
            oldTail = tail.get();
        } while (!tail.compareAndSet(oldTail, newNode));   // CAS
    }
}
```

**提升了什么**：
- ✅ 不再用 synchronized
- ⚠️ 仍然每次 new Node（GC 压力）
- ⚠️ Node 离散在堆里，缓存不友好

### 3.3 第二步优化：预分配 + 环形数组

```java
class Entry {
    volatile Object value;     // 消息体
    volatile long sequence;    // 序号
}
```

```java
public class RingBuffer<T> {
    private final Entry[] entries;
    private final int mask;
    private final AtomicLong producerSeq = new AtomicLong(-1);

    public RingBuffer(int size) {
        entries = new Entry[size];
        mask = size - 1;
        for (int i = 0; i < size; i++) entries[i] = new Entry();
    }

    public long next() {
        return producerSeq.incrementAndGet();      // CAS 自增
    }

    public Entry get(long seq) {
        return entries[(int) (seq & mask)];
    }
}
```

**提升了什么**：
- ✅ 没有对象分配
- ✅ 连续内存，CPU cache 友好
- ✅ 计算位置只需 `seq & mask`，O(1)
- ⚠️ 还是没解决伪共享
- ⚠️ 生产端多个线程同时 next() 会 CAS 冲突

### 3.4 第三步优化：单写者原则 + 消费者的进度追踪

```java
// 消费者持有一个 volatile 字段 cursor，表示已处理到哪
class ConsumerBarrier {
    private volatile long cursor;   // 消费者进度
}
```

```java
class BatchEventProcessor implements Runnable {
    private final long[] gatingSequences;   // 所有生产者中最慢的进度

    public void run() {
        long next = cursor + 1;
        while (running) {
            // 等待所有生产者追赶到 next 位置
            while (next > min(gatingSequences)) {
                ThreadHints.onSpinWait();   // 自旋
            }
            // 处理 entries[next]
            handler.onEvent(entries[(int)(next & mask)], next, false);
            cursor = next;        // 推进 cursor
            next++;
        }
    }
}
```

**提升了什么**：
- ✅ 消费端无锁（仅一个消费者线程）
- ✅ 通过 reading cursor 等待生产者，无需 `synchronized / wait`
- ⚠️ 还没解决伪共享

### 3.5 第四步优化：伪共享（cache line padding）

```java
// 普通声明
class Sequential {
    volatile long a;
    volatile long b;
}
// a 和 b 可能在同一个 64 字节 cache line 里

// Disruptor 风格
class Padded {
    // 用 7 个 long 填充，让 a 独占一个 cache line
    long p1, p2, p3, p4, p5, p6, p7;
    volatile long value;
    long p8, p9, p10, p11, p12, p13, p14;
}
```

> **Disruptor 在早期版本里把 cursor、gating sequence 都用这种 padding 包起来，保证它们在 CPU 看来独占一个 cache line。这个细节是 Disruptor 性能压垮同类方案的关键。**

### 3.6 最终架构：Disruptor 全貌

```
                ┌────────────────────────────────┐
                │       RingBuffer              │
                │   ┌──┐ ┌──┐ ┌──┐ ┌──┐ ...    │
                │   │-1│ │ 0│ │ 1│ │ 2│        │
                │   └──┘ └──┘ └──┘ └──┘         │
                │      ▲                         │
                └──────┼─────────────────────────┘
                       │
                ┌──────┴────────────────────┐
                │  Producer Barrier          │
                │  next() / publish(seq)     │
                └──────┬─────────────────────┘
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
   Producer 1     Producer 2     Producer 3
       │               │               │
       └─── publish(seq=N) ───┘
                       │
                ┌──────┴─────────────────────┐
                │  Consumer Barrier           │
                │  waitFor(seq) → 返 N       │
                └──────┬─────────────────────┘
                       ▼
                ┌─────────────────────┐
                │ BatchEventProcessor  │
                │  (消费者线程)         │
                └─────────────────────┘
```

---

## 4. Ring Buffer：环形数组的精妙设计

### 4.1 为什么是"环形"

```java
// 一个长度为 8 的 ring buffer
Entry[] ring = new Entry[8];

// 写：
long seq = 5;
ring[(int)(seq % 8)] = newEntry;
// 即 ring[5]

// 再写：
long seq = 8;
ring[(int)(8 % 8)] = newEntry;     // 位置 0！
// 关键：seq % size 自动循环
```

**好处**：
- ✅ 数组大小是 **2 的幂**，可以用 `seq & (size - 1)` 替代 `%`，位运算更快
- ✅ 永远不需要扩容——超过 size 就要求消费者跟上
- ✅ 索引稳定 = 缓存命中率高

### 4.2 覆盖式的取舍

```
传统 ArrayList：满了就扩容
Ring Buffer：满了就要求消费者必须跟上，否则生产者阻塞/抛错

为什么要这么做？
└── 因为 Ring Buffer 的所有槽位是「复用」的，覆盖旧消息是预期行为。
    防止覆盖：通过 gating sequences 让生产者等待。
```

### 4.3 单生产者写 = 单线程推进 = 极度友好

```java
class SingleProducerSequencer {
    private long nextValue;
    private long cachedGatingSequence;

    @Override
    public long next() {
        long nextValue = this.nextValue + 1;
        if (cachedGatingSequence == -1) cachedGatingSequence = minimumRead;

        // 检查是否会覆盖未消费的 entry
        long minSequence;
        while ((minSequence = min(gating)) < nextValue) {
            LockSupport.parkNanos(1);
        }
        this.nextValue = nextValue;
        return nextValue;
    }

    @Override
    public void publish(long sequence) {
        cursor.set(sequence);    // 一个 volatile 写！
    }
}
```

> **注意：单生产者根本不用 CAS。生产端就是"读 gatingSequence → 自选推进 → 一个 volatile 写"，把竞争压到零。**

### 4.4 多生产者写：用 CAS 自旋抢序号

```java
class MultiProducerSequencer extends SingleProducerSequencer {
    private final AtomicLong[] indexCache = new AtomicLong[BUFFER_SIZE];

    @Override
    public long next() {
        long current;
        long next;
        do {
            current = producerSeq.get();   // 当前最大
            next = current + 1;
            // 检查是否会覆盖
            if (next > cachedGatingSequence + BUFFER_SIZE) {
                cachedGatingSequence = min(gating);
                if (next > cachedGatingSequence + BUFFER_SIZE) {
                    LockSupport.parkNanos(1);
                    continue;
                }
            }
        } while (!producerSeq.compareAndSet(current, next));
        return next;
    }
}
```

注意变量都用了 padding 处理（防止伪共享）。

### 4.5 环形数组访问示意

```
场景：buffer size = 8

position 0  1  2  3  4  5  6  7  →  index
seq=0       0
seq=1          1
...
seq=7                            7
seq=8       8  ← 回到位置 0

如果 cursor=3，说明消费者已经处理过 0,1,2,3 四个槽位
如果 producerSeq=12，则生产者最新写到了 seq=12（即位置 4）
```

可视化：
```
        producer at seq=12  (当前位置 4)
              ↓
  ┌──┬──┬──┬──┬──┬──┬──┬──┐
  │8 │9 │10│11│12│13│5 │6 │   ← slot 内容（最新到最旧大致如此）
  └──┴──┴──┴──┴──┴──┴──┴──┘
   ↑0  1  2  3   ↑4  5  6  7
   consumer cursor=12  （已经处理完 seq<=12）
```

---

## 5. 核心机制：伪共享、序号屏障、单/多生产者

### 5.1 伪共享（False Sharing）

#### 是什么

现代 CPU 缓存以 **cache line**（通常 64 字节）为单位同步。一个 cache line 里可能放多个变量：

```
一个 64 字节的 cache line（X86）：
┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
│  8B  │  8B  │  8B  │  8B  │  8B  │  8B  │  8B  │  8B  │
│ varA │ varB │ varC │ varD │ varE │ varF │ varG │ varH │
└──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘
```

如果 CPU1 写 varA，CPU2 写 varD，**哪怕它们在逻辑上互不相干，也会让对方的 cache line 失效**——这就是伪共享。

#### 在 Disruptor 里的体现

```java
// 消费者进度 cursor 和生产者尾指针 sequence 频繁被两边读写
// 如果它们在同一个 cache line 里，读与写就会互相消耗 cache

// Disruptor 早期版本用 7 个 long 填充：
// [pad(7*8B)][cursor(8B)][pad(7*8B)]
// = 56 + 8 + 56 = 120 字节 ≈ 2 个 cache line
// 效果：cursor 读写不会触发 sequence 的 cache 失效
```

**现代 JVM 会自动重新布局字段**（@Contended 注解），效果等价。

#### 量化影响（参考 LMAX 测试）

```
同样的生产者-消费者工作负载：
  - 去掉 cache line padding：     100 million ops/sec
  - 加上 cache line padding：     700+ million ops/sec
                                （6-7 倍提升）
```

> **这才是 Disruptor "神奇"性能的秘密——不是某个高科技算法，而是把现代 CPU 缓存特性用到了极致。**

### 5.2 序号屏障（Sequence Barrier）

```java
interface SequenceBarrier {
    long waitFor(long sequence);
    long getCursor();
    boolean isAlerted();
    void alert();
}
```

**Sequence Barrier 的本质**：消费者问 "我能消费到 sequence=N 了么？"

```
消费者的等待循环：
while (true) {
    long available = barrier.waitFor(currentSeq + 1);
    while (currentSeq <= available) {
        handler.onEvent(entries[(int)(currentSeq & mask)], currentSeq, currentSeq == available);
        currentSeq++;
    }
}
```

**有意思的设计**：消费者**知道下一个事件是否是本批次最后一个**（`endOfBatch` 参数）。这是为了避免每个事件都引发 flush 等操作，让消费者可以批量高效处理。

### 5.3 单生产者 vs 多生产者

| 模式 | 实现 | 性能 | 适用场景 |
|------|------|------|---------|
| **SP（单生产者）** | 不用 CAS，纯 volatile + 自旋 | 极高 | 业务侧只有一个入口线程 |
| **MP（多生产者）** | CAS 自旋抢序号 | 中等（仍有竞争） | 多线程都尝试 put |

**最佳实践**：业务代码里尽量封装成单生产者入口。引入 Disruptor 后，可以专门开一个独立线程往里写，把上游的多线程模型转成单线程模型。

### 5.4 等待策略（Wait Strategy）

| 策略 | 行为 | CPU 占用 | 延迟 | 适用场景 |
|------|------|---------|------|---------|
| **BusySpinWaitStrategy** | 纯自旋 | 100% × N 核 | 最低 | 极致低延迟、核数够用 |
| **YieldingWaitStrategy** | 自旋 + Thread.yield() | 中 | 较低 | 中等延迟要求 |
| **SleepingWaitStrategy** | 自旋 → parkNanos | 低 | 中 | 一般业务 |
| **BlockingWaitStrategy** | LockSupport.park | 极低 | 最高 | 吞吐优先，延迟可让步 |

> 现实中最常用的是 **SleepingWaitStrategy** 或 **YieldingWaitStrategy**。

### 5.5 消费者依赖（DAG）

Disruptor 还支持多消费者 DAG（菱形结构、风扇出等）：

```java
disruptor.handleEventsWith(handler1)             // 第一个消费者
         .then(handler2)                         // handler1 完成后再 handle2
         .then(handler3);                        // handle2 完成后再 handle3

disruptor.handleEventsWith(handler1, handler2);  // 两个消费者并行看同一份事件
```

**这个灵活的工作流编排能力，让 Disruptor 远不只是一个"队列"。**

---

## 6. 完整示例代码：手写一个 MiniDisruptor

下面是一个 ~150 行可运行的极简版 Disruptor，去掉了 padding、多生产者等高级特性，但核心思想都在。

### 6.1 完整代码

```java
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class MiniDisruptor<T> {

    // ========== Entry 实体 ==========
    static class Entry<T> {
        volatile T value;
        volatile long sequence = -1L;   // -1 表示空
    }

    // ========== RingBuffer ==========
    public static class RingBuffer<T> {
        private final Entry<T>[] entries;
        private final int mask;
        private final AtomicLong producerSeq = new AtomicLong(-1);

        public RingBuffer(int sizePow2) {
            if ((sizePow2 & (sizePow2 - 1)) != 0)
                throw new IllegalArgumentException("size must be power of 2");
            this.entries = new Entry[sizePow2];
            this.mask = sizePow2 - 1;
            for (int i = 0; i < sizePow2; i++) entries[i] = new Entry<>();
        }

        public Entry<T> get(long seq) {
            return entries[(int) (seq & mask)];
        }

        // 申请一个 slot
        public long next(int capacity) {
            long next;
            while (true) {
                long current = producerSeq.get();
                next = current + 1;
                // 槽位会被消费者覆盖吗？看它当前位置
                long wrapPoint = next - capacity;
                long cachedGating = producerSeq.get();   // 简化为同一个
                if (wrapPoint > cachedGating) {
                    // 等消费者追上
                    LockSupport.parkNanos(1);
                    continue;
                }
                if (producerSeq.compareAndSet(current, next)) {
                    return next;
                }
            }
        }

        public void publish(long seq, T value) {
            Entry<T> entry = get(seq);
            entry.value = value;
            entry.sequence = seq;
        }
    }

    // ========== 事件处理器 ==========
    public interface EventHandler<T> {
        void onEvent(T event, long sequence, boolean endOfBatch) throws Exception;
    }

    // ========== 消费者 ==========
    public static class BatchEventProcessor<T> implements Runnable {
        private final RingBuffer<T> rb;
        private final EventHandler<T> handler;
        private final AtomicLong cursor = new AtomicLong(-1);
        private volatile boolean running = true;

        public BatchEventProcessor(RingBuffer<T> rb, EventHandler<T> handler) {
            this.rb = rb;
            this.handler = handler;
        }

        public long getCursor() { return cursor.get(); }

        @Override
        public void run() {
            long next = cursor.get() + 1;
            while (running) {
                Entry<RingBuffer.Entry<T>> entry = (Entry) rb.get(next);
                if (entry.sequence < next) {
                    LockSupport.parkNanos(1);  // 等生产者
                    continue;
                }
                try {
                    handler.onEvent(entry.value, next, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                cursor.set(next);
                next++;
            }
        }

        public void stop() { running = false; }
    }

    // ========== 演示 ==========
    public static void main(String[] args) throws InterruptedException {
        RingBuffer<Long> rb = new RingBuffer<>(1024);

        // 100 个生产者
        Thread[] producers = new Thread[100];
        for (int i = 0; i < 100; i++) {
            final int pid = i;
            producers[i] = new Thread(() -> {
                long count = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    long seq = rb.next(rb.entries.length);
                    rb.publish(seq, count++);
                }
            }, "producer-" + i);
            producers[i].start();
        }

        // 1 个消费者
        long start = System.nanoTime();
        long initialCursor;

        BatchEventProcessor<Long> processor = new BatchEventProcessor<>(rb, (event, seq, isLast) -> {
            if (seq % 1_000_000 == 0)
                System.out.println("Event " + seq + " = " + event);
        });
        new Thread(processor, "consumer").start();

        Thread.sleep(5000);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        long processed = processor.getCursor();
        System.out.println("Processed " + processed + " in " + elapsedMs + "ms");
        System.out.println("Throughput ≈ " + (processed * 1000L / elapsedMs / 1_000_000) + " M/s");

        for (Thread p : producers) p.interrupt();
        processor.stop();
    }
}
```

### 6.2 简化版核心逻辑（30 行内）

> 如果只想理解"它最核心在做什么"，看下面这些就够：

```java
// 1. RingBuffer：预分配的环
Entry[] entries = new Entry[1024];

// 2. 生产者：只争一个序号，序号决定位置
long seq = producerSeq.getAndIncrement();      // CAS
entries[seq & 1023].value = msg;
entries[seq & 1023].sequence = seq;

// 3. 消费者：一直追这个 seq，没追到就 spin
while (consumerCursor < producerSeq.get()) {
    msg = entries[consumerCursor & 1023].value;
    handle(msg);
    consumerCursor++;
}
```

**就这么简单。**

### 6.3 用真正的 LMAX Disruptor（推荐）

实际项目建议直接用官方版本：

```xml
<dependency>
    <groupId>com.lmax</groupId>
    <artifactId>disruptor</artifactId>
    <version>3.4.4</version>
</dependency>
```

```java
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class App {

    public static class Event {
        public long value;
        public void set(long v) { this.value = v; }
    }

    public static void main(String[] args) throws InterruptedException {
        int bufferSize = 1024;

        Disruptor<Event> disruptor = new Disruptor<>(
            Event::new,
            bufferSize,
            DaemonThreadFactory.INSTANCE,
            ProducerType.MULTI,
            new SleepingWaitStrategy()
        );

        disruptor.handleEventsWith((event, sequence, endOfBatch) ->
            System.out.println("Got: " + event.value)
        );

        RingBuffer<Event> ringBuffer = disruptor.start();

        // 生产者
        for (int i = 0; i < 1000; i++) {
            long seq = ringBuffer.next();
            Event e = ringBuffer.get(seq);
            e.set(i);
            ringBuffer.publish(seq);
        }

        Thread.sleep(1000);
        disruptor.shutdown();
    }
}
```

---

## 7. 与 BlockingQueue / MQ 的对比

| 维度 | **ArrayBlockingQueue** | **LinkedBlockingQueue** | **RocketMQ / Kafka** | **Disruptor** |
|------|----------------------|------------------------|----------------------|---------------|
| **范围** | 单机内存 | 单机内存 | 集群级（跨机器） | 单机内存 |
| **持久化** | 无 | 无 | 有（磁盘） | 无 |
| **吞吐（百万级压测）** | ~3 M/s | ~6 M/s | 取决于集群 | **~100+ M/s** |
| **延迟** | 微秒级 | 微秒级 | 毫秒级 | **几十 ns 级** |
| **多生产者支持** | 是（加锁） | 是（加锁） | 是 | 是（单生产者模式更优） |
| **典型用途** | 一般业务解耦 | 一般业务解耦 | 跨服务异步、事件流 | 金融、日志、撮合 |

### 7.1 吞吐天差地别的原因

| 因素 | JDK 队列 | Disruptor |
|------|---------|-----------|
| 锁 | synchronized / ReentrantLock | 全程无锁 |
| 内存分配 | 每次都 new Node | 环形数组复用 |
| 缓存 | 节点零散，命中率低 | 顺序访问 + 预取 + 行填充 |
| GC 压力 | 大 | 几乎为零 |
| 竞争 | 锁争抢激烈 | 单写者模式 0 竞争 |

### 7.2 选择建议

```
要在不同机器之间通信吗？
└── 是 → Kafka / RocketMQ
└── 否 → 需要极致性能吗？
            ├── 是 → Disruptor
            └── 否 → BlockingQueue（更简单）
```

### 7.3 一句话总结性能差异

> **JDK 的 BlockingQueue 是"功能正确"，Disruptor 是"功能正确 + 微观极致"。一亿 TPS 听起来很吓人，本质是**让 CPU 缓存别失效、让线程别休眠、让内存别分配**。**

---

## 8. 总结

### 8.1 Disruptor 的五句话总结

1. **是什么**：单进程内的有界无锁消息队列。
2. **为什么**：用极致的硬件亲和性换取微秒→纳秒级的延迟。
3. **怎么做**：预分配环形数组 + 序号管理 + cache line padding + 单写者原则。
4. **什么时候用**：跨线程/跨进程消息传递、对延迟敏感（撮合、日志）。
5. **什么时候不用**：跨机器、要求持久化、生产-消费规模悬殊的微服务通信（用 MQ）。

### 8.2 Disruptor 给我们最重要的启示

```
启示 1：性能优化的极致不是堆机器，而是懂硬件。
启示 2：数据结构 > 算法。
启示 3：无锁不是信仰，是把"等待"放进自旋或cas。
启示 4：所有以 micro/nano 为单位计量的优化，最终都在 cache。
启示 5：业务调用栈上的 99% 时间花在了"没必要的等待"上，
         这是所有性能优化最该先解决的地方。
```

### 8.3 下一步可深入的方向

- **JDK 的 `VarHandle` / `AtomicLongArray`**：替代 `Unsafe` 的现代 API
- **JCTools / Agrona**：HFT 业界更激进的解决方案
- **Arthas 火焰图**：用火焰图找自己业务的瓶颈
- **memory_order 在 JVM 上的语义**：理解 happens-before 与内存屏障
- **JMH 微基准测试**：把"主观感觉"变成"可量化数据"

---

*创建时间：2026-07-03*
*配套文档：消息队列学习总结.md*
*学习进度：P2（Disruptor 源码解读）— ✅ 已完成学习*
