# Bug 诊断记录：多生产者 Demo "卡住" 与错读问题

> 配套代码：[`code/disruptor-lab/src/main/java/io/ddia/disruptor/lab/multiproducer/MiniMultiProducerDemo.java`](../../code/disruptor-lab/src/main/java/io/ddia/disruptor/lab/multiproducer/MiniMultiProducerDemo.java)
> 配套主文档：[`docs/mq-learning/Disruptor详解.md`](Disruptor详解.md)
>
> 诊断状态：已定位卡死与错读的共同根因。`availableBuffer` 只缓解了消费者卡在旧 `Entry.sequence` 上的问题，当前代码仍可能错读数据；完整修复必须调整 `MultiProducerSequencer.next()` 的绕环保护。

---

## 目录

1. [现象描述](#1-现象描述)
2. [不是"残留进程"：是真正的算法缺陷](#2-不是残留进程是真正的算法缺陷)
3. [根因分析：绕环保护失效导致 Entry.sequence 被覆盖](#3-根因分析绕环保护失效导致-entrysequence-被覆盖)
4. [验证：用日志复现问题](#4-验证用日志复现问题)
5. [第一次修复尝试：引入 availableBuffer 位图](#5-第一次修复尝试引入-availablebuffer-位图)
6. [JMM 可见性陷阱：为什么 long[] 不行要用 AtomicLongArray](#6-jmm-可见性陷阱为什么-long-不行要用-atomiclongarray)
7. [LMAX 原版思路对照](#7-lmax-原版思路对照)
8. [表面运行结果与遗留问题](#8-表面运行结果与遗留问题)
9. [二次验证：当前代码仍会错读数据](#9-二次验证当前代码仍会错读数据)

---

## 1. 现象描述

跑多生产者 demo 改小到 `perProducer = 5_000` 之后，程序**依然不结束、不打印最终统计**：

```bash
mvn -q -f code/disruptor-lab/pom.xml exec:java \
    -Dexec.mainClass=io.ddia.disruptor.lab.multiproducer.MiniMultiProducerDemo
```

看起来像是「上次跑剩的残留进程把这次卡住了」，但 `pgrep` 没有残留进程，**这是程序自己死循环**。

---

## 2. 不是"残留进程"：是真正的算法缺陷

> **结论先行：这不是 JVM 没清理干净也不是线程饥饿，而是多生产者的绕环保护没有真正挡住生产者，导致还没被消费者处理的槽位被后续序号覆盖。消费者再用 `Entry.sequence` 等老序号，就会永远 mismatch。**

证据：

1. 三个生产者线程都 `join()` 了，能跑出 `producer.join()` 说明生产者线程确实跑完了
2. 消费者在 `while (consumer.processed() < total - 1)` 里死等，但 `processed()` 永远追不上
3. 加日志后能看到消费者一直卡在 `next=1`、`published=149505` 这种值上，跟 `next` 完全对不上

「残留进程不会发生」是因为：

- Maven `exec:java` 是前台 fork 子进程跑 main，main 退出 JVM 就退
- 三个生产者都是 `setDaemon(true)`，主线程不退也会被强制终止
- `pgrep` 啥也搜不到

---

## 3. 根因分析：绕环保护失效导致 Entry.sequence 被覆盖

先把层次分清：

- **直接现象**：消费者等 `next=1`，但同一个槽里的 `Entry.sequence` 已经变成 `149505`，所以永远等不到 `1`
- **真正原因**：生产者本不该在消费者没跟上时继续绕环写同一个槽，但 `MultiProducerSequencer.next()` 里的容量保护写错了，导致生产者仍然 CAS 成功并继续申请新序号
- **设计问题**：简化 demo 又把 `Entry.sequence` 同时当作“槽里是哪条消息”和“这条 seq 是否已发布”的标记，一旦槽被复用覆盖，消费者就没有独立的发布状态可查

### 3.1 简化版多生产者的发布-消费约定

```java
// RingBuffer.publish()
Entry<T> e = entries[(int) (seq & sequencer.mask())];
e.value = value;
e.sequence = seq;                  // <-- 用 Entry.sequence 当"是否已发布"标记
sequencer.publish(seq);            // 推 cursor

// BatchEventProcessor.run()
Entry<T> e = rb.entries[(int) (next & seq.mask())];
long published = e.sequence;       // 读这个标记
if (published != next) {           // 还没发布到这一条
    LockSupport.parkNanos(1L);
    continue;
}
```

### 3.2 槽会被反复写

关键参数：

| 参数 | 值 |
|---|---|
| `bufferSize` | 1024 |
| `perProducer` | 50,000（修后） |
| `producerCount` | 3 |
| 总消息数 | 150,000 |

总共 150,000 条消息要写进 1024 长的环形数组，**每个槽平均会被写 150,000 / 1024 ≈ 146 次**。

`seq & 1023` 落在同一个 slot 上的所有 seq：

```
1, 1025, 2049, 3073, ... , 149505, ...
```

正常情况下，这些序号当然都会复用同一个槽，但**必须等消费者处理完旧序号以后才能复用**。环形队列不是不能覆盖，而是不能覆盖“还没被消费的旧槽”。

### 3.3 真正的算法错误：`continue` 没有阻止 CAS

`MultiProducerSequencer.next()` 里本来有绕环保护：

```java
wrapPoint = next - bufferSize;
if (wrapPoint > cached) {
    long min = minGating();
    if (wrapPoint > min) {
        Thread.onSpinWait();
        continue;
    }
    cachedGating.set(min);
    cached = min;
}
} while (!nextValue.compareAndSet(current, next));
```

它想表达的是：

> 如果 `wrapPoint > minGating()`，说明生产者申请的 `next` 已经要追上甚至越过消费者了，这个槽还不能复用。生产者应该等待，然后重新读取 `nextValue` 和消费者进度。

但这里有一个 Java 控制流陷阱：这段代码在 `do { ... } while (!CAS)` 结构里，`continue` 跳到的是 `while` 条件判断，而不是跳回 `do` 块开头。

所以执行路径实际上变成了：

1. 发现 `wrapPoint > min`，说明容量不够，应该等待
2. 执行 `continue`
3. 直接进入 `while (!nextValue.compareAndSet(current, next))`
4. 如果 CAS 成功，`next()` 返回这个本不该申请成功的序号

也就是说，容量保护虽然写在代码里，但在最关键的分支上没有生效。生产者会在消费者还没消费旧槽时继续把 `nextValue` 往前推，最终绕环覆盖还没处理的 `Entry`。

正确的逻辑应该确保“容量不足”这一轮**不会执行 CAS**，例如用普通 `while (true)` 循环并在容量不足时 `continue` 到循环开头，或者把 CAS 放在确认容量足够之后的分支里。

### 3.4 时序问题：消费者启动慢，读到覆盖后的值

假设三个生产者启动极快，消费者读完 cursor 时 cursor 已经是 `149999`：

1. 消费者从 `next=0` 开始处理
2. 处理到 `next=1` 时去读 `Entry[1 & 1023].sequence` = `Entry[1].sequence`
3. **因为 `next()` 的绕环保护失效**，后续生产者已经可以抢到 `1025`、`2049`、`3073` 这类本应等待消费者后才能复用的序号
4. 消费者读到的 `published = 2049`，而它要等 `published == 1`
5. 消费者 park、重读、再读、再 park...，**永远等不到 1**，因为 Entry[1] 已经被写到 `149505` 了

这里不需要假设某个生产者“还没来得及写 `seq=1`”。只要消费者落后、而生产者越过容量保护继续绕环，旧槽就可能被覆盖。`Entry.sequence` 被覆盖是结果，不是第一个出错点。

### 3.5 另一个语义问题：cursor 不能简单 `set(sequence)`

当前 `publish(sequence)` 还是：

```java
public void publish(long sequence) {
    cursor.set(sequence);
}
```

这在多生产者里也容易误导。多生产者的发布顺序可以和申请顺序不同：

```text
线程 A 抢到 10，还没发布
线程 B 抢到 11，先发布 cursor=11
线程 A 后发布 cursor=10
```

如果把 `cursor` 理解成“连续可消费到哪里”，那 `cursor=11` 是假的，因为 10 可能还没发布；如果后面又写回 `10`，`cursor` 甚至会倒退。

所以多生产者下需要分开两个概念：

- `nextValue` / claimed cursor：生产者最多已经申请到了哪个序号
- `availableBuffer`：每个具体序号是否真的发布完成

消费者不能只看一个 `cursor` 就认为 `[0..cursor]` 都可消费，必须结合 `availableBuffer` 找从 `next` 开始的连续可用区间。

### 3.6 日志铁证

```
[consumer] mismatch #299371000: next=1, published=149505 (slot idx=1)
```

- `next=1` —— 消费者处理的第一批消息里的第二条
- `published=149505` —— 该槽被写过的最后一个序号
- `slot idx=1` —— `Entry[1]`，`149505 & 1023 == 1`，确认是同一个槽

**这说明旧槽确实被覆盖了。更准确地说：`next()` 没有正确阻止生产者绕环覆盖未消费槽位，而消费者又依赖会被复用的 `Entry.sequence` 当发布状态，两个问题叠在一起导致卡死。**

---

## 4. 验证：用日志复现问题

为了科学地证明这个 bug 而不是靠猜，加了两类诊断日志：

1. **消费者侧**：

   ```java
   if (mismatchHitCount == 1 || mismatchHitCount % 1000 == 0) {
       System.err.printf("[consumer] mismatch #%d: next=%d, published=%d (slot idx=%d)%n",
               mismatchHitCount, next, published, (int) (next & seq.mask()));
   }
   ```

2. **main 侧硬超时**（10 秒）：

   ```java
   if (System.nanoTime() > deadline) {
       System.err.printf("[main] TIMEOUT after 10s. consumer.processed=%d, total-1=%d%n",
               consumer.processed(), total - 1);
       consumer.stop();
       return;
   }
   ```

### 4.1 跑原始版本（不带超时）

输出会这样：

```
[consumer] mismatch #299371000: next=1, published=149505 (slot idx=1)
[consumer] mismatch #299372000: next=1, published=149505 (slot idx=1)
... 一直刷
```

到时间用户看不到任何东西，因为 `mismatchHitCount % 1000` 命中频率太低，加上 `tail -80` 又把 stdout 截掉。

### 4.2 跑带超时版本（10 秒）

10 秒后强制退出，能看到卡死现场的 `next` 和 `published`，**直接证明消费者等的是旧序号，但槽位已经被后续序号覆盖**。再回看 `next()` 的控制流，就能定位到更底层的绕环保护失效。

---

## 5. 第一次修复尝试：引入 availableBuffer 位图

当时的第一版修复思路是：参考 LMAX 真实实现，给每个 seq 配一个独立的"是否已发布"标记位，**这个标记位不会被 `Entry` 槽位复用覆盖**。

这个方向只对了一半。`availableBuffer` 确实解决了“发布状态不能放在 Entry 上”的问题；但它不能替代 `next()` 里的容量保护，也不能保证 ring slot 里的 value 仍然属于当前 sequence。真正完整的修复需要两件事：

1. `next()` 在 `wrapPoint > minGating()` 时必须等待，不能继续 CAS 申请序号
2. 发布状态使用 `availableBuffer`，消费者用它判断具体 `seq` 是否已经发布

### 5.1 数据结构

```java
static final class AvailableBuffer {
    final AtomicLongArray bits;
    AvailableBuffer(int capacity) {
        this.bits = new AtomicLongArray((capacity + 63) >>> 6);
    }
    void set(long seq) {
        int idx = (int) (seq >>> 6);
        long mask = 1L << (int) (seq & 63);
        while (true) {
            long old = bits.get(idx);
            long newV = old | mask;
            if (old == newV) return;
            if (bits.compareAndSet(idx, old, newV)) return;
        }
    }
    boolean isAvailable(long seq) {
        int idx = (int) (seq >>> 6);
        long mask = 1L << (int) (seq & 63);
        return (bits.get(idx) & mask) != 0;
    }
    void clear(long seq) {
        // CAS 清掉一位
    }
}
```

### 5.2 生产端加 set

```java
public void publish(T value) {
    long seq = sequencer.next();                 // CAS 抢序号
    Entry<T> e = entries[(int) (seq & sequencer.mask())];
    e.value = value;
    e.sequence = seq;
    available.set(seq);                          // ← 新增：在位图里打点
    sequencer.publish(seq);                      // 推 cursor
}
```

### 5.3 消费端查位图、不再读 Entry.sequence

```java
public void run() {
    long next = cursor.get() + 1;
    while (running) {
        long available = seq.cursor();
        while (next <= available) {
            // 不再读 e.sequence，改查位图
            if (!rb.available.isAvailable(next)) {
                LockSupport.parkNanos(1L);
                available = seq.cursor();
                continue;
            }
            Entry<T> e = rb.entries[(int) (next & seq.mask())];
            handler.onEvent(e.value, next, next == available);
            rb.available.clear(next);             // 处理完清掉位，避免内存膨胀
            cursor.set(next);
            next++;
        }
        if (next > available) {
            LockSupport.parkNanos(1L);
        }
    }
}
```

> 注意：`available.set(seq)` 必须发生在对消费者可见的发布信号之前，否则消费者看到上界推进后查位图可能查不到（见下一节 JMM 分析）。不过在多生产者里，这个“发布信号”不能简单理解成 `cursor.set(sequence)` 就等价于 `[0..sequence]` 全部可用。

---

## 6. JMM 可见性陷阱：为什么 long[] 不行要用 AtomicLongArray

第一版修复用的是普通 `long[]`，跑出来**不是永久死循环**而是**消费到一半卡死**：

```
[consumer] not-yet-available #131616000: next=773 (slot idx=773)
... 卡在 next=773
```

### 6.1 happens-before 链

消费者要查到 `seq=N` 的位，至少需要两个保证：

1. producer 写入 bit 后，consumer 能看到这个写入
2. 多个 producer 同时设置同一个 `long` word 里的不同 bit 时，不能互相覆盖

```
producer thread:                       consumer thread:
──────────────                         ──────────────
e.value = value;                       long available = seq.cursor();
e.sequence = seq;        ───?───      while (next <= available) {
available.set(seq);     ← 这里        if (!rb.available.isAvailable(next))
sequencer.publish(seq);  ← cursor 写                          ↑
                                       └── 这里能否看到 set(seq)？
```

`sequencer.publish(seq)` 内部是 `Sequence.set` = volatile 写。对单个生产者来说，`available.set(seq)` 写在 `cursor.set(seq)` 前面，consumer 如果读到了这次 volatile 写，按 release/acquire 语义可以看到之前的写。

但这个 demo 是多生产者，普通 `long[]` 还会遇到更直接的问题：设置 bit 是读-改-写，不是原子操作。

假设两个生产者同时设置同一个 `long` word：

```text
old = 0
producer A 要 set bit 1，算出 0b0010
producer B 要 set bit 2，算出 0b0100
A 写回 0b0010
B 写回 0b0100
```

最终 bit 1 丢了。消费者等 `seq=1` 时就会一直看到 unavailable。

### 6.2 实测结果

实际跑确实会出现「消费者读到 cursor 推进了但某个 bit 一直不可用」的情况。具体表现就是 `next=773` 卡住。这不一定只是缓存可见性问题，更可能是普通 `long[]` 的并发读-改-写丢了更新。

### 6.3 改用 `AtomicLongArray`

`AtomicLongArray.get/compareAndSet` 同时解决两个问题：

- CAS 保证多个 producer 设置同一个 word 的不同 bit 不会丢更新
- atomic/volatile 读写语义保证 producer 写入的 bit 对 consumer 可见

> **教训：跨线程共享的发布位图不能用普通 `long[]` 做非原子读-改-写，必须用 Atomic / VarHandle / Unsafe 这类带原子性和可见性保证的机制。**

---

## 7. LMAX 原版思路对照

真实的 LMAX Disruptor 里，多生产者发布追踪也是用位图，叫 `availableBuffer`：

- `next()` / `tryNext()` 负责申请序号，并且必须用消费者 gating sequence 防止生产者绕环覆盖未消费槽
- `publish(seq)` 把这个 seq 标记为 available
- 消费者等待某个 `next` 时，会结合 cursor 上界和 `availableBuffer` 判断从 `next` 开始最多连续可用到哪里
- 消费者不靠 `Entry.sequence` 判断发布状态

`Entry.sequence`（在真实 LMAX 里叫 `event.getSequence()`）通常是 `entry` 写入时记录的 seq，消费者用它来**识别"我拿到的是哪条消息"**，而不是**判断是否已发布**。

我们这个简化 demo 把两个语义都压在了 `Entry.sequence` 上，所以才漏掉了「Entry 会被覆盖」这个坑。同时，`next()` 里的 `do-while + continue` 又让绕环保护失效，使覆盖真的发生在未消费槽位上。正确实现中 `Entry.sequence` 仍然可以有，但只能用来标识数据来源；发布状态应该交给 `availableBuffer`，容量保护仍然必须由 sequencer 正确执行。

---

## 8. 表面运行结果与遗留问题

下面这段是当时引入 `availableBuffer` 后看到的运行输出。它只能说明程序没有再卡死，**不能证明消费到的数据正确**。

`perProducer = 50_000`，`producerCount = 3`，`total = 150,000`：

```
[consumer] processed 0
[consumer] processed 16384
[consumer] processed 32768
... (stdout 行因为超时侦测信号交错)

===== 多生产者 Disruptor 运行结果 =====
生产者线程数    : 3
消息总数        : 150,000 条
端到端耗时      : 47 ms
吞吐量          : 3.14 M ops/s

===== 对照单生产者 =====
publish() 次数 (volatile 写) : 146,402
CAS 尝试次数                 : 149,297  <-- 这里不为 0
CAS / 消息                   : 1.02

结论：多生产者路径上 CAS != 0，这是单生产者比多生产者快一个数量级的根本原因。
```

当时的数据点解读也需要修正：

- **publish() 次数 146,402 ≠ 150,000**：这个计数来自未同步的普通 `long` 字段，多个生产者并发自增会丢更新，因此不能拿它证明发布次数或消息完整性。
- **CAS / 消息 ≈ 1.02**：每条消息平均 1 次 CAS 抢号，是多生产者路径的最小理论下限（实际会比这个高）
- **3.14 M ops/s**：这个吞吐量只是“主循环推进到了结束条件”的结果。由于当时没有校验 `event == sequence`，它不能证明数据没有被错读。

不过这个结果只能说明 `availableBuffer` 路线缓解了 `Entry.sequence` 被覆盖后的等待问题，不能说明多生产者 sequencer 已经完全正确。完整版本仍应修正 `next()` 的绕环等待逻辑，并重新定义 cursor / availableBuffer 的职责。

---

## 9. 二次验证：当前代码仍会错读数据

后来重新检查时发现，第 5 节的 `availableBuffer` 修复仍然不完整。它解决了“消费者等 `Entry.sequence == next` 导致永久卡住”的问题，但没有解决更底层的覆盖问题：

> 生产者仍然可能在消费者读取旧槽之前绕环覆盖该槽。`availableBuffer` 只能说明某个 `seq` 曾经发布过，不能保证 ring slot 里现在还保存着这个 `seq` 对应的 value。

### 9.1 为什么原来的验证不够

当前 demo 的消费者只检查：

```java
if (event == null) {
    throw new IllegalStateException("消费到 null @ " + sequence);
}
```

这只能证明“读到的槽里有一个非空 value”，不能证明：

```text
正在消费 sequence=N 时，读到的 event 就是 sequence=N 对应的数据。
```

如果 `Entry[0]` 原本保存 `seq=0` 的数据，随后被 `seq=59984` 覆盖，那么消费者处理 `sequence=0` 时读到 `event=59984`，只要它不是 `null`，原 demo 仍然会继续运行，并打印出看似正常的吞吐结果。

这就是之前修复记录的问题：它把“程序不再卡死”误认为“算法已经正确”。

### 9.2 验证方式：让 event 直接等于 seq

为了验证数据是否真的有序，可以让生产者把申请到的 `seq` 本身写入 event：

```java
long seq = sequencer.next();
Entry<Long> e = ring.entries[(int) (seq & sequencer.mask())];
e.value = seq;
e.sequence = seq;
ring.available.set(seq);
sequencer.publish(seq);
```

消费者则严格检查：

```java
if (event == null || event.longValue() != sequence) {
    throw new AssertionError(
            "wrong event: sequence=" + sequence + ", event=" + event);
}
```

这个验证比“非空检查”强得多。因为只要消费者读到了被后续序号覆盖的槽，`event != sequence` 就会立刻暴露。

### 9.3 实测结果

用当前代码运行这个验证，很快失败：

```text
[consumer] not-yet-available #1: next=0 (slot idx=0)
java.lang.AssertionError: wrong event: sequence=0, event=59984
```

含义非常直接：

- 消费者正在按顺序处理 `sequence=0`
- 但它从 `Entry[0]` 读出来的 value 已经是 `59984`
- 说明 `Entry[0]` 在消费者读取 `seq=0` 之前，已经被后面的生产者绕环覆盖

所以当前代码虽然可能跑完，但它不是正确的多生产者 RingBuffer。

### 9.4 错误修复记录

这次 bug 实际经历了三层误判：

1. **第一层问题：卡死**
   - 原因被观察为 `Entry.sequence` 被覆盖
   - 消费者等待 `published == next`，但槽位已经变成后续序号

2. **第二层修复：引入 `availableBuffer`**
   - 这个修复让消费者不再依赖 `Entry.sequence`
   - 因此程序不再卡在 “等待旧 sequence” 上
   - 但它只证明某个 `seq` 曾经发布过，没有证明对应数据还在 slot 里

3. **第三层真因：容量保护失效**
   - `MultiProducerSequencer.next()` 在 `wrapPoint > minGating()` 时本应等待
   - 但 CAS 被放在 `do-while` 的 while 条件里
   - `continue` 会跳到 while 条件，仍然可能执行 `compareAndSet(current, next)`
   - 结果是容量不足时仍然可能申请新序号，生产者提前绕环覆盖未消费槽

准确结论：

> `availableBuffer` 是多生产者发布状态的一部分，但不是完整修复。完整修复必须先保证 `next()` 不会越过消费者 gating sequence，禁止生产者覆盖未消费槽；然后再用 `availableBuffer` 表示每个具体序号是否已经发布。

### 9.5 后续修复方向

`next()` 应改成更明确的控制流：容量不足时直接回到循环开头，不能执行 CAS。

示意：

```java
while (true) {
    long current = nextValue.get();
    long next = current + 1;
    long wrapPoint = next - bufferSize;
    long cached = cachedGating.get();

    if (wrapPoint > cached) {
        long min = minGating();
        if (wrapPoint > min) {
            Thread.onSpinWait();
            continue; // 重新读取 current / next / min，不执行 CAS
        }
        cachedGating.set(min);
    }

    if (nextValue.compareAndSet(current, next)) {
        return next;
    }
}
```

核心原则：

> 只有确认容量足够以后，才允许执行 CAS 抢号。

---

## 附：改动的文件清单

| 文件 | 改动 |
|---|---|
| [`MiniMultiProducerDemo.java`](../../code/disruptor-lab/src/main/java/io/ddia/disruptor/lab/multiproducer/MiniMultiProducerDemo.java) | 1. 新增 `AvailableBuffer` 类<br>2. `RingBuffer` 持 `available`，构造时接收 capacity<br>3. `RingBuffer.publish()` 末尾 `available.set(seq)`<br>4. 消费者查 `available.isAvailable(next)` 而非 `Entry.sequence`<br>5. 消费者处理完 `available.clear(next)`<br>6. `main` 里加超时、日志输出 |

> 当前这份修复记录里的代码改动集中在 `MiniMultiProducerDemo.java`。如果要把算法彻底修正，`MultiProducerSequencer.next()` 的容量保护也需要调整；否则 `availableBuffer` 只是绕开了 `Entry.sequence` 被覆盖后的等待问题，没有解释并修掉覆盖为什么会提前发生。
