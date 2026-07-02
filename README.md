# DDIA (Designing Data-Intensive Applications) 学习路线与 Java 实践指南

本仓库用于记录学习 《Designing Data-Intensive Applications》（中文译名：《数据密集型应用系统设计》）过程中的笔记、思维导图以及配套的 Java 代码实现。

---

## 📚 目录结构建议

推荐在学习过程中采用如下目录结构来组织笔记与实践代码：

```text
ddia/
├── README.md                 # 学习主文档与计划表
├── notes/                    # 读书笔记与思维导图
│   ├── part1-storage/        # 第一部分：数据系统基石 (Ch 1-4)
│   ├── part2-distributed/    # 第二部分：分布式数据 (Ch 5-9)
│   └── part3-derived/        # 第三部分：衍生数据 (Ch 10-12)
└── code/                     # Java 动手实践项目
    ├── serialization/        # Ch 4: 序列化对比 (Protobuf vs Avro vs JSON)
    ├── bitcask/              # Ch 3: 实现一个简单的 Bitcask 存储引擎
    ├── lsm-tree/             # Ch 3: 简易 LSM-Tree / SSTable 实现
    └── consensus/            # Ch 9: 简单的分布式共识/状态机模拟
```

---

## 🛠️ Java 动手实践构想

DDIA 是一本偏向原理的书，如果只读不写，很容易“合上书就忘”。结合你的 Java 背景，以下是几个非常适合在学习过程中动手实现的迷你项目：

### 1. 简易 Bitcask 存储引擎 (对应 Chapter 3)
*   **目标**：理解“追加式日志”与“内存哈希索引”。
*   **核心实现**：
    *   用 Java 的 `RandomAccessFile` 实现追加写日志（Append-only log）。
    *   在内存中维护一个 `HashMap<Key, ByteOffset>` 记录每个 Key 最新的物理偏移量。
    *   实现后台段合并（Compaction）与垃圾回收机制。
    *   实现启动时的索引重建。

### 2. 序列化格式性能对比 (对应 Chapter 4)
*   **目标**：理解二进制编码与文本编码的差异。
*   **核心实现**：
    *   编写相同的实体对象。
    *   分别使用 JSON（Jackson）、Protocol Buffers、Avro 和 Java 原生序列化进行编码。
    *   对比**序列化后的字节大小**以及**序列化/反序列化耗时**，直观感受 Schema 的威力。

### 3. 基于 Netty 的简易 RPC 与脑裂模拟 (对应 Chapter 5, 8)
*   **目标**：体验分布式环境下的网络分区。
*   **核心实现**：
    *   使用 Netty 构建一个简单的 Master-Follower 复制拓扑。
    *   手动注入网络丢包或延迟，模拟“网络分区”。
    *   观察并处理非对称网络故障带来的“脑裂（Split-Brain）”问题。

---

## 📈 学习进度打卡表

- [ ] **第一部分：数据系统基石**
  - [ ] Chapter 1. 可靠、可伸缩与可维护的系统
  - [ ] Chapter 2. 数据模型与查询语言
  - [ ] Chapter 3. 数据存储与检索 (推荐动手实现 `bitcask`)
  - [ ] Chapter 4. 数据编码与演进 (推荐动手实现 `serialization`)
- [ ] **第二部分：分布式数据系统**
  - [ ] Chapter 5. 数据复制 (Replication)
  - [ ] Chapter 6. 数据分区 (Partitioning)
  - [ ] Chapter 7. 事务 (Transactions)
  - [ ] Chapter 8. 分布式系统的麻烦
  - [ ] Chapter 9. 一致性与共识
- [ ] **第三部分：衍生数据**
  - [ ] Chapter 10. 批处理 (Batch Processing)
  - [ ] Chapter 11. 流处理 (Stream Processing)
  - [ ] Chapter 12. 数据系统的未来
