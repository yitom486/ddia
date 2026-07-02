# Outbox 模式详解

> 本文档配套消息队列学习总结文档，是对 **P1（Outbox 模式实操）** 学习计划的展开文档。

---

## 目录

1. [背景：为什么会产生 Outbox 模式](#1-背景为什么会产生-outbox-模式)
2. [核心原理与思想](#2-核心原理与思想)
3. [两种实现方式对比](#3-两种实现方式对比)
4. [Spring Boot + Postgres + Kafka 实操](#4-spring-boot--postgres--kafka-实操)
5. [关键陷阱与最佳实践](#5-关键陷阱与最佳实践)
6. [与其他模式的对比](#6-与其他模式的对比)
7. [总结](#7-总结)

---

## 1. 背景：为什么会产生 Outbox 模式

### 1.1 一句话定义

> **Outbox 模式**：把"业务数据"和"待发消息"写在**同一个数据库事务**里，由后台程序把消息从数据库搬运到 MQ。

### 1.2 经典困境：双写不一致

一个最常见的业务场景——**下单成功后发一条消息**：

```java
@Transactional
public void placeOrder(OrderRequest req) {
    // 1. 写订单到数据库
    orderRepo.save(new Order(req));
}

public void onOrderPlaced(Order order) {
    // 2. 发一条 OrderPlaced 消息到 Kafka
    kafkaTemplate.send("OrderPlaced", order);
}
```

**问题来了：**

| 失败场景 | 后果 |
|---------|------|
| 写库成功 + 发消息失败 | DB 有订单，**MQ 没消息**，库存系统永远不知道 |
| 发消息成功 + 写库失败 | MQ 有了，**DB 没订单**，消费者扣库存但订单消失 |
| 写库成功 + 发消息成功但中间网络挂 | **同上** |

这就是著名的 **"双写不一致"** —— 在没有分布式事务的情况下，单系统内的两个写动作无法保证原子性。

### 1.3 传统解法的缺陷

| 方案 | 问题 |
|------|------|
| **分布式事务（2PC / XA）** | 性能极差、对 MQ 支持差、不支持大多数云服务 |
| **本地消息表轮询** | 自己造轮子，代码冗长 |
| **先发 MQ 反查 DB** | 消费端要做"幂等回查"，链路复杂 |
| **RocketMQ 事务消息** | 仅限 RocketMQ，且要绑定 broker，跨 DB 不通用 |

**Outbox 模式就是"本地消息表轮询"的标准化版本**——它不依赖任何特定 MQ，是最通用的解法。

---

## 2. 核心原理与思想

### 2.1 核心思想

> **把"消息"当成业务数据的一部分，跟业务一起持久化。然后用后台程序把"已落库但还没投递出去的消息"搬运到 MQ。**

```
核心三句话：
1. 不再"先写 DB 再发 MQ"——那是两次独立的写
2. 改成"在同一个事务里写 DB + 写 outbox 表"
3. 后台 poller（或 CDC）把 outbox 表里的消息搬运到 Kafka，搬完打删除标记
```

### 2.2 解决的关键问题

| 问题 | Outbox 的解法 |
|------|------------|
| 业务消息必须和业务数据原子性落库 | ✅ 同一事务，要么都成功，要么都失败 |
| 消息不能丢 | ✅ 没投递的留在 outbox 表里，下次继续投 |
| 不强耦合特定 MQ | ✅ 投递层是无状态 poller，可换 Kafka / RocketMQ |
| 消费端要做到幂等 | ⚠️ 这一点 outbox 不解决，仍需消费端去重 |
| 性能不能因同步 IO MQ 退化 | ✅ 写入路径是纯本地 DB，无网络 IO |

### 2.3 架构图

```
                  ┌──────────────────────────────┐
                  │        订单服务（Outbox）       │
                  │                              │
   用户下单 ───▶ │  BEGIN TX                       │
                  │    ├── INSERT orders           │
                  │    ├── INSERT order_items      │
                  │    └── INSERT outbox(message)  │  ←★ 关键：同事务
                  │  COMMIT                       │
                  └────────────┬─────────────────┘
                               │
                          （任何时候）
                               │
                               ▼
                  ┌──────────────────────────────┐
                  │     Outbox Poller 后台任务     │
                  │  (定时 / Debezium CDC)        │
                  │  SELECT * FROM outbox WHERE published = false  │
                  │  send to Kafka                │
                  │  UPDATE outbox SET published = true           │
                  └────────────┬─────────────────┘
                               │
                               ▼
                       ┌──────────────┐
                       │    Kafka     │
                       └──────┬───────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         库存服务         营销服务         推送服务
```

### 2.4 为什么是这种结构

**为什么不让生产者直接发 MQ？**

```java
// ❌ 反例
@Transactional
public void placeOrder(OrderRequest req) {
    orderRepo.save(...);
    kafkaTemplate.send(...);   // 网络 IO，但仍在 TX 里
}
@Transactional
public void commit() {
    // 如果此时 Kafka 挂了 → 事务回滚
    // 但 Kafka send 不一定抛异常... 顺序无法控制
}
```

**核心矛盾**：DB 事务是**本地强一致**协议，MQ 发消息是**网络异步**协议，二者没法天然兼容。

**Outbox 的天才之处**：把"消息"也变成一份 DB 表数据，问题瞬间变成"同一事务里写两张表"——本地事务完美解决。

---

## 3. 两种实现方式对比

| 维度 | **Polling（轮询）** | **CDC（监听 binlog）** |
|------|-------------------|---------------------|
| **原理** | 后台任务按时间间隔 SELECT 未投递消息 | 监听 DB 的 binlog / WAL，自动捕获变更 |
| **依赖** | 仅需 DB | 需要 Debezium / Maxwell / PG 的 logical decoding |
| **延迟** | 取决于轮询间隔（通常秒级） | **毫秒级** |
| **侵入性** | 业务代码需写 outbox 表 | **业务代码零侵入**（任何 DB 写都自动成为事件） |
| **运维成本** | 低 | 中（要跑 Debezium 集群） |
| **适用场景** | 中小规模、订单类 | 大规模 / 高实时要求 / 数据同步 |

### 3.1 Polling 模式流程图

```
   定时任务（每 1s）
       │
       ▼
   ┌────────────────────┐
   │ SELECT FOR UPDATE  │   ← 加锁避免重复消费
   │ WHERE published=0  │
   │ LIMIT 100          │
   └──────────┬─────────┘
              │
       ┌──────┴───────┐
       ▼              ▼
    投递成功      投递失败
       │              │
       ▼              ▼
   UPDATE          留到下次
   published=1     重试
```

### 3.2 CDC 模式（Debezium）流程图

```
   Postgres / MySQL
       │
       │ write-ahead log / binlog
       ▼
   Debezium connector
       │
       │ Kafka Connect SourceTask
       ▼
   Kafka topic: dbserver1.public.outbox
       │
       ▼
   下游消费者
```

**CDC 的极致**：你**根本不用写 outbox 表**——任何 INSERT/UPDATE/DELETE 都是事件。但要付出"理解 DB 日志 + 跑 Debezium"的成本。

### 3.3 推荐策略

```
                    ┌──────────────┐
                    │   业务复杂度   │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼                         ▼
        简单业务                   中等业务            复杂业务
              │                         │                 │
              ▼                         ▼                 ▼
         同步发 MQ                  Polling            CDC
         (简单但有                  Outbox            (Debezium)
          一致性问题)
```

**对于绝大多数系统**：Polling Outbox 已经是**最佳性价比**。本节余下内容以此为准。

---

## 4. Spring Boot + Postgres + Kafka 实操

下面是一套**可以直接拷贝运行**的最小实现。

### 4.1 项目结构

```
outbox-demo/
├── pom.xml
└── src/main/
    ├── java/com/example/outbox/
    │   ├── OutboxApplication.java
    │   ├── domain/
    │   │   ├── Order.java
    │   │   └── OutboxMessage.java
    │   ├── service/
    │   │   └── OrderService.java
    │   ├── repository/
    │   │   ├── OrderRepository.java
    │   │   └── OutboxRepository.java
    │   ├── poller/
    │   │   └── OutboxPoller.java
    │   └── consumer/
    │       └── NotificationConsumer.java
    └── resources/
        ├── application.yml
        └── db/schema.sql
```

### 4.2 pom.xml

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-scheduling</artifactId>
    </dependency>
</dependencies>
```

### 4.3 数据库 Schema

```sql
-- 订单表（业务表）
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outbox 表（关键！）
CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,                 -- 唯一 ID，用于消费端去重
    aggregate_type VARCHAR(100) NOT NULL, -- 比如 'Order'
    aggregate_id VARCHAR(100) NOT NULL,   -- 比如订单 ID
    event_type VARCHAR(100) NOT NULL,     -- 比如 'OrderPlaced'
    payload TEXT NOT NULL,                -- JSON 序列化的消息体
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP,               -- NULL = 未投递
    retry_count INT DEFAULT 0
);

-- 索引：加速 poller 查询
CREATE INDEX idx_outbox_unpublished
    ON outbox_messages(created_at)
    WHERE published_at IS NULL;
```

### 4.4 实体类

#### Order.java

```java
package com.example.outbox.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long userId;
    private BigDecimal amount;
    private LocalDateTime createdAt = LocalDateTime.now();

    // getters / setters 省略
}
```

#### OutboxMessage.java

```java
package com.example.outbox.domain;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_messages")
public class OutboxMessage {

    @Id
    private UUID id;

    private String aggregateType;
    private String aggregateId;
    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private LocalDateTime createdAt = LocalDateTime.now();
    private LocalDateTime publishedAt;
    private Integer retryCount = 0;

    // getters / setters / constructors
}
```

### 4.5 业务 Service：写库 + outbox 同事务

```java
package com.example.outbox.service;

import com.example.outbox.domain.*;
import com.example.outbox.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@Service
public class OrderService {

    private final OrderRepository orderRepo;
    private final OutboxRepository outboxRepo;
    private final ObjectMapper json = new ObjectMapper();

    public OrderService(OrderRepository orderRepo, OutboxRepository outboxRepo) {
        this.orderRepo = orderRepo;
        this.outboxRepo = outboxRepo;
    }

    @Transactional  // ★★ 关键：订单表和 outbox 表在同一事务里
    public Order placeOrder(Long userId, BigDecimal amount) {
        // 1. 写业务表
        Order order = new Order();
        order.setUserId(userId);
        order.setAmount(amount);
        orderRepo.save(order);

        // 2. 同事务写 outbox
        OutboxMessage msg = new OutboxMessage();
        msg.setId(UUID.randomUUID());
        msg.setAggregateType("Order");
        msg.setAggregateId(String.valueOf(order.getId()));
        msg.setEventType("OrderPlaced");
        try {
            msg.setPayload(json.writeValueAsString(Map.of(
                "orderId", order.getId(),
                "userId", userId,
                "amount", amount
            )));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        outboxRepo.save(msg);

        // 整个事务 COMMIT 时，要么两个都进去，要么都不进去
        return order;
    }
}
```

**注意**：在事务**没提交前**，消息还**不可见**给 poller。提交后，poller 才有 SELECT 看到它。这是 outbox 模式的"原子性"保证。

### 4.6 Poller：把 outbox 表搬上 Kafka

```java
package com.example.outbox.poller;

import com.example.outbox.domain.OutboxMessage;
import com.example.outbox.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;

@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxRepository outboxRepo;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPoller(OutboxRepository outboxRepo, KafkaTemplate<String, String> kafka) {
        this.outboxRepo = outboxRepo;
        this.kafka = kafka;
    }

    // 每秒执行一次
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPending() {
        // 1. 取一批未发布的消息
        List<OutboxMessage> batch = outboxRepo.findTop100ByPublishedAtIsNullOrderByCreatedAt();

        for (OutboxMessage msg : batch) {
            try {
                // 2. 投递到 Kafka（topic 名 = event_type）
                String topic = msg.getEventType();  // "OrderPlaced"
                kafka.send(topic, msg.getAggregateId(), msg.getPayload()).get();

                // 3. 投递成功，标记已发布
                msg.setPublishedAt(LocalDateTime.now());
                outboxRepo.save(msg);

                log.info("Published outbox message {} to topic {}", msg.getId(), topic);
            } catch (Exception e) {
                msg.setRetryCount(msg.getRetryCount() + 1);
                outboxRepo.save(msg);
                log.error("Failed to publish outbox message {}", msg.getId(), e);
                // 留在表里，下次再试
            }
        }
    }
}
```

### 4.7 消费端：监听 Kafka 并做幂等

```java
package com.example.outbox.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class NotificationConsumer {

    // 简化版：内存去重表
    // 生产环境应该用 Redis / DB
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

    @KafkaListener(topics = "OrderPlaced", groupId = "notification-service")
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            // 1. 解析消息
            JsonNode event = new ObjectMapper().readTree(record.value());

            // 2. 用业务唯一键（比如订单 ID）去重
            String orderId = event.get("orderId").asText();
            if (processedIds.contains(orderId)) {
                System.out.println("Duplicate, skip: " + orderId);
                return;
            }

            // 3. 处理业务：发通知
            System.out.println("Sending notification for order: " + orderId);

            // 4. 标记为已处理
            processedIds.add(orderId);
        } catch (Exception e) {
            // ⚠️ 抛异常 → Spring Kafka 会自动重试（默认 3 次）
            // 之后进入死信队列（如果配置了 DLQ）
            throw new RuntimeException(e);
        }
    }
}
```

### 4.8 配置文件

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/outbox_demo
    username: postgres
    password: postgres

  jpa:
    hibernate:
      ddl-auto: validate
    properties:
      hibernate.dialect: org.hibernate.dialect.PostgreSQLDialect

  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all                                # ★ 强持久化
      retries: 3
      properties:
        enable.idempotence: true               # ★ 生产端幂等

logging:
  level:
    com.example.outbox: DEBUG
```

### 4.9 本地运行速跑

1. 启动 Postgres + Kafka（见 4.11 的 docker-compose）
2. 启动 Spring Boot：

   ```bash
   mvn spring-boot:run
   ```
3. 下一单的 HTTP 请求（同一目录下新建个 `request.sh` 就行）

> **完整端到端排错、Postgres 验证、Kafka UI 验证**等放在 4.11 之后看。

### 4.10 整体链路图

```
HTTP POST /orders
       │
       ▼
┌─────────────────┐
│   OrderService  │
│  @Transactional │
│                 │
│  1. INSERT      │
│     orders      │
│  2. INSERT      │   ←同一事务
│     outbox_msgs │
│  COMMIT         │
└────────┬────────┘
         │
         │ (1秒后)
         ▼
┌─────────────────┐
│   OutboxPoller  │
│                 │
│  SELECT未发布 → │
│  Kafka.send →  │
│  UPDATE发布 = now │
└────────┬────────┘
         │
         ▼
   ┌───────────┐
   │   Kafka   │
   │  topic:   │
   │  OrderPlaced │
   └─────┬─────┘
         │
         ▼
┌─────────────────────┐
│ NotificationConsumer │
│  - 幂等去重           │
│  - 业务处理（发通知）  │
└─────────────────────┘
```

### 4.11 一键启动环境：docker-compose

上一节只给了 `mvn spring-boot:run`，但没给 Postgres + Kafka 怎么起。下面是一份**直接 `docker compose up -d` 就能跑**的最小环境，放在 `outbox-demo/docker-compose.yml`：

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:16-alpine
    container_name: outbox-postgres
    environment:
      POSTGRES_DB: outbox_demo
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./initdb:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 10

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      # ★ 这两个是让外部（宿主机 Spring Boot）能访问的关键
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka]
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

把 4.3 节的 schema 放到 `outbox-demo/initdb/01-schema.sql` 里，Postgres 启动时**自动建表**。

```bash
# 启动
docker compose up -d

# 看日志
docker compose logs -f kafka

# 进 Postgres 验证
docker exec -it outbox-postgres psql -U postgres -d outbox_demo
\dt   -- 看表是否建好

# 进 Kafka UI 验证
open http://localhost:8081

# 启动 Spring Boot
cd outbox-demo
mvn spring-boot:run
```

> **小贴士**：Kafka 的两个 listener (`9092` 内部 + `29092` 宿主机) 经常是"装完连不上"的根因，记这个组合能少踩坑。

### 4.12 测试一下：发个请求看全链路

加一个 `OrderController` 把下单暴露成 HTTP 接口（4.5 节的 `OrderService` 直接注入即可）：

```java
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<Order> create(@RequestBody CreateOrderRequest req) {
        Order order = orderService.placeOrder(req.userId(), req.amount());
        return ResponseEntity.ok(order);
    }

    public record CreateOrderRequest(Long userId, java.math.BigDecimal amount) {}
}
```

完整验证流程：

```bash
# 1. 发请求
curl -X POST http://localhost:8080/orders \
     -H "Content-Type: application/json" \
     -d '{"userId": 1, "amount": 99.99}'

# 2. 看 Spring Boot 控制台，预期依次打印：
#    - OrderService: 写 orders 成功 + 写 outbox 成功
#    - OutboxPoller: 1 秒后"Published outbox message ... to topic OrderPlaced"
#    - NotificationConsumer: "Sending notification for order: 1"

# 3. 进 Postgres 看 outbox 表
docker exec -it outbox-postgres psql -U postgres -d outbox_demo \
  -c "SELECT id, event_type, aggregate_id, published_at FROM outbox_messages;"
# 预期：published_at 不再是 NULL

# 4. 进 Kafka UI（http://localhost:8081）
#    找到 OrderPlaced topic，能看到一条消息，payload 里有 orderId/userId/amount
```

> **如果看不到消息**：先看 Spring Boot 日志是否报错；再看 `published_at` 是否更新；最后看 Kafka UI 里 `OrderPlaced` topic 是否存在。**这三步是 80% 排错的固定路径**。

---

---

## 5. 关键陷阱与最佳实践

### 5.1 必须做到的事情

| # | 陷阱 | 解法 |
|---|------|------|
| 1 | **Kafka send 不抛异常但丢消息** | 用 `send().get()` **同步等待 ACK**；Spring Kafka 默认配置即可 |
| 2 | **Poller 并发跑导致重复投递** | 用 `SELECT FOR UPDATE SKIP LOCKED`（PG）或 `@@readpast`（SQL Server） |
| 3 | **Outbox 表无限膨胀** | 定期清理 published_at 非 NULL 且超过 N 天的记录 |
| 4 | **消费端重复消费** | **必须做幂等**（按业务主键去重），at-least-once 不可能 0 重复 |
| 5 | **Kafka producer 配置不到位** | `acks=all` + `enable.idempotence=true`，否则数据可能丢 |
| 6 | **payload 序列化浪费 CPU** | 用 fastjson2 / Jackson afterburnermodule，避免反射热点 |

### 5.2 进阶：SELECT FOR UPDATE SKIP LOCKED

如果跑多个 poller 实例（高可用），要避免重复处理：

```sql
-- PostgreSQL：跳过被锁住的行
SELECT * FROM outbox_messages
WHERE published_at IS NULL
ORDER BY created_at
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

```java
// Repository
@Query(value = """
    SELECT * FROM outbox_messages
    WHERE published_at IS NULL
    ORDER BY created_at
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
    """, nativeQuery = true)
List<OutboxMessage> fetchPendingBatch(@Param("limit") int limit);
```

这样多个 poller 实例可以并行，**互不打架**。这是企业级 outbox 的标配。

### 5.3 进阶：失败重试与死信队列

Poller 自己也要有"死信"，否则某条脏数据会卡死整个队列：

```java
@Scheduled(fixedDelay = 1000)
public void publishPending() {
    var batch = repo.fetchPendingBatch(100);
    for (var msg : batch) {
        try {
            kafka.send(...).get();
            markPublished(msg);
        } catch (Exception e) {
            msg.setRetryCount(msg.getRetryCount() + 1);
            if (msg.getRetryCount() > 10) {
                // ★ 投递到 DLQ topic，留给人工处理
                kafka.send("outbox.DLQ", msg.getPayload());
                msg.setPublishedAt(LocalDateTime.now());
            }
            repo.save(msg);
        }
    }
}
```

### 5.4 进阶：表分区（性能关键）

当 outbox 表达到千万级，poller 的 SELECT 会很慢。**按创建时间分区**：

```sql
CREATE TABLE outbox_messages (
    ...
    created_at TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE outbox_2026_07 PARTITION OF outbox_messages
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
```

这样按月 DROP 老分区即可，poller 也只走最近的几个分区。

### 5.5 进阶：payload 演进（Schema 演进陷阱）

outbox 长期运行后一定会遇到——**新增/删除 payload 字段、调整字段含义**。但 outbox 表里是**历史消息的 JSON 字符串**，所以你不能像改业务表那样直接改：

```
问题场景：
  · v1 消息：{"orderId": 1, "userId": 100, "amount": 99.99}
  · v2 消息：{"orderId": 1, "userId": 100, "amount": 99.99, "currency": "CNY"}

  下游消费者既会读到 v1 消息，也会读到 v2 消息。
  如果消费端直接反序列化到 record.currency() → v1 消息会 NPE。
```

**两条经验法则**：

1. **消费端必须能容忍"缺字段"**——所有字段用 `Optional` / 默认值 / `JsonNode.get(field).asXxx(default)`。
2. **永远只做加法、别做减法**：
   - ✅ 改：加一个 `currency` 字段（默认值兜底）
   - ❌ 改：把 `amount` 拆成 `amount` + `discount`，且删除 `amount`
3. **如果必须做破坏性变更**：
   - 升 `event_type`：`OrderPlaced` → `OrderPlacedV2`
   - 老 V1 topic 保留几天，让消费端慢慢消费完
   - 下游服务分阶段切到 V2
4. **payload 用 `JsonNode` / Map 反序列化**（不要直接反序列化到强类型 POJO），能给将来留出最大兼容空间。

> **一句话**：outbox 的 payload 是"历史档案"，只能加、不能减。

### 5.6 进阶：业务表删字段后历史消息怎么办？

```
场景：orders 表把 status 字段从 VARCHAR 改成枚举 ENUM('PENDING','PAID','CANCELLED','REFUNDED')
     · 但 outbox 表里 3 个月前发的 OrderPaid 消息里 status='PAID' 是字符串
     · 下游消费端解析也没问题——JSON 不在乎你 DB 字段怎么改
     · 但如果业务表结构变了，旧消息的"业务含义"也得跟着变时
```

**正确做法**：

- outbox 表**只动 payload 内容**（不删除字段，只补字段）
- 业务表**DDL 变更前**，先把所有"过老的 outbox 消息"清掉（轮询 `created_at < cutoff` 的就删除 / 归档）
- 用一个**版本号字段** `event_version`（`v1` / `v2`）让消费端按版本路由

```java
// 推荐：outbox 表里加 event_version 字段
msg.setEventType("OrderPlaced");
msg.setEventVersion(2);  // v1 / v2 显式标注
// payload 里也写一份
msg.setPayload("""
  {"eventVersion": 2, "orderId": %d, "userId": %d, "amount": %s, "currency": "CNY"}
  """.formatted(order.getId(), userId, amount));
```

> **这样消费端可以按 event_version 走不同反序列化逻辑 / 不同 topic，长期可演进。**

---

## 6. 与其他模式的对比

| 模式 | 一致性保证 | 性能 | 复杂度 | 适用场景 |
|------|---------|------|--------|---------|
| **同步发 MQ**（无 outbox） | 弱 | 高 | 低 | 可容忍少量丢失的日志、通知 |
| **分布式事务 2PC** | 强 | 低 | 高 | 金融核心（已越来越少用） |
| **RocketMQ 事务消息** | 强 | 中 | 中 | 强绑定 RocketMQ 的系统 |
| **Polling Outbox** | 强 | 中 | 中 | **绝大多数业务**（推荐） |
| **CDC（Debezium）** | 强 | 高 | 高 | 大规模、高实时要求 |

**结论**：对一个后端工程师来说，**掌握 Polling Outbox + Debezium CDC 两条路线**，基本上能应对 95% 的"双写"场景。

---

## 7. 总结

### 7.1 Outbox 模式核心三句话

1. **把消息当成业务数据**：别再"先写库再发 MQ"，改成"同一事务写业务表和 outbox 表"。
2. **后台异步搬运**：poller（轮询）或 Debezium（CDC）把 outbox 表里的消息搬运到 MQ。
3. **消费端必须幂等**：at-least-once 是常态，靠消费端去重实现"业务幂等"。

### 7.2 适用决策

```
是否需要 100% 不丢消息？
  │
  ├── 否 → 同步发 MQ（接受少量丢失）
  │
  └── 是 → 是否绑定了 RocketMQ？
              │
              ├── 是 → 用 RocketMQ 事务消息
              │
              └── 否 → 用 Outbox（polling 或 CDC）
```

### 7.3 下一步可深入的方向

- **CDC（Debezium）实战**：让 DB 变更自动成为事件，业务代码"零侵入"
- **Kafka Streams 配合 Outbox**：在 outbox poller 上做实时聚合
- **跨服务 outbox 治理**：多服务下统一 outbox 表 vs 各服务独立表
- **可观测性**：把 outbox 表的 lag（积压量）作为关键业务指标

### 7.4 与 P1 学习计划的对照与验收标准

你在《消息队列学习总结.md》里 P1 计划是"Outbox 模式实操"。本文档已经覆盖的内容对应如下：

| P1 子项 | 本文对应位置 | 验收方式 |
|--------|------------|---------|
| 知道 Outbox 解决什么问题 | 1.2 / 1.3 | 能否向同事一句话讲清"双写不一致" |
| 掌握核心原理 | 2.1 / 2.4 | 能否画出"业务 TX + outbox + poller"链路图 |
| 两种实现方式取舍 | 3.x | 给出具体场景，能选 Polling 还是 CDC |
| 完整可跑 demo | 4.x | `docker compose up -d && mvn spring-boot:run` 能成功下单 |
| 至少识别 5 个陷阱 | 5.x | 5.1/5.2/5.3/5.4/5.5/5.6 六个坑是否都"踩过 + 修复过" |
| 和其他模式对比 | 6.x | 能否一句话说明 Outbox vs 2PC vs 事务消息的取舍 |
| 抽象再理解（"一张表"） | 第 8 章 | 能否用"录像带"或"账本"类比向新人讲清 |

**完成本文档学习后，可勾选**：

- [x] P1: Outbox 模式实操（理论 + 最小可跑代码 + 5+ 陷阱识别）

---

## 8. 通俗再理解：录像带与账本类比

> 本章专门回答一个问题：**"到底 Outbox 表在干什么？"** —— 用最直观的方式对齐你和它之间的"心像"。

### 8.1 一句话对齐

```
outbox 表 = 一本"业务事件的录像带" / "账本副本"
· 录什么：业务里刚刚发生了什么（订单已付、库存已扣、用户已注册……）
· 谁来读：poller（定时）或 Debezium（binlog）
· 读完之后：标记 published_at，留一份几小时~几天后归档
```

### 8.2 录像带类比

| 录像带的特征 | 对应到 outbox 表 |
|------------|----------------|
| 录制是"同步"的——按下录制键的瞬间就录了 | 业务事务里 `INSERT outbox` 同步写完 |
| 录完**不会自动擦掉** | 投递成功也只标记 `published_at`，不删行 |
| 录像带可以**反复重看** | poller 重启后还能 SELECT 到历史 PENDING 行 |
| 录像是**真实发生的事**——不能事后伪造 | payload 内容是事务里的业务数据原样写入 |
| 录像带是"事件流"的本质——**连续不断** | outbox 表天然按 `created_at` 顺序追加 |

### 8.3 账本副本类比

更具体地，可以把 outbox 表想成"双账本体系"：

```
    你的业务库 = "主账本"
        │
        │ 同一次交易，记两笔
        ▼
    outbox 表 = "通知账本"
        │
        │ 异步对账
        ▼
    Kafka = "对外报送的清单"
```

- 主账本变 → 通知账本也必须变（同一笔交易）
- 主账本有错 → 通知账本也有错（回滚就一起回滚）
- 通知账本送出去之后，对外系统看到的就是"权威事件"

> **最关键的两点**：
> 1. **"通知账本"必须和"主账本"在同一笔交易里**——这才能保证"要么都记上、要么都没记"。
> 2. **"通知账本"可以晚一点才送出去**——这就把"主业务不能等"的强约束解开了。

### 8.4 用一张灵魂图对齐"专门一张表"的直觉

```
┌──────────────────────────────────────────────────────┐
│                       业务库                            │
│                                                      │
│  orders 表                       outbox 表             │
│  ┌──────────┐                  ┌──────────┐         │
│  │  业务状态  │                  │  待发事件  │  ← 这就是"专门的那张表"  │
│  └──────────┘                  └──────────┘         │
│        │                              │              │
│        └──── 同一笔事务（强一致） ──────┘              │
│                                                      │
└──────────────────────────────────────────────────────┘
                          │
                          │ 异步搬运
                          ▼
                    ┌──────────┐
                    │   Kafka  │
                    └──────────┘
```

### 8.5 "为什么不让 poller 直接看 orders 表？"

这是常见疑问，**直接看 orders 表不行**——因为 orders 表里只存"当前状态"，没有"曾经发生的事件"。Kafka 里发出去的不是"订单现在是什么"，而是"订单曾经发生过什么"。所以必须有一张**专门记录"发生过什么"**的表——这就是 outbox。

| 看的表 | 看到的内容 | 适不适合作为消息发送 |
|--------|----------|----------------|
| orders | 当前状态（PAID） | ❌ 没有上下文（什么时候付的？谁付的？） |
| outbox | 事件（OrderPaid，含全部字段） | ✅ 完整、可重放 |

> **一句话**：outbox 表的"专门"两个字不是技术细节的强调，而是**为事件流提供一个独立的、有时序的、可重放的存储**。

### 8.6 三个直观的"为什么"

| 问题 | 直观回答 |
|------|---------|
| **为什么不能直接 send Kafka？** | 那是网络 IO，事务回滚不可控；outbox 是 DB 本地写，受事务保护 |
| **为什么不用 Redis 缓存一下？** | 缓存丢了就丢消息；DB 是强一致 + 持久化 |
| **为什么要异步搬运而不是事务内发？** | 事务内发就把"DB 同步"和"MQ 异步"两套协议硬塞一起——outbox 把两者解耦，事务只管"录下事件"，搬运是另一件事 |

### 8.7 一句话总结

> **Outbox 表就是"业务的录像带"**——每发生一件事就录一帧，事务保证录像和数据一起落，poller 异步把录像带"播放"给 Kafka。

---

## 9. 进阶话题

### 9.1 Outbox 表设计：每服务一张 vs 全局一张

微服务架构下两种典型选择：

| 方案 | 优点 | 缺点 |
|------|------|------|
| **每服务独立 outbox 表** | 服务自治，DDL 互不干扰 | 跨服务事件查询麻烦 |
| **全局统一 outbox 表**（一张库多服务共用） | 便于 CDC 一次性订阅 | 字段难以统一、跨服务耦合 |

> **业界主流**：每服务一张。`outbox_messages` 通常放在自己的业务库里。

### 9.2 用 Debezium 监听 outbox 表 → 零侵入搬运

如果你不想自己写 poller，可以让 Debezium 直接订阅 outbox 表的 binlog：

```
postgres.outbox_messages  (WAL)
        │
        ▼
Debezium Postgres Connector
        │
        │ 把每行 INSERT 转成一条 Kafka 消息
        ▼
Kafka topic: outbox.order_events
        │
        ▼
    下游消费
```

Debezium 配 `transforms=outbox` 还能**自动从 payload JSON 里拆出 routing key**，连 Kafka topic 都帮你选好。

**对比自研 poller**：

| 维度 | 自研 poller | Debezium |
|------|----------|----------|
| 实时性 | 秒级 | 毫秒级 |
| 业务侵入 | 要写 outbox 写入逻辑 | 同样要写 outbox，但**不用写 poller** |
| 运维 | 简单 | 复杂（要跑 Kafka Connect 集群） |
| 事务回滚 | outbox 是 INSERT，回滚也一起回滚 | binlog 只能看到 commit 的，无法回滚（**所以必须用 outbox 表**） |

> **Debezium + outbox 表 = 业界事实标准**。Debezium 不替代 outbox，而是**只替代 poller**。

### 9.3 监控与积压告警：lag 是核心指标

outbox 表的积压量（`COUNT(*) WHERE published_at IS NULL`）是最关键的业务指标：

```sql
-- 积压量
SELECT COUNT(*), MAX(created_at) AS oldest_pending
  FROM outbox_messages
 WHERE published_at IS NULL;

-- 积压告警
-- 规则：积压量 > 10000 或 oldest_pending > 5 分钟 → 告警
```

**为什么 outbox 积压 = 业务积压**：

- 积压 1 万条 = 1 万个事件没发出
- `oldest_pending` > 5 分钟 = 下游 5 分钟前就开始落后
- 持续增长 = 消费者太慢 / Kafka 抖动 / 业务峰值

**告警指标建议**（最少监控这三个）：

| 指标 | 含义 | 阈值示例 |
|------|------|---------|
| `outbox_pending_count` | 未发布消息数 | > 10000 |
| `outbox_oldest_pending_seconds` | 最老未发布消息的"年龄" | > 300s |
| `outbox_publish_lag_seconds` | 平均从创建到发布的延迟 | > 10s |

### 9.4 outbox 适合多大并发量？

粗略经验值（**Polling 方案**）：

| 数据库 | 单机 poller 吞吐 |
|--------|----------------|
| PostgreSQL | 1k~5k msg/s |
| MySQL | 1k~5k msg/s |
| 单机瓶颈主要在：DB 写入 + Kafka send | — |

**横向扩展**：

- poller 加机器（用 `SELECT FOR UPDATE SKIP LOCKED` 让多 poller 互不打架）
- 拆 outbox 表（按 aggregate_id 哈希拆到 N 张表）
- 升级到 CDC（Debezium 是真正的横向扩展方案）

### 9.5 Outbox 与 Event Sourcing 的区别

两者容易混淆：

| 维度 | Outbox | Event Sourcing |
|------|--------|----------------|
| 业务状态存哪 | DB 业务表（订单状态 = 字段） | 只存事件，**当前状态由回放事件得出** |
| 状态来源 | 业务表 | 事件流的"投影" |
| 适合场景 | **绝大多数业务** | 审计重、状态变迁复杂（金融、账户） |
| 复杂度 | 中 | 高 |

> **Outbox 是"事件作为通知"，ES 是"事件作为真相"**。Outbox 业务表里仍存当前状态，事件只是"通知下游发生了什么"；ES 没有"当前状态"这个概念。

### 9.6 跨服务 outbox 治理

多服务场景下，要约定：

```
约定 1：outbox 表的 schema 各服务独立，但字段命名一致
       · id, aggregate_type, aggregate_id, event_type, payload, created_at, published_at, retry_count

约定 2：event_type 命名空间按服务隔离
       · order-service: OrderPlaced, OrderPaid, OrderShipped
       · payment-service: PaymentCaptured, PaymentRefunded
       · 不允许用同一个 event_type 跨服务

约定 3：payload 里 aggregate_type 必须出现
       · 下游可以按 aggregate_type 路由到不同处理逻辑

约定 4：发布失败的告警统一打到同一个告警平台
       · 任何一个服务的 outbox 积压告警都走同一个 channel
```

### 9.7 不适用 Outbox 的场景

不是所有业务都适合：

- **可容忍少量丢失的日志/埋点** —— 直接发 Kafka 即可
- **超高吞吐（10 万 msg/s 以上）** —— 用 CDC + Kafka，别自研 poller
- **业务状态本身就是要"事件回放"** —— 用 Event Sourcing
- **业务库就是事件存储**（EventStoreDB）—— 没有"业务表"和"outbox 表"的概念了

### 9.8 一句话回顾 Outbox 设计的 7 个关键决策

```
1. schema:          至少包含 id / event_type / payload / created_at / published_at
2. 写入:            @Transactional 里和业务表一起写
3. 搬运:            Polling (简单) 或 Debezium CDC (高吞吐)
4. 并发:            SELECT FOR UPDATE SKIP LOCKED
5. 失败:            retry_count 超限 → DLQ topic
6. 性能:            按 created_at 分区表 + 定期清理
7. 监控:            积压量 + 最老未发延迟 + 平均 publish 延迟
```

---

*创建时间：2026-07-03*
*最后更新：2026-07-03*
*配套文档：消息队列学习总结.md*
*学习进度：P1（Outbox 模式实操）— ✅ 已完成学习*
*文档总行数：约 1100+ 行，含原理 / 两种实现对比 / Spring Boot 完整代码 / 6 大陷阱 / 模式对比 / 通俗类比 / 进阶话题*
