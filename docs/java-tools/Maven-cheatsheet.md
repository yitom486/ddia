# Java 工具链速查 — Maven + JMH + JOL

> 本文档沉淀 Maven 命令、JMH/JOL 是什么、命令行 vs IDE 的取舍，以及"Java 命令行跑代码"的最小粒度。
> 配合 `code/disruptor-lab` 学习使用——这是这个项目第一次正经用上 maven 命令行。

---

## 目录

1. [Maven 是什么——一句话定位](#1-maven-是什么一句话定位)
2. [与 npm / uv / pip 的类比](#2-与-npm--uv--pip-的类比)
3. [必背的 8 条 mvn 命令](#3-必背的-8-条-mvn-命令)
4. [常用参数与组合技巧](#4-常用参数与组合技巧)
5. [Maven 生命周期一张图](#5-maven-生命周期一张图)
6. [命令行 vs IDE：含金量与取舍](#6-命令行-vs-ide含金量与取舍)
7. ["最小可运行单位"是什么](#7-最小可运行单位是什么)
8. [JMH 是什么——可信的微基准](#8-jmh-是什么可信的微基准)
9. [JOL 是什么——看穿对象布局](#9-jol-是什么看穿对象布局)
10. [JMH vs JOL：一句话区分](#10-jmh-vs-jol一句话区分)
11. [本项目里怎么用](#11-本项目里怎么用)

---

## 1. Maven 是什么——一句话定位

**Maven 是 Java 生态的"包管理 + 构建 + 跑测试 + 跑 main"一站式工具。**

- 包管理 = 自动下载第三方库到本地缓存
- 构建 = 把 `.java` 编成 `.class`，再打成 `.jar`
- 测试 = 跑 JUnit
- 跑 main = 等价于 `java XxxMain`，但少打一堆 classpath

所有这些事都通过一个清单文件 `pom.xml` 描述。

---

## 2. 与 npm / uv / pip 的类比

| 类比 | 包仓库 | 锁文件 | 主要动作 | Java 对应 |
|------|--------|--------|---------|-----------|
| **npm** | npmjs.com | `package-lock.json` | `npm install / test / run build` | Maven |
| **pip / uv** | pypi.org | `requirements.txt` / `uv.lock` | `uv pip install / run` | Maven |
| **cargo** | crates.io | `Cargo.lock` | `cargo build / run / bench` | Maven |
| **go modules** | proxy.golang.org | `go.sum` | `go build / test / run` | Maven |

### 2.1 `pom.xml` ≈ `package.json`

```xml
<!-- pom.xml 节选（disruptor-lab 项目） -->
<dependencies>
    <dependency>
        <groupId>org.openjdk.jmh</groupId>
        <artifactId>jmh-core</artifactId>
        <version>1.37</version>
    </dependency>
    <dependency>
        <groupId>org.openjdk.jol</groupId>
        <artifactId>jol-core</artifactId>
        <version>0.17</version>
    </dependency>
</dependencies>
```

| npm 里的概念 | Maven 对应 |
|------------|-----------|
| `package.json` | `pom.xml` |
| `node_modules/`（仓库级、随项目走） | `~/.m2/repository/`（全局级、所有项目共享） |
| `package-lock.json` | 自动维护，你不用手写 |
| `node script.js` | `mvn exec:java -Dexec.mainClass=...` |
| `npx xxx` | ❌ Maven 没有内建，习惯装到 PATH（用 sdkman！）|

### 2.2 一个关键差异

- **npm**：`node_modules` 在项目里，删项目文件就没了
- **Maven**：依赖缓存到 `~/.m2/repository`，删项目文件依赖还在 → 重装只需要几秒

这意味着：**不同项目共享同一个第三方库，省空间、省下载时间**。

---

## 3. 必背的 8 条 mvn 命令

```bash
# 1. 编译（只到 .class，不到 jar）
mvn compile

# 2. 跑测试
mvn test

# 3. 打 jar 包（输出到 target/xxx-1.0.0-SNAPSHOT.jar）
mvn package

# 4. 清理 target/ 目录
mvn clean

# 5. 安装到本地仓库（~/.m2/repository/...）——自己写多模块时用
mvn install

# 6. 跑某个 main 方法（demo 用得最多）
mvn exec:java -Dexec.mainClass=com.x.MyMain

# 7. 清干净再重新编译一遍
mvn clean compile

# 8. 跳过测试
mvn package -DskipTests
```

### 3.1 整个 cheat sheet 速记表

| 命令 | 等价的 npm | 何时用 |
|------|----------|--------|
| `mvn compile` | `tsc` (TypeScript 编译) | 改完 java 想看一下能不能过编译 |
| `mvn test` | `npm test` | 跑 JUnit |
| `mvn package` | `npm run build` | 出 jar 包 |
| `mvn install` | `npm publish` 到私有源 | 让别的本地项目能当依赖 |
| `mvn exec:java` | `node script.js` | 跑 demo/工具 |
| `mvn dependency:tree` | `npm ls` | 看依赖冲突 |
| `mvn clean` | `rm -rf dist/` | 想重新来过 |
| `mvn -DskipTests package` | `npm run build -- --skip-tests` | 不跑测试直接打包 |

---

## 4. 常用参数与组合技巧

### 4.1 行为开关类

```bash
# 静默模式：只输出 ERROR/最终结果
mvn -q exec:java -Dexec.mainClass=...

# 跳过测试
mvn package -DskipTests

# 离线模式（不联网用本地缓存）
mvn -o package

# 多线程编译（4 核都用上）
mvn -T 4 compile

# 看完整依赖树
mvn dependency:tree -Dverbose
```

### 4.2 组合形式

```bash
# 清干净再重新打 jar
mvn clean package

# 一次过：清 + 编译 + 跑 demo
mvn clean compile exec:java -Dexec.mainClass=io.x.Main

# 完整流水线（CI 风格）
mvn clean verify
# = clean + compile + test + package + integration-test + verify
```

### 4.3 传参到 main 的方式

```bash
# 通过 -D 传给 main 的 args（注意：不是每个工具都识别）
mvn exec:java -Dexec.mainClass=com.x.Main -Dexec.args="arg1 arg2"

# 推荐方式：在 main 里用 System.getProperty("xxx") 自己读
mvn exec:java -Dexec.mainClass=com.x.Main -DmyFlag=true
```

### 4.4 多模块项目里精准定位

```bash
# 只编译指定子模块
mvn -pl :module-a compile

# 加上 -am（also-make）：把依赖一起编了
mvn -pl :module-b -am package

# 跳过某些模块
mvn -pl '!,module-x' clean install

# 注意：mvn 操作"模块"靠 pom 里的 <modules> 声明
```

---

## 5. Maven 生命周期一张图

```
pom.xml + src/main/java + src/test/java
              │
              ▼
        ┌────────────┐
        │ validate   │   校验 pom、目录结构
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ compile    │   .java → target/classes/.class
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ test       │   跑 JUnit（src/test/java 里的 @Test）
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ package    │   打成 jar：target/xxx-1.0.0-SNAPSHOT.jar
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ install    │   装到 ~/.m2/repository（本地共享）
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ deploy     │   上传到公司私有仓库（一般在 CI 上）
        └────────────┘
```

### 5.1 一个关键事实：执行 `mvn package` 会**先**跑完它之前的阶段

> **所以你 `mvn package` 没跑测试？不对——它会先 compile 再 test 再 package。**
>
> 这就是为什么 `mvn package -DskipTests` 是个常用选项——它跳过 **test 阶段**，而不是只跳过 package。

### 5.2 不属于内置生命周期的"插件目标"

像 `exec:java`、`dependency:tree` 这种是**插件**提供的，不是生命周期。

- 写法：`插件名:目标名`
- 例：`mvn exec:java`、`mvn dependency:tree`

它们必须在某个生命周期阶段**之前运行**，否则 fail。`exec-maven-plugin` 自己设置好绑定到 `test` 阶段。

---

## 6. 命令行 vs IDE：含金量与取舍

| 维度 | IDE 点点点 | mvn 命令行 |
|------|----------|----------|
| **可复现性** | 别人不知道你点的哪几个按钮 | `mvn package` 一行，所有人一致 |
| **CI/CD** | IDE 不能跑在服务器上 | 命令行是工业标准 |
| **看细节** | IDE 把日志折叠了 | 命令行原汁原味，错误能 grep |
| **学原理** | 不会真的理解"编译怎么发生的" | 会知道 source → target → jar 这一套 |
| **并发** | 给 Maven 指定 fork 数 | `mvn -T 4 compile` 4 线程并行编译 |
| **可调试** | 一键打断点 | ❌ 不能，要靠日志或 jdb |
| **可自动化** | ❌ IDE 不能批处理 | shell 脚本一把梭 |
| **找文件** | 索引、跳转 | 靠 grep/ripgrep |

### 6.1 真实工程里的角色分工

> **命令行做构建，IDE 做开发。**

- 写代码、改代码、看代码：IDE
- 编译、跑测试、打包：终端 + mvn
- Debug 调试：IDE
- CI/CD：纯命令行

### 6.2 "含金量"建议

1. **先学会命令行**——你写一段 `mvn clean compile exec:java` 跑通一遍，比 IDE 点 10 个按钮学得多
2. **再学 IDE 的快捷键**——熟悉后能省时间，但别当 IDE User Only
3. **文档/复现脚本**一律写命令行——share 给别人不会因为 IDE 不同而失败

---

## 7. "最小可运行单位"是什么

提问："mvn 跑的必须是带 main 的类吗？" —— 看情况，按场景分。

### 7.1 各种入口对照

| 你想跑什么 | 怎么表达 | 备注 |
|----------|---------|------|
| **某个 main 方法** | `mvn exec:java -Dexec.mainClass=...Xxx` | 本项目里 4 个 demo 都是这个 |
| **某个测试方法** | `mvn test -Dtest=MyTest#methodName` | JUnit |
| **整个测试类** | `mvn test -Dtest=MyTest` | 跑一个类里所有 `@Test` |
| **所有测试** | `mvn test` | 整个项目的 JUnit |
| **JMH benchmark** | `java -jar target/xxx.jar MyBench` | JMH 必须 java -jar，不能 mvn test |
| **打成 jar 给别人用** | `mvn package -DskipTests` | 别人 `java -jar` 或当依赖 |

### 7.2 结论

- ✅ **demo 类** 必须带 `public static void main(String[] args)`
- ✅ **测试方法** 带 `@Test`
- ✅ **微基准方法** 带 `@Benchmark`
- ❌ **没有"指定跑普通工具方法"的语法**——普通方法，就在 `main` 里调它

### 7.3 一个反直觉的小知识

> **`mvn exec:java -Dexec.mainClass=Xxx`** — 这个"main class"是**全限定名**（带包名），不是文件路径。
>
> 错：`mvn exec:java -Dexec.mainClass=Main.java` ❌
>
> 对：`mvn exec:java -Dexec.mainClass=io.ddia.disruptor.lab.singleproducer.MiniSingleProducerDemo` ✅

---

## 8. JMH 是什么——可信的微基准

### 8.1 一句话

> **JMH（Java Microbenchmark Harness）是 OpenJDK 官方的"Java 微基准测试框架"。**

它解决了**手写时间统计不准**的问题：
- 让 JVM 先把代码跑热（JIT 编译完）
- 多轮迭代做统计
- 给出置信区间（± Error）
- 默认 fork JVM（消除旧进程残留状态干扰）

### 8.2 用法骨架

```java
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)     // 测 ops/秒
public class MyBench {

    long counter;

    @Benchmark
    public void increment() {
        counter++;
    }
}
```

跑：

```bash
# 1. 先打成 fat jar
mvn -q package -DskipTests

# 2. 用 java -jar 跑
java -jar target/xxx-1.0.0-SNAPSHOT.jar MyBench

# 3. （可选）控制预热轮数、迭代次数、线程数
java -jar target/xxx-1.0.0-SNAPSHOT.jar MyBench \
     -wi 3 -i 5 -f 1 -t 4
```

### 8.3 怎么读 JMH 输出

```
Benchmark                  Mode  Cnt    Score    Error   Units
MyBench.increment          thrpt    5  720.3 ±  12.4  ops/us

# 含义：
#   Mode  = throughput（吞吐模式）
#   Cnt   = 跑了 5 轮
#   Score = 720.3 million ops/秒
#   Error = 置信区间 ±12.4
#   Units = ops/us（每微秒多少次操作）
```

### 8.4 为什么不用 `System.nanoTime()`

```java
// ❌ 这种代码的可信度极低
long start = System.nanoTime();
doWork();
long elapsed = System.nanoTime() - start;
System.out.println("耗时: " + elapsed + "ns");
```

为什么不行？
- 第一次跑 JIT 还没热——耗时虚高
- 中间穿插 GC——噪声
- CPU 还没升频、还没降频——不可控
- 其他测试残留——互相干扰

JMH 自动做：**预热 + N 轮 + fork 子进程 + 统计置信区间**——把这些坑全填了。

### 8.5 JMH 在 disruptor-lab 里

```java
// code/disruptor-lab/.../falsesharing/FalseSharingBench.java
@Benchmark @Group("plain") @GroupThreads(1)
public void readPlain(State s) { s.value(); }

@Benchmark @Group("padded") @GroupThreads(1)
public void readPadded(State s) { s.value(); }
```

用来量化："加 cache line padding 之后，写吞吐**提升了几倍**"。

---

## 9. JOL 是什么——看穿对象布局

### 9.1 一句话

> **JOL（Java Object Layout）是 OpenJDK 出的"打印对象在堆里占多少字节、字段都在哪个偏移位置"的工具。**

它不能让你写快代码，但它能让你**亲眼看到 JVM 给你分配了什么**——这对理解 cache line padding、对象头、`@Contended` 都非常有帮助。

### 9.2 用法骨架

```java
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

System.out.println(ClassLayout.parseInstance(new Padded()).toPrintable());
System.out.println(GraphLayout.parseInstance(obj).toFootprint());
```

### 9.3 输出长这样

```
io.ddia.disruptor.lab.falsesharing.ObjectLayoutDemo$Padded object internals:
 OFFSET  SIZE   TYPE DESCRIPTION         VALUE
      0    12        (object header)     ...    ← 对象头（mark + class pointer）
     12     4    int Padded.alignment     0     ← 自动补位让 8 字节对齐
     16     8   long Padded.p00          0
     ...
    128     8   long Padded.v0          0       ← 你会看到 v0 和 v1 之间差 72 字节
    ...
    200     8   long Padded.v1          0           而不是默认的 8 字节
```

### 9.4 用法上的小技巧

```bash
# 想看更准确的布局（去掉压缩指针）
java -XX:-UseCompressedOops -jar target/xxx.jar ObjectLayoutDemo

# GraphLayout 还会递归打印整张对象图
System.out.println(GraphLayout.parseInstance(obj).toFootprint());
```

### 9.5 JOL 在 disruptor-lab 里

`ObjectLayoutDemo.java` 直接演示对比：
- `Plain` 对象里 4 个 `volatile long`，offset = 16 / 24 / 32 / 40 → **全在一条 64B cache line 内（伪共享）**
- `Padded` 对象里 4 个值，offset 隔约 72 字节 → **每值独占一条 cache line**
- `Sequence` 是 Disruptor 风格的 padding，左 7 + 右 7 → 也独占

---

## 10. JMH vs JOL：一句话区分

| JOL | JMH |
|-----|-----|
| 让你**看见**内存 | 让你**量化**速度 |
| 输出：对象布局、字段 offset、整体大小 | 输出：分数 ± 误差（带置信区间） |
| 一次性 `println` 即可 | 必须有 @Benchmark，跑几分钟 |
| 教学/验证用 | 性能调优的"证据" |

> **JOL 让你相信 "padding 真起作用了"，JMH 让你相信 "padding 真变快了"** —— 看到 + 量化，两件事。

---

## 11. 本项目里怎么用

### 11.1 第一次运行（首次需要联网下依赖）

```bash
cd /home/tomy/projects/java/ddia/code/disruptor-lab

# 一次性编译（首次约 30-60 秒，主要在拉 JMH/JOL）
mvn -q -DskipTests compile
```

### 11.2 跑 4 个入口

| Demo | 入口 | 命令 |
|------|------|------|
| **A. 单生产者 Demo** | `MiniSingleProducerDemo` | `mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.singleproducer.MiniSingleProducerDemo` |
| **B. 单 vs 多对比** | `SingleVsMultiProducerDemo` | `mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.compare.SingleVsMultiProducerDemo` |
| **C. JOL 看布局** | `ObjectLayoutDemo` | `mvn -q exec:java -Dexec.mainClass=io.ddia.disruptor.lab.falsesharing.ObjectLayoutDemo` |
| **D. JMH 跑基准** | `FalseSharingBench` | 见下 |

### 11.3 JMH 微基准（D）

```bash
# 第一步：打包成 fat jar
mvn -q package -DskipTests

# 第二步：用 java -jar 跑
java -jar target/disruptor-lab-1.0.0-SNAPSHOT.jar \
     io.ddia.disruptor.lab.falsesharing.FalseSharingBench \
     -wi 3 -i 5 -f 1 -t 4
```

→ 看到 `plain` vs `padded` 两组的 `score ± error` 对比，量化 cache line padding 的收益。

### 11.4 一个"懒人脚本"建议

```bash
# save as ./run-demo.sh
#!/bin/bash
DEMO=$1
[[ -z "$DEMO" ]] && { echo "Usage: ./run-demo.sh single|singleVsmulti|jol|bench"; exit 1; }
case $DEMO in
  single)        CLASS=io.ddia.disruptor.lab.singleproducer.MiniSingleProducerDemo ;;
  singleVsmulti) CLASS=io.ddia.disruptor.lab.compare.SingleVsMultiProducerDemo ;;
  jol)           CLASS=io.ddia.disruptor.lab.falsesharing.ObjectLayoutDemo ;;
  bench)
    mvn -q package -DskipTests
    exec java -jar target/disruptor-lab-1.0.0-SNAPSHOT.jar \
         io.ddia.disruptor.lab.falsesharing.FalseSharingBench -wi 3 -i 5 -f 1 -t 4 ;;
  *) echo "Unknown: $DEMO"; exit 1 ;;
esac
mvn -q exec:java -Dexec.mainClass=$CLASS
```

执行：`chmod +x run-demo.sh && ./run-demo.sh jol`

---

## 12. 进阶参考

| 概念 | 推荐阅读 |
|------|---------|
| Maven 全套命令 | https://maven.apache.org/guides/getting-started/ |
| JMH 官方 sample | https://github.com/openjdk/jmh/tree/master/jmh-samples |
| JOL sample | https://github.com/openjdk/jol |
| 多模块项目 | pom 里 `<modules>` 区块，和 `-pl` 参数联动 |
| mvnw (Maven Wrapper) | `./mvnw` 写法，让项目自带 Maven 版本 |
| Java 启动参数 | `-Xms / -Xmx / -XX:MaxGCPauseMillis` 等 JVM 调优 |

---

*创建时间：2026-07-04*
*配套文档：消息队列学习总结.md / Disruptor详解.md*
*位置：docs/java-tools/Maven-cheatsheet.md*
