# Maven vs Gradle：构建工具核心原理与设计思想

> 本文档剖析 Java 两大构建工具的设计哲学、核心原理、架构差异，以及它们在思想上的"合"与"分"。
> 适合已经会用 Maven/Gradle，想理解"为什么这样设计"的开发者。

---

## 目录

1. [先问本质：构建工具解决什么问题](#1-先问本质构建工具解决什么问题)
2. [两种范式：声明式 vs 脚本式](#2-两种范式声明式-vs-脚本式)
3. [Maven 核心原理](#3-maven-核心原理)
4. [Gradle 核心原理](#4-gradle-核心原理)
5. [依赖解析：两种思想的交锋](#5-依赖解析两种思想的交锋)
6. [生命周期与任务执行模型](#6-生命周期与任务执行模型)
7. [增量构建与缓存机制](#7-增量构建与缓存机制)
8. [思想融合：现代构建工具的趋势](#8-思想融合现代构建工具的趋势)
9. [选型建议：何时用 Maven，何时用 Gradle](#9-选型建议何时用-maven何时用-gradle)

---

## 1. 先问本质：构建工具解决什么问题

在深入技术细节之前，我们需要理解构建工具存在的根本原因。

### 1.1 软件构建的复杂性

一个现代 Java 项目的构建，远不止 `javac Main.java` 那么简单：

```
┌─────────────────────────────────────────────────────────────┐
│                    构建需要解决的问题                         │
├─────────────────────────────────────────────────────────────┤
│  1. 依赖管理：去哪里找 jar？版本冲突怎么办？                   │
│  2. 源码编译：如何组织目录结构？如何处理增量编译？              │
│  3. 资源处理：配置文件、国际化资源如何打包？                   │
│  4. 测试执行：单元测试、集成测试如何区分？                     │
│  5. 报告生成：测试覆盖率、Javadoc 如何生成？                   │
│  6. 部署打包：jar、war、fat-jar 如何打？                      │
│  7. 多模块协调：父子项目依赖关系如何管理？                     │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 构建工具的进化史

```
时间线：Java 构建工具的演进

1995 Java 诞生
    │
    ├─ 早期：手动 + Makefile（Make 是 C 的工具，勉强适配 Java）
    │
    ├─ 2000 年代初：Ant（Apache Ant）
    │   - 纯 XML 配置，任务（Task）驱动
    │   - 灵活但无规范，项目结构五花八门
    │
    ├─ 2004：Maven（Apache Maven）
    │   - "约定优于配置"（Convention over Configuration）
    │   - 标准化的项目结构
    │   - 声明式的依赖管理与生命周期
    │
    └─ 2008：Gradle
        - Groovy DSL → Kotlin DSL
        - 脚本式 + 声明式混合
        - 增量构建、构建缓存
        - Android 官方构建工具
```

---

## 2. 两种范式：声明式 vs 脚本式

这是理解 Maven 和 Gradle 差异的核心切入点。

### 2.1 声明式（Declarative）—— Maven

**核心思想：你告诉"要什么"，工具决定"怎么做"。**

```xml
<!-- Maven: 我声明需要 junit，版本 4.13 -->
<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.13</version>
    <scope>test</scope>
</dependency>
```

Maven 不关心你用什么方式去下载、缓存、解析这个依赖——你只说"我需要 junit 4.13"。

### 2.2 脚本式（Scripted）—— Gradle

**核心思想：你描述"怎么做"，工具执行你的步骤。**

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    testImplementation("junit:junit:4.13")
}
```

表面上看差不多，但关键是：这段代码是**可编程的**：

```kotlin
// Gradle: 可以写条件逻辑
val isSnapshot = version.toString().endsWith("-SNAPSHOT")

dependencies {
    if (isSnapshot) {
        implementation("com.example:lib:+")
    } else {
        implementation("com.example:lib:1.0.0")
    }
}
```

### 2.3 一张图理解两种范式

```
┌─────────────────────────────────────────────────────────────────┐
│                        构建工具的两种范式                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   声明式 (Maven)              脚本式 (Gradle)                   │
│   ─────────────              ──────────────                    │
│                                                                 │
│   ┌─────────────┐            ┌─────────────┐                    │
│   │  我要什么   │            │  我要怎么做  │                    │
│   │   (What)    │            │   (How)     │                    │
│   └──────┬──────┘            └──────┬──────┘                    │
│          │                          │                            │
│          ▼                          ▼                            │
│   ┌─────────────┐            ┌─────────────┐                    │
│   │ Maven 引擎  │            │ Gradle 引擎 │                    │
│   │   分析依赖   │            │   执行脚本   │                    │
│   │  执行生命周期 │            │  构建任务图  │                    │
│   └─────────────┘            └─────────────┘                    │
│                                                                 │
│   优点：简单、可预测          优点：灵活、可编程                  │
│   缺点：扩展需要插件          缺点：可能写出奇怪的东西             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Maven 核心原理

### 3.1 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        Maven 架构图                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   pom.xml ──────────► Maven Core Engine                         │
│         │                        │                              │
│         │                        ▼                              │
│         │            ┌─────────────────────┐                    │
│         │            │   生命周期 (Lifecycle) │                  │
│         │            │  clean │ compile │   │                   │
│         │            │  test  │ package │... │                   │
│         │            └─────────────────────┘                    │
│         │                        │                              │
│         │                        ├──► Plugin 1                  │
│         │                        ├──► Plugin 2                  │
│         │                        └──► Plugin N                  │
│         │                                                         │
│         │                        Plugin 执行顺序由 phase 决定     │
│         ▼                                                         │
│   ┌──────────────────┐                                            │
│   │   本地仓库        │  ~/.m2/repository                         │
│   │  (Local Repo)    │                                            │
│   └────────┬─────────┘                                            │
│            │                                                      │
│            │ (jar 包找不到?)                                       │
│            ▼                                                      │
│   ┌──────────────────┐                                            │
│   │  远程仓库        │  maven central / 公司私有 Nexus            │
│   │ (Remote Repo)    │                                            │
│   └──────────────────┘                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 约定优于配置（Convention over Configuration）

Maven 的灵魂原则：**如果你遵循我的目录结构，你几乎不需要配置。**

```
标准 Maven 项目结构：
─────────────────────
project/
├── pom.xml                    # 项目对象模型
├── src/
│   ├── main/
│   │   ├── java/              # 源代码（编译后进入 target/classes）
│   │   └── resources/         # 资源文件（复制到 target/classes）
│   └── test/
│       ├── java/              # 测试源代码
│       └── resources/         # 测试资源文件
└── target/                    # 所有构建产物在这里
    ├── classes/
    ├── test-classes/
    └── myapp-1.0.jar
```

**约定是什么？**

| 约定项 | Maven 默认值 |
|-------|-------------|
| 源代码目录 | `src/main/java` |
| 资源目录 | `src/main/resources` |
| 测试源码 | `src/test/java` |
| 编译输出 | `target/classes` |
| 打包格式 | `jar` |
| Java 版本 | 1.4（远古默认值） |

你不需要告诉 Maven"源码在哪里"——它假设你用的是上面的结构。

### 3.3 依赖作用域（Scope）

Maven 通过 `scope` 控制依赖在何时生效：

```xml
<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.13</version>
    <scope>test</scope>        <!-- 只在测试时生效 -->
</dependency>
```

| Scope | 编译时 | 测试时 | 运行时 | 打包进 jar |
|-------|--------|--------|--------|-----------|
| `compile` | ✓ | ✓ | ✓ | ✓ |
| `provided` | ✓ | ✓ | ✗ | ✗ |
| `runtime` | ✗ | ✓ | ✓ | ✓ |
| `test` | ✗ | ✓ | ✗ | ✗ |
| `system` | ✓ | ✓ | ✗ | ✗ |
| `import` | - | - | - | - |

**关键理解**：
- `provided`：容器已提供，如 Servlet API（Tomcat 自带）
- `runtime`：编译不需要，运行时需要，如 JDBC 驱动
- `test`：只有测试代码需要，如 JUnit

### 3.4 依赖传递与调解（Dependency Mediation）

当 A 依赖 B，B 依赖 C 时：

```
A → B (1.0) → C (2.0)
A → D (1.5) → C (1.8)
```

Maven 的**调解规则**：
1. 最短路径优先：A→D→C 比 A→B→C 更短，选 C (1.8)
2. 路径相同时，先声明优先：A 先声明 B，就用 B 的 C 版本

```xml
<!-- 强制指定版本（解决冲突） -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>32.1.3-jre</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

---

## 4. Gradle 核心原理

### 4.1 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        Gradle 架构图                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   build.gradle.kts ──► Gradle Build Scripts                     │
│         │                        │                              │
│         │                        ▼                              │
│         │            ┌─────────────────────┐                    │
│         │            │   Task Graph Builder │                   │
│         │            │  (有向无环图 DAG)      │                   │
│         │            └──────────┬──────────┘                    │
│         │                       │                                │
│         │                       ▼                                │
│         │            ┌─────────────────────┐                    │
│         │            │     Task Executor    │                   │
│         │            │   (并行 + 增量执行)    │                   │
│         │            └──────────┬──────────┘                    │
│         │                       │                                │
│         │            ┌──────────┴──────────┐                    │
│         │            │                       │                    │
│         │            ▼                       ▼                    │
│         │       ┌───────┐             ┌───────┐                  │
│         │       │ Task1 │             │ Task2 │                  │
│         │       └───────┘             └───────┐                  │
│         │                                       │                  │
│         ▼                                       ▼                  │
│   ┌──────────────────┐              ┌──────────────────┐        │
│   │   Build Cache    │              │   Daemon         │        │
│   │   (共享构建缓存)   │              │   (守护进程)       │        │
│   └──────────────────┘              └──────────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Task（任务）—— Gradle 的核心抽象

Maven 的基本单位是**阶段（Phase）**，Gradle 的基本单位是**任务（Task）**。

```kotlin
// 定义一个任务
tasks.register<Copy>("copyLibs") {
    from("libs/")
    into(buildDir.resolve("libs"))
}
```

**Task 有输入、输出、动作**：

```
┌─────────────────────────────────────────┐
│              Task 抽象                   │
├─────────────────────────────────────────┤
│  输入 ──► [动作/Action] ──► 输出        │
│  FileSet   doLast / doFirst   FileSet   │
└─────────────────────────────────────────┘
```

**为什么这很重要？**

```
Task 的输入输出让 Gradle 能够：
1. 判断任务是否需要执行（输入没变？跳过！）
2. 增量构建（只处理变化的部分）
3. 并行执行（没有依赖关系的任务同时跑）
4. 构建缓存（远程/本地缓存复用结果）
```

### 4.3 有向无环图（DAG）

Gradle 在执行任何任务之前，先构建一张任务依赖图：

```kotlin
// 声明任务依赖
tasks.register("deploy") {
    dependsOn("build", "sign", "upload")
}

tasks.register("build") {
    dependsOn("compileJava", "processResources")
}
```

```
任务依赖图（Graph）：
───────────────

         deploy
          / | \
         /  |  \
        v   v   v
      build sign upload
        |
        v
    ┌───────┴───────┐
    v               v
compileJava   processResources
```

**DAG 的特性**：
- **有向**：依赖关系有方向
- **无环**：不能循环依赖（A 依赖 B，B 依赖 A 会报错）
- **拓扑排序**：按依赖顺序执行

### 4.4 Gradle 的增量构建原理

```
传统构建：              增量构建（Gradle）：

[Clean]                  [检查输入]
   │                         │
   ▼                         ├─► 变了？─► 执行
[Build All]               ├─► 没变？─► 跳过（UP-TO-DATE）
   │                       │
   ▼                    [检查输出]
   ...                   ├─► 被删除？─► 重新执行
                       ├─► 存在且有效？─► 跳过
```

**验证机制**：

```
┌─────────────────────────────────────────────────────┐
│              增量构建的验证逻辑                       │
├─────────────────────────────────────────────────────┤
│                                                     │
│   上次构建状态：                                      │
│   ┌──────────────────────────────────────────────┐  │
│   │  input hash: abc123                          │  │
│   │  output files: [A.class, B.class]            │  │
│   └──────────────────────────────────────────────┘  │
│                        │                             │
│                        ▼                             │
│              比较 input hash                         │
│                        │                             │
│          ┌─────────────┴─────────────┐              │
│          ▼                           ▼               │
│    hash 相同                      hash 不同           │
│    输出文件存在                    输出文件缺失        │
│          │                           │               │
│          ▼                           ▼               │
│    ✓ 跳过执行 (SKIPPED)           ✗ 重新执行          │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### 4.5 脚本式 DSL 的能力

Gradle 的真正威力在于 DSL 可以做任何事：

```kotlin
// 条件化配置
plugins.withId("java") {
    val isCI = System.getenv("CI") == "true"
    
    tasks.withType<Test> {
        if (isCI) {
            maxHeapSize = "2g"
            setForkEvery(1)
        }
    }
}

// 代码生成
val generatedDir = layout.buildDirectory.dir("generated-src").get()
tasks.register("generateVersion") {
    val versionFile = file("$generatedDir/Version.kt")
    outputs.file(versionFile)
    doLast {
        versionFile.writeText("""
            package myapp
            object Version {
                const val VALUE = "${project.version}"
                const val TIMESTAMP = "${System.currentTimeMillis()}"
            }
        """.trimIndent())
    }
}
```

---

## 5. 依赖解析：两种思想的交锋

### 5.1 Maven 的依赖解析

**Maven 依赖解析流程**：

```
1. 解析 pom.xml 中的 <dependencies>
        │
        ▼
2. 检查本地仓库 ~/.m2/repository
        │
        ├─► 找到？─► 使用
        │
        └─► 没找到？─► 从远程仓库下载
        │                 │
        │                 ▼
        │          Maven Central / Nexus
        │                 │
        │                 ▼
        │          下载到本地仓库
        │
3. 处理传递依赖（transitive dependencies）
        │
        ▼
4. 依赖调解（ mediation）
```

**关键文件：settings.xml**

```xml
<!-- 本地仓库位置 -->
<localRepository>/path/to/my-local-repo</localRepository>

<!-- 镜像配置（国内加速） -->
<mirrors>
    <mirror>
        <id>aliyun</id>
        <mirrorOf>central</mirrorOf>
        <name>Aliyun Maven</name>
        <url>https://maven.aliyun.com/repository/public</url>
    </mirror>
</mirrors>

<!-- 认证信息（私有仓库） -->
<servers>
    <server>
        <id>my-nexus</id>
        <username>admin</username>
        <password>secret</password>
    </server>
</servers>
```

### 5.2 Gradle 的依赖解析

**Gradle 依赖解析流程**：

```
┌─────────────────────────────────────────────────────────────┐
│                   Gradle 依赖解析                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   build.gradle.kts                                           │
│         │                                                   │
│         ▼                                                   │
│   Configuration（配置项）                                    │
│   ┌─────────────────────────────────────────────────────┐  │
│   │  implementation  ──► 编译 + 运行时可见               │  │
│   │  api             ──► 编译 + 运行时 + 传递依赖可见      │  │
│   │  compileOnly     ──► 仅编译时可见                    │  │
│   │  runtimeOnly    ──► 仅运行时可见                    │  │
│   │  testImplementation                                   │  │
│   └─────────────────────────────────────────────────────┘  │
│         │                                                   │
│         ▼                                                   │
│   Repository Handler                                         │
│   ┌─────────────────────────────────────────────────────┐  │
│   │  mavenCentral()                                      │  │
│   │  google()                                            │  │
│   │  maven { url = uri("https://.../repo") }             │  │
│   │  flatDir { dirs("libs") }                           │  │
│   └─────────────────────────────────────────────────────┘  │
│         │                                                   │
│         ▼                                                   │
│   依赖图解析 + 版本冲突解决                                   │
│         │                                                   │
│         ▼                                                   │
│   解析结果缓存                                               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 版本冲突解决策略对比

```
Maven:
┌────────────────────────────────────────┐
│  <dependencyManagement> 强制指定       │
│  最短路径原则                          │
│  第一声明优先                          │
│  不能动态排除某个传递依赖               │
└────────────────────────────────────────┘

Gradle:
┌────────────────────────────────────────┐
│  动态排除/替换某个传递依赖              │
│  强制版本 (force)                       │
│  选择版本 (prefer)                      │
│  严格版本 (strictly)                   │
└────────────────────────────────────────┘
```

```kotlin
// Gradle: 精细控制版本
configurations.all {
    resolutionStrategy {
        // 强制使用某版本
        force("com.google.guava:guava:32.1.3-jre")
        
        // 动态替换某个传递依赖
        substitute(module("org.apache.httpcomponents:httpclient"))
            .using(module("org.apache.httpcomponents:httpclient:4.5.14"))
        
        // 严格模式（冲突时报错）
        strictly("1.0")
    }
}
```

---

## 6. 生命周期与任务执行模型

### 6.1 Maven 生命周期

Maven 有三套独立生命周期，**互不影响**：

```
┌─────────────────────────────────────────────────────────────────┐
│                        Maven 生命周期                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   clean          default          site                          │
│   ─────          ──────          ─────                          │
│     │               │               │                           │
│     ├── pre-clean   │               ├── pre-site                │
│     ├── clean ──────┼── compile ────┼── site                     │
│     └── post-clean  │               │                           │
│                     ├── test        ├── post-site                │
│                     │               │                           │
│                     ├── package ────┼── deploy                   │
│                     │               │                           │
│                     └── install     │                           │
│                                     │                           │
│   执行顺序：pre-clean → clean → post-clean                      │
│                                                                 │
│   注意：执行 mvn clean install = 先清理，再跑 default的install    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Phase 与 Plugin Goal 的绑定**：

```
Phase (阶段)              绑定的 Plugin:Goal
─────────────────────────────────────────────
process-resources     resources:resources (复制资源文件)
compile               compiler:compile
process-classes       (无默认绑定)
test                  surefire:test
package               jar:jar / war:war
install               install:install
deploy                deploy:deploy
```

### 6.2 Gradle 任务模型

```
┌─────────────────────────────────────────────────────────────────┐
│                        Gradle 任务模型                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   build.gradle.kts 中定义：                                      │
│                                                                 │
│   tasks.register("hello") {                                     │
│       doLast {                                                  │
│           println("Hello!")                                     │
│       }                                                         │
│   }                                                             │
│                                                                 │
│   执行：./gradlew hello                                         │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  Task Lifecycle:                                         │  │
│   │                                                           │  │
│   │  配置阶段 (Configuration)                                │  │
│   │  ┌─────────────────────────────────────────────────────┐ │  │
│   │  │  所有 Task 的 doFirst/doLast 被收集                   │ │  │
│   │  │  依赖关系被解析                                       │ │  │
│   │  │  属性被赋值                                          │ │  │
│   │  └─────────────────────────────────────────────────────┘ │  │
│   │                           │                               │  │
│   │                           ▼                               │  │
│   │  执行阶段 (Execution)                                    │  │
│   │  ┌─────────────────────────────────────────────────────┐ │  │
│   │  │  按 DAG 顺序执行 Task                                │ │  │
│   │  │  跳过 UP-TO-DATE 的 Task                             │ │  │
│   │  │  并行执行无依赖的 Task                                │ │  │
│   │  └─────────────────────────────────────────────────────┘ │  │
│   │                                                           │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.3 关键区别

| 特性 | Maven | Gradle |
|-----|-------|--------|
| 基本执行单元 | Phase（阶段） | Task（任务） |
| 执行顺序 | 线性生命周期 | DAG 图排序 |
| 并行执行 | 不支持 | 支持 |
| 增量构建 | 有限支持 | 完整支持 |
| 任务依赖 | 隐式（phase） | 显式（dependsOn） |

---

## 7. 增量构建与缓存机制

### 7.1 Maven 的增量构建

Maven 的增量能力相对有限：

```
Maven 的增量策略：
─────────────────
1. up-to-date 检查（基础版）
   - 如果源文件和目标文件时间戳相同，跳过
   
2. useIncrementalCompilation（Java 9+）
   - Java 编译器只编译受影响的类
   
3. 但：
   - 不会跳过整个 task
   - 依赖解析每次都重新执行
   - 没有真正的构建缓存
```

### 7.2 Gradle 的增量构建

```
Gradle 增量构建的完整机制：
──────────────────────────

┌─────────────────────────────────────────────────────────────┐
│  1. Input Checksum                                          │
│     ┌─────────────────────────────────────────────────────┐ │
│     │  源文件 ──► Hash ──► 比较上次 hash                    │ │
│     │  文件内容        如果相同 → 跳过整个 Task              │ │
│     └─────────────────────────────────────────────────────┘ │
│                                                             │
│  2. Output Checksum                                         │
│     ┌─────────────────────────────────────────────────────┐ │
│     │  检查 target/classes 中的 .class 文件                │ │
│     │  如果文件存在且 hash 匹配 → 跳过                    │ │
│     └─────────────────────────────────────────────────────┘ │
│                                                             │
│  3. 构建缓存 (Build Cache)                                  │
│     ┌─────────────────────────────────────────────────────┐ │
│     │                                                      │ │
│     │   本地缓存 (~/.gradle/caches)                        │ │
│     │           │                                          │ │
│     │           │  (开启后)                                 │ │
│     │           ▼                                          │ │
│     │   远程缓存 (Gradle Build Cache)                      │ │
│     │                                                      │ │
│     │   CI 服务器之间可以共享 .class 结果                   │ │
│     │                                                      │ │
│     └─────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

```kotlin
// 启用远程构建缓存
// gradle.properties
org.gradle.caching=true
org.gradle.remote.build-cache=true
org.gradle.remote.build-cache.url=https://cache.example.com
```

---

## 8. 思想融合：现代构建工具的趋势

### 8.1 Maven 的"脚本化"尝试

Maven 在演进中也在吸收 Gradle 的思想：

**1. Maven Wrapper（mvnw）**

```bash
# 不用安装 Maven，用项目自带的 mvnw
./mvnw clean install
```

**2. Maven Build Cache Extensions**

```xml
<!-- 启用构建缓存（需要额外插件） -->
<extensions>
    <extension>
        <groupId>org.apache.maven.extensions</groupId>
        <artifactId>build-cache-extension</artifactId>
        <version>1.0.0</version>
    </extension>
</extensions>
```

**3. 多模块项目的 BOM（Bills of Materials）**

```xml
<!-- 统一管理依赖版本 -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava-bom</artifactId>
            <version>32.1.3-jre</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### 8.2 Gradle 的"声明化"改进

Gradle 也在向更声明式的方向演进：

**1. Gradle Plugins（更像 Maven 的插件机制）**

```kotlin
// 应用插件，就像 Maven 引入依赖
plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}
```

**2. Platform 和 BOM 支持**

```kotlin
// 类似 Maven BOM
val springBootPlatform = platform("org.springframework.boot:spring-boot-dependencies:3.1.5")
dependencies {
    implementation(platform)
    implementation("com.google.guava:guava")
}
```

### 8.3 未来趋势

```
┌─────────────────────────────────────────────────────────────┐
│               构建工具的未来方向                               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. 配置即代码                                                │
│     - Maven 开始支持 Groovy 脚本                             │
│     - Gradle DSL 越来越声明式                                │
│                                                             │
│  2. 统一依赖管理                                              │
│     - Maven BOM vs Gradle Platform                          │
│                                                             │
│  3. 更好的增量与缓存                                          │
│     - 两者都在加强构建缓存能力                                │
│                                                             │
│  4. Kotlin 作为新标准                                         │
│     - Maven 需要 XML                                         │
│     - Gradle 可以用 Kotlin DSL                              │
│     - Kotlin 生态正在影响 Java 构建                          │
│                                                             │
│  5. 构建可见性                                                │
│     - Gradle Build Scan（可视化分析）                       │
│     - Maven 也在加强构建报告                                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 9. 选型建议：何时用 Maven，何时用 Gradle

### 9.1 选择 Maven 的场景

```
✅ 适合 Maven 的情况：
─────────────────────
1. 企业级 Java 项目
   - Spring、Hibernate 等成熟框架
   - 大量现有 Maven 项目

2. 需要严格的依赖控制
   - BOM 管理依赖版本
   - 依赖审查流程

3. 团队成员水平参差不齐
   - 声明式配置降低学习曲线
   - 约定优于配置减少决策

4. 生态依赖 Maven Central
   - 大部分库以 Maven 为主

5. CI/CD 流程成熟
   - Maven Wrapper 保证构建一致性
```

### 9.2 选择 Gradle 的场景

```
✅ 适合 Gradle 的情况：
──────────────────────
1. Android 开发
   - Android Studio 官方支持
   - 大量 Android 特有插件

2. 微服务 + 多模块项目
   - 复杂依赖图需要灵活配置
   - 并行构建加速

3. 需要高度定制构建流程
   - 代码生成 + 动态配置
   - 特殊打包需求

4. 追求构建性能
   - 增量构建、构建缓存
   - 并行执行

5. Kotlin 优先团队
   - build.gradle.kts 用 Kotlin 写
   - 与 Kotlin 生态无缝集成
```

### 9.3 一句话总结

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│   Maven = "你按照我的规矩来，我帮你搞定一切"                   │
│           稳定、成熟、约定大于灵活                            │
│                                                             │
│   Gradle = "你来描述怎么构建，我来执行"                        │
│            灵活、高效、学习曲线略高                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 附录：核心概念速查表

| 概念 | Maven | Gradle |
|-----|-------|--------|
| 配置文件 | `pom.xml` | `build.gradle.kts` |
| 依赖声明 | `<dependency>` | `dependencies { }` |
| 插件系统 | `<plugin>` | `plugins { }` |
| 构建阶段 | Lifecycle Phase | Task |
| 任务执行顺序 | 线性 | DAG |
| 增量构建 | 基础 | 完整 |
| 脚本能力 | 有限（插件） | 完整（DSL） |
| 并行构建 | ✗ | ✓ |
| 构建缓存 | 依赖下载缓存 | 完整构建缓存 |
| 学习曲线 | 平缓 | 较陡 |

---

*文档版本：1.0*  
*创建日期：2026-07-07*
