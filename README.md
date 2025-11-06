# MiniDB - 基于LSM Tree的键值数据库实现

MiniDB 是一个轻量级键值数据库实现，提供基本的键值存储功能。
该项目旨在展示嵌入式数据库的核心工作原理，包括内存表、持久化存储、日志系统等关键组件，
主要是个人用于学习用途。

## 核心功能

- 基本的键值操作：支持 `put`、`get`、`delete` 等操作
- 事务支持：通过 WriteBatch 实现批量操作
- 持久化存储：数据会被持久化到磁盘，重启后可恢复
- 预写日志(WAL)：确保数据操作的安全性
- 版本控制：通过 Manifest 管理不同版本的 SSTable 集合
- 分层存储：采用类似 LSM-Tree 的结构，优化读写性能

## 架构设计

MiniDB 的核心架构包括以下组件：

1. **内存表(MemTable)**：作为写入的第一站，提供快速的插入和查询
2. **预写日志(WAL)**：在数据写入内存表前先写入日志，确保崩溃后可恢复
3. **SSTable**：磁盘上的有序键值对集合，用于持久化存储
4. **版本控制(Version/VersionSet)**：管理不同层级的 SSTable
5. **Manifest**：记录版本变更历史，用于系统恢复

## 核心组件详解

### 1. 预写日志(WAL)

WAL (Write-Ahead Logging) 是保证数据安全的关键组件，所有写入操作在更新内存表之前都会先写入 WAL。

- `WAL` 类：负责日志的写入和恢复
- `LogReader`/`LogWriter`：日志的读写工具
- 支持记录分片和校验和验证，确保数据完整性

### 2. 内存表(MemTable)

内存表是数据写入的第一站，采用有序数据结构（如跳表）实现，提供高效的插入、查询和删除操作。

- 当内存表达到预设大小阈值时，会被转换为Immutable MemTable
- Immutable MemTable 会被异步刷新到磁盘，生成SSTable
- 内存表操作在并发环境下通过读写锁保证线程安全

### 3. SSTable

SSTable (Sorted String Table) 是磁盘上的有序键值对集合，是数据持久化的主要形式。

- `SSTable` 类：负责 SSTable 文件的读写
- `BlockBuilder`：构建 SSTable 中的数据块
- `Footer`：存储 SSTable 的元数据索引和数据索引
- 采用块级压缩和缓存机制提升读取性能
- 每个 SSTable 包含有序的键值对，支持二分查找

### 4. 版本控制

版本控制系统管理不同层级的 SSTable，负责数据的压缩和合并。

- `Version`：表示数据库的一个版本，包含各层级的 SSTable
- `VersionEdit`：记录版本之间的变更
- `Manifest`：持久化存储版本变更历史，用于系统恢复
- `Compaction`：合并不同层级的 SSTable，减少数据冗余，提升查询效率

### 5. 事务支持

通过 `WriteBatch` 实现批量操作的原子性，确保一组操作要么全部成功，要么全部失败。

- 支持批量 `put` 和 `delete` 操作
- 操作序列会被序列化后写入日志，保证崩溃可恢复
- 提供事务边界，确保操作的原子性

## 快速开始

### 环境要求

- JDK 17 及以上版本
- Maven 3.6+（用于构建项目）

### 构建项目

```bash
# 克隆仓库
git clone https://github.com/yourusername/minidb.git
cd minidb

# 编译项目
mvn clean package
```

### 基本使用示例

```java
// 打开数据库
Options options = new Options().setCreateIfMissing(true);
MiniDB db = MiniDB.open(options, "/path/to/db");

try {
    // 写入数据
    db.put("name".getBytes(), "Alice".getBytes());
    db.put("age".getBytes(), "30".getBytes());
    
    // 读取数据
    byte[] value = db.get("name".getBytes());
    System.out.println("Name: " + new String(value));
    
    // 批量操作
    try (WriteBatch batch = db.createWriteBatch()) {
        batch.put("email".getBytes(), "alice@example.com".getBytes());
        batch.delete("age".getBytes());
        db.write(batch);
    }
    
    // 删除数据
    db.delete("name".getBytes());
} finally {
    // 关闭数据库
    db.close();
}
```

## 注意事项

- 本项目主要用于学习和演示目的，不建议直接用于生产环境
- 性能优化和错误处理仍有提升空间
