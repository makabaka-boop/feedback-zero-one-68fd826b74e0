# minidw —— 从零实现的迷你数仓（Python + SQLite）

只用 Python 标准库实现（`sqlite3` + `http.server`），不依赖任何 ORM 或迁移框架。
每张用户表都是**真实的 SQLite 表**，带 `PRIMARY KEY` / `FOREIGN KEY` 声明并启用外键检查；
行数据存放在物理表中，绝不用 JSON 模拟。模型演进（改名、增删列、增删表）由本项目的
规划器手工 diff 并在一个事务内完成重建。

## 文件结构

```
server.py            HTTP 接口层与入口（python3 server.py --db warehouse.db --port 8000）
warehouse.py         核心引擎：模型校验、迁移规划与执行、查询
tests/acceptance.py  验收测试（黑盒，通过 HTTP 驱动真实服务器进程）
```

运行服务：

```bash
python3 server.py --db warehouse.db --host 127.0.0.1 --port 8000
```

运行验收测试（130 项断言，覆盖互换表列名、循环引用、回滚、崩溃恢复等）：

```bash
python3 tests/acceptance.py
```

## 模型规则

模型是一个表数组。每个表、每个列都有：

- **id**：非空 ASCII 字符串，**全局唯一**（表 id 与所有列 id 一起判重）；
- **名称**：匹配 `[a-z][a-z_]*`，禁止 `sys_`、`sqlite_` 前缀；表名在表间唯一，
  列名在同表内唯一；物理表、物理列与模型同名（SQL 标识符一律加引号，关键字可作名）；
- 每表**恰好一个主键列**：非空整数（`INTEGER PRIMARY KEY NOT NULL`）；
- 其余列只能是**可空整数**或**可空字符串**；整数为有符号 64 位，拒绝布尔
  （JSON `true/false` 在整数列一律报错）；
- 整数列可用 `"ref": "<表id>"` 引用该表主键，**允许自引用与循环外键**；
  `null` 不检查引用，非 `null` 必须存在目标主键。

模型文档示例：

```json
{
  "tables": [
    {"id": "t1", "name": "a", "columns": [
      {"id": "c1", "name": "x", "type": "integer", "primary_key": true},
      {"id": "c2", "name": "f", "type": "integer", "ref": "t2"},
      {"id": "c3", "name": "v", "type": "string"}]},
    {"id": "t2", "name": "b", "columns": [
      {"id": "c4", "name": "y", "type": "integer", "primary_key": true},
      {"id": "c5", "name": "g", "type": "integer", "ref": "t1"}]}
  ]
}
```

列对象字段：`id`、`name`、`type`（`"integer"`/`"string"`）、`primary_key`（可选布尔，
默认 `false`）、`ref`（可选，默认 `null`）。未知字段一律拒绝。

## HTTP 接口

所有请求/响应均为 JSON。成功响应含 `"ok": true`；拒绝响应含
`{"ok": false, "error": {"code": ..., "message": ...}}`。

| 状态码 | 含义 |
|---|---|
| 400 | `bad_request` / `validation` / `fk_violation` / `constraint_violation` |
| 404 | `not_found`（未知端点或表名） |
| 405 | `method_not_allowed` |
| 409 | `not_initialized` / `already_initialized` / `version_conflict` / `migration_conflict` |

### POST /init —— 初始化装载（只允许一次）

```json
{
  "model": { "tables": [ ... ] },
  "data":  { "t1": [[1, 10, "hello"], [2, 20, "world"]], "t2": [[10, 1]] }
}
```

- `data` 以**表 id** 为键；每行按**当前列声明顺序**给出；缺省表视为空表。
- 主键不可为 `null`；整数校验 64 位范围并拒绝布尔；外键在提交时统一检查
  （循环引用可正常装载），违反则整次拒绝。
- 成功返回 `{"ok": true, "version": 1}`；此后再次调用返回 `409 already_initialized`。
  失败的初始化不占用初始化名额，修正后可重发。

### POST /migrate —— 模型迁移

```json
{
  "migration_id": "m-0001",
  "version": 1,
  "model": { "tables": [ ...完整的新版本模型... ] },
  "defaults": { "t1": {"c9": 42}, "t2": {"ca": null} }
}
```

- `migration_id`：非空 ASCII；`version`：本迁移基于的当前版本（版本从 1 开始）。
- `model` 是**完整的新版本模型**，不是增量。
- `defaults`：以表 id → 列 id 索引，为**保留表上的新增列**声明常量默认值
  （同类型常量或 `null`），逐保留行填充；缺声明、类型不符、给保留列提供默认值、
  指向不存在的表/列都会被拒绝。新表初始为空，其新列的默认值可省略（提供则忽略）。
- 成功返回 `{"ok": true, "version": <旧版本+1>, "migration_id": ..., "replayed": false}`。

### GET /model —— 查询当前模型

返回 `{"ok": true, "version": N, "model": {...}}`，列对象被规范化为
`{id, name, type, primary_key, ref}` 五字段。

### GET /data/<表名> —— 查询数据

返回 `{"ok": true, "table": ..., "columns": [...按声明顺序...], "rows": [[...], ...]}`，
行按**主键升序**、值按当前列声明顺序排列。表名不存在返回 404。

### GET /migrations —— 迁移记录

返回已成功迁移的重试记录（`migration_id`、`from_version`、`to_version`、`applied_at`）。

### GET /health —— 存活探针

## 迁移语义

- **身份由 id 承载**：同 id 的表/列保持身份（可改名）；同名但新 id 是新实体，
  不继承旧数据。id 的种类不可变（表 id 不能变成列 id，反之亦然）。
- **保留列不得改类型**，值逐值保留；**主键列 id 不可更换**；**列不得跨表移动**。
- **删列**丢弃其值；**删表**丢弃其行；**新表**初始为空。
- 迁移后全库必须满足主键与全部外键：删表、默认值或重建导致悬空引用时
  **整次拒绝**（先校验模型，再对重建结果执行 `PRAGMA foreign_key_check`）。
- 列声明顺序即行的输入/输出顺序；列重排、改名、增删都会触发表重建。

## 版本与幂等

- 初始化后版本为 1，每次**成功**迁移版本加一。
- **只有成功的迁移占用 `migration_id`**；失败的迁移不写记录，同一 id 修正后可重用。
- id 已被占用时，按**请求的 JSON 结构**（键序无关的规范化形式）判定：
  内容相同 → 幂等重放，返回 `replayed: true` 与当时产生的版本号，不改变任何状态；
  内容不同 → `409 migration_conflict`。
- id 未占用时，版本不等于当前版本（过旧或超前）→ `409 version_conflict`。

## 原子性、并发与崩溃恢复

- 每次初始化/迁移都在**单个 SQLite 事务**内完成：模型元数据、用户表、行、
  重试记录一起提交；任何拒绝都整体回滚，不改变已持久化状态。
- 数据库使用 WAL 日志与 `synchronous=FULL`；所有请求经全局锁串行化，
  多请求等价于某个串行执行；读请求只见完整的旧版本或新版本。
- 表重建遵循 SQLite 官方模式变更流程：事务外先 `PRAGMA foreign_keys=OFF`，
  事务内以临时名建新表、逐值拷贝、删旧表、改回正式名，
  `PRAGMA foreign_key_check` 校验通过后提交，事务外恢复 `foreign_keys=ON`。
  除该流程外，所有常规操作始终在外键检查开启下执行。
- 崩溃恢复（验收测试以 `DW_CRASH_POINT` 环境变量模拟断电，仅限测试）：
  - 提交前强退 → 重启后保留**旧版本**（未提交事务被回滚）；
  - 提交后、响应前强退 → 重启后保留**新版本**，客户端重发同一请求即幂等重放。

## 题目示例

```bash
# 表 t 名 a、主键 k 名 x、行 [[1]]
curl -X POST localhost:8000/init -d '{
  "model": {"tables": [{"id": "t", "name": "a", "columns": [
    {"id": "k", "name": "x", "type": "integer", "primary_key": true}]}]},
  "data": {"t": [[1]]}}'
# => {"ok": true, "version": 1}

# 改名 b、y（id 不变）
curl -X POST localhost:8000/migrate -d '{
  "migration_id": "m1", "version": 1,
  "model": {"tables": [{"id": "t", "name": "b", "columns": [
    {"id": "k", "name": "y", "type": "integer", "primary_key": true}]}]}}'
# => {"ok": true, "version": 2, "migration_id": "m1", "replayed": false}

# 查 b 得 [[1]]
curl localhost:8000/data/b
# => {"ok": true, "table": "b", "columns": ["y"], "rows": [[1]]}
```

物理表随之重建为 `CREATE TABLE "b" ("y" INTEGER PRIMARY KEY NOT NULL)`。
