# 两端快照对账协议（Python + SQLite）

两个机房各持有一份**不可变**的同源快照。两端进程只通过公开消息交换：紧凑摘要、必要的明细行、
以及最终的内容承诺。目标是**只传输不一致的行**，而不是每次把整张表上传比较。

本仓库包含：

* 可运行的协议实现（`recon/`）；
* **关系数据集对账**：多张互相关联的表组成的数据集组对账 + 本地镜像原子发布
  （`recon/dataset.py`、`recon/grouprecon.py`、`recon/mirror.py`，见第 6 节）；
* **按显式 schema 迁移目录对账**：两端 schema 版本不同时，先把各自数据转换到指定
  目标版本，再复用既有组对账与镜像发布（`recon/migrate.py`，见第 7 节）；
* 两端**各自独立的操作系统进程**演练（`recon/twoprocess.py`、`recon/worker.py`）；
* 独立测试套件（`tests/`，100 个测试，纯标准库运行，无需 pytest）；
* 独立于协议实现的逐行比对 oracle（`recon/oracle.py`）。

---

## 1. 快速开始

```bash
# 全部测试（约 2.5 分钟）
python3 tests/run_tests.py

# 10 万行、3 条差异：两个真实进程（默认 --mode processes）
python3 -m recon.cli --workdir /tmp/recon-demo --fresh demo --rows 100000 --profile sparse

# 进程内演练（同样的引擎，便于快速观察）
python3 -m recon.cli --workdir /tmp/recon-demo --fresh demo --rows 100000 --mode threads

# 关系数据集对账：两端各 4 张关联表，对账 + 镜像原子发布
python3 -m recon.cli --workdir /tmp/recon-group --fresh group --customers 2000 --orders 8000

# schema 迁移对账：左端 v1、右端 v2，按目录迁移到 v3 后对账并发布镜像
python3 -m recon.cli --workdir /tmp/recon-migrate --fresh migrate --customers 2000 --orders 8000

# 复现 README 中的全部基准（含密集差异 + 极小摘要预算）
python3 -m recon.cli --workdir /tmp/recon-bench --fresh benchmark
```

`benchmark` 子命令对同一 `seed` 的数据重建快照；其输出（`report.json`）含每个场景的
wire 字节、两侧分方向字节、帧数、回退 region 列表与耗时。

> 下列数字对固定输入是确定性的：在相同 Python/SQLite 版本下重复运行，
> 字节数与差异集合逐字节一致（只有耗时随机器波动）。

每个 demo 目录会留下：两份只读快照 `left.sqlite` / `right.sqlite`、会话结果 JSON、
以及 responder 的应答缓存。进程模式下另有两进程字节统计（`two_process_run.json`）。

---

## 2. 固定快照与规范编码

### 2.1 快照

* 每端有且只有一张快照，由 `create_snapshot()` 一次性写入，随后文件权限设为只读（`0444`），
  协议进程始终以 SQLite `mode=ro` 打开。应答缓存写在**另一个** sidecar 文件里，
  因此快照在文件系统层面也不可被协议写坏。
* 快照元数据表记录：schema 线格式、承诺参数、行数、首尾 row_id、内容承诺。
  每次打开快照都会**从存储的规范字节重新计算承诺**；不一致则拒绝服务（`SnapshotError`）。
* 行以**规范字节**（BLOB）存储，不存 SQL 动态类型投影，避免 `0` / `'0'` / `FALSE` / `NULL`
  在 SQLite 里看起来相同。

### 2.2 schema 声明

按顺序声明字段，类型为 `int64` / `text` / `bool`，可空性显式声明：

```python
Schema.declare(
    ("payload", "text",  False),
    ("amount",  "int64", True),
    ("active",  "bool",  False),
    ("note",    "text",  True),
)
```

字段名、类型、顺序、可空性都属于 schema；两侧 schema 必须逐项一致，否则握手上报
`schema_mismatch`，不会静默采用任何默认值。

### 2.3 规范行编码

每个字段编码为 `<类型前缀><负载>`：

| 类型 | 编码 |
|---|---|
| `NULL` | `N`，无负载 |
| `text` | `T` + `u32 大端字节长度` + 原始 UTF-8 字节 |
| `bool` | `B` + `0x00`/`0x01` |
| `int64` | `I` + 8 字节大端二进制补码 |

由此得到题目要求的全部性质：

* `NULL`、空串、`0`、`FALSE` 两两不同，且**bool 永不等价于整数**（Python 侧拒绝把 `int`
  写进 bool 列，反之亦然）。
* 文本原样保留，不做 Unicode 规范化；`"é"`（预组合）与 `"é"`（分解）不同，
  内嵌 NUL、换行、超长文本（测试里用 320 KB 单元格）都按字节保留。
* 每个变长字段自带长度前缀，字段边界无歧义：`("ab","c")` 与 `("a","bc")` 编码不同。
* 行身份是独立的 `row_id`。**相同业务内容但 `row_id` 不同是两行**。
* 同一 `row_id` 的内容前后不同 => 一对“前像/后像”，协议按 `changed` 交付，不会误记成
  一次删除加一次新增。

### 2.4 内容承诺

`compute_commitment()` 对以下元素做域分离的 SHA-256：

```
域标签, schema 线格式, 排序后的承诺参数, 总行数, 排序后首行 row_id, 末行 row_id,
然后按 row_id 排序的每一行规范字节（均带长度前缀）
```

因此承诺绑定了 **schema、编码参数、总行数、排序边界和全部规范内容**。
排序基于 row_id 的规范字节，是确定性全序。

---

## 3. 协议

### 3.1 消息封装

所有消息是「4 字节大端长度 + 规范 JSON」。信封固定包含：

```
v, kind, msg_id, session, from, to,
snapshot: {id, schema_sha, commitment, rows, first, last},   # 发送方快照身份
peer:     {同上},                                             # 接收方快照身份（发送方已知）
negotiated: {schema, commit, summary},
request_id, request_msg_id（应答）
```

接收方在**做任何工作之前**校验：`to` 是自己、`session` 是当前会话、`snapshot` 与本会话
钉住的对方快照完全一致、`peer` 是自己的快照、`negotiated` 与会话协商结果一致。
过期响应、跨快照响应、发错端点的帧都会得到**显式 `error` 帧**（不含任何数据），
而不是被静默丢弃或误用。

### 3.2 握手

1. 发起方发送 `hello`：自己的快照身份、完整 schema、协商的摘要预算、承诺参数。
2. 应答方校验 schema 与承诺参数完全一致，采用发起方**配置**的摘要预算（绝不按数据或差异
   位置调整），返回 `hello_ack`：自己的快照身份、schema、协商结果。
3. 双方各自钉住对方快照身份，之后每条消息都携带它。

### 3.3 分层键摘要

行按 `row_id` 的域分离哈希放进 `position`（默认取 SHA-256 前 64 bit，可配置更窄以制造碰撞）。
摘要节点是 `(gen, tag)`：`gen` 固定 position 的高位前缀，根节点 `gen=0`；每个节点恰好
分裂成 `tag<<1` 与 `(tag<<1)|1`。**位置只由 row_id 决定**，不依赖字段值、不依赖对端视图、
更不依赖测试数据里差异的位置。

节点摘要（约 100 字节）：

* 行数；
* 所有 `position || 32 字节行摘要` 指纹的 XOR 与模和；
* 上述累加量的域分离 SHA-256；
* 该区域（见下）的 row_id 中位数。

`position` 还存在可选的 row_id 区间约束 `[lo..hi]`，用于摘要预算耗尽时的**键空间二分**。
节点 + 区间合称 region，双方对同一 region 的解释完全一致。

**cord**（可解码形式）：`<u64 行数><(u64 position, 32B 行摘要)>*`，按 position 排序。
比较两条 cord 得到**精确的候选 position 集合**。cord 只是差异探测器，从不当作内容使用。

摘要与 cord 都附有域分离的 **node proof**，绑定 `kind`、generation/tag/位宽、区间端点及
负载；证明不符即视为无效摘要。

### 3.4 定位流程

1. 取根节点两侧摘要。相等 => 视为（SHA-256 假设下）无差异，但**仍要**做第 3.6 步承诺核验。
2. 不等则按批向对端请求摘要，对每个 region：
   * 摘要相等：跳过（审计日志记 `summary_equal`）；
   * 差异且规模大：先按哈希前缀分裂（`hash_split`）；
   * 哈希层数用尽：用对端摘要里给出的 row_id 中位数做**键区间二分**（`key_split`）；
   * 规模小到可解：批量请求 cord（同一轮所有叶子合并成一个请求）；
   * cord 超预算、无法解码或 proof 不符：显式记录原因，然后继续分裂；确实无法再分裂时，
     对**该 region** 做精确回退（`fallback_exact`，记录 region 标签、页数、行数）。
3. 由 cord 得到候选 position 后，发起方分页请求**对端真实规范行**（`probe`），
   按 `row_id` + 规范字节逐条比对。摘要单元算出的 ID **绝不**直接用于改数据。
4. 所有已确认差异进入差异集合；同一 `row_id` 的差异只可能落在一个 region 中，天然去重。

关键性质：

* **稀疏差异时节省传输**：只交换摘要和少量明细（见第 5 节实测）。
* **差异密集 / 摘要不利时仍然正确**：摘要被拒、过小或损坏都会触发分裂或具名局部回退，
  不会把“解不出来”当作相等，也不会静默丢剩余项。局部回退允许传该局部全部行，
  但**只针对该 region**，并写入审计日志；绝不会因为一处坏摘要就传整表。

### 3.5 应用与承诺核验

发起方在本地临时结构（内存 map + 真实临时 SQLite 库，可关闭）中应用差异：
保留自己的行、去掉 only_left 与自己一侧的 changed、写入对端的 only_right 与 changed 后像；
随后**从存储字节重新解码并重新计算承诺**，要求：

```
applied_commit == peer 快照承诺
```

若不等，则做一次**穷尽重扫**（忽略“摘要相等即跳过”的乐观剪枝），再次核验。
仍然不等则返回 `complete=false`，原因是显式协议错误，绝不谎报成功。

### 3.6 空表、全同、重复与乱序

* 空表 / 完全相同：也要真正走完握手 + 摘要 + 承诺核验（测试断言了帧数和字节数）。
* 重复请求：应答缓存在应答方**自己的** SQLite 缓存里，键 = `session + request_id +
  request_msg_id`；重复投递返回**字节完全相同**的应答，不重复计算、不重复累加差异。
* 乱序 / 过期响应：发起方只接受 `request_id` 与 `request_msg_id` 都对得上的应答；
  其他帧按身份丢弃并计数（`ignored_frames` / `duplicate_answers_ignored`），
  绝不按到达顺序归属。

---

## 4. 信任假设（重要）

* 本协议**不做节点认证、不做密码学签名**。对端可能在任意时刻说谎。
* 正确性依赖 **SHA-256 抗碰撞假设**：摘要相等、承诺相等都被当作内容相等的证据。
  这不等于“散列绝无碰撞”；在抗碰撞假设被打破时协议结论不再成立。
  所有哈希用途都有域分离（`RECON:COMMIT`、`RECON:ITEM`、`RECON:NODE-PROOF`、
  `RECON:FINGERPRINT` 等），但域分离不提供抗碰撞之外的任何保证。
* 对端在明细行上撒谎会被**最终承诺核验**发现（测试
  `test_corrupted_detail_cannot_produce_false_success`），表现为 `incomplete` +
  协议错误，而不是错误成功。
* 快照只读、承诺在每次打开时重算，用于防止**本地存储损坏**，不用于防御能改写快照文件的
  攻击者。

---

## 5. 实测数据

环境：Linux / Python 3.11.2 / SQLite 3.40.1。
“wire”是两端实际发出的帧字节总和（4 字节长度前缀 + JSON，含全部消息开销）；
“canonical”是一侧全部规范行的原始字节数（不含任何封装）。
复现命令：`python3 -m recon.cli --workdir /tmp/recon-bench --fresh benchmark`。

### 5.1 10 万行，仅 3 条差异（1 改、1 删、1 增）

| 指标 | 进程内 | 两个真实进程 |
|---|---:|---:|
| 行数（左/右） | 100 000 / 100 000 | 100 000 / 100 000 |
| 差异（only_left / only_right / changed） | 1 / 1 / 1 | 1 / 1 / 1 |
| **wire 总字节** | **86 841** | **86 841** |
| 发起方发出 | 27 170 | 27 170 |
| 应答方发出 | 59 671 | 59 671 |
| 帧数 | 36 | 36 |
| 左侧全表 canonical 字节 | 9 582 927 | 9 582 927 |
| wire / 全表 canonical | **0.91 %** | **0.91 %** |
| 摘要请求 / 覆盖 region | 14 / 65 | 14 / 65 |
| cord 请求 / 字节 | 3 / 5 504 | 3 / 5 504 |
| probe 请求 / 实际传输行数 | 3 / 2 | 3 / 2 |
| 回退 | 0 | 0 |
| 最终承诺与对端快照承诺一致 | 是 | 是（`rc=[0,0]`） |

参与协商的 `msg` 类型字节分布（发起方）：`hello` 1 335、`summaries` 21 948、`probe` 3 887；
应答方：`hello_ack` 1 409、`summaries_ack` 54 101、`probe_ack` 4 161。
**两端差集在表中逐条核对**：1 条改值（同 row_id 前后像）、1 条删除、1 条新增，
与独立逐行 oracle 完全一致。

### 5.2 差异密集 + 故意极小的摘要预算

20 000 行（右侧 19 306 行），4 939 条改、2 094 条删、1 400 条增（`variant_dense(seed=7)`）；
`position_bits=10, max_gen=10, leaf_limit=8, cord_max_bytes=64`（小于一条 40 字节 cord 项），
`rows_per_chunk=512, max_nodes_per_query=64`。

| 指标 | 进程内 | 两个真实进程 |
|---|---:|---:|
| 完成（complete） | 是 | 是（`rc=[0,0]`） |
| 差异（only_left / only_right / changed） | 2 094 / 1 400 / 4 939 | 2 094 / 1 400 / 4 939 |
| **具名局部回退 region 数** | 1 806 | 1 806 |
| 回退实际传输行数 | 1 806 | 1 806 |
| probe 实际传输行数 | 5 849 | 5 849 |
| 摘要请求 / cord 请求 | 583 / 5 849 | 583 / 5 849 |
| wire 总字节 | 43 783 300 | 43 783 300 |
| 最终承诺与对端快照承诺一致 | 是 | 是 |

摘要预算过小导致大量叶子不可用 cord，于是协议按 region 分裂、具名回退，
结果仍然完整；回退日志逐条给出 region 标签。差异集合与独立 oracle 完全一致，
应用后的承诺等于对端快照在磁盘上的承诺。

> 说明：密集场景下 wire 体积**大于**一张表的 canonical 字节，这是差异密集时的固有代价
> ——必须传输大量明细行。协议没有为了数字好看而少传或漏传。

### 5.3 完全相同 / 空表

| 场景 | wire 字节 | 传输行数 | 结果 |
|---|---:|---:|---|
| 5 000 行完全相同 | 5 619 | 0 | `complete`，差异 0/0/0 |
| 空表 vs 空表 | 5 152 | 0 | `complete`，差异 0/0/0 |

两者都完整走完握手、摘要比较与双方承诺核验。

---

## 6. 关系数据集对账（多表组）

实际交接的数据常由多张互相关联的表组成。`recon/dataset.py`、`recon/grouprecon.py`、
`recon/mirror.py` 在单表协议之上提供**关系数据集对账**：两端各自提交一组表快照和关系
清单，工具复用现有的紧凑摘要、明细核验和局部回退逐表定位差异，并把对端状态构建为本地
镜像的新代次。已有单表命令（`demo` / `benchmark`）不受影响。

### 6.1 数据集组

一个数据集组是一个目录：

```
group.json              # format、数据集标识、采集批次标识、关系清单、表名列表
tables/<表名>.sqlite     # 每张表一个不可变快照（现有单表格式）
```

* 每张表文件内部带 `dataset_meta`（数据集标识 + 批次标识 + 表名）；该标签只是元数据，
  不进入表级内容承诺。打开组时逐表重算承诺，并要求组内所有表文件的批次标识一致——
  **不同批次拼不成一个有效组**。
* 记录身份 = 表名 + row_id；row_id 只在表内唯一。每张表的列类型、空值规则、规范编码、
  内容承诺与单表协议完全相同。
* 关系清单（带显式版本号）声明外键 `子表.列 -> 父表.row_id`：引用列必须是 text
  （row_id 的类型），可空外键允许 NULL；清单的内容哈希进入组身份。
* **组身份**绑定：数据集标识、批次标识、清单哈希、每个表名及其 schema 与内容承诺。
* 两端兼容性：数据集标识、表名集合、各表 schema、关系清单必须一致；缺表、类型变化、
  清单不同、数据集不同都是显式错误（`GroupCompatibilityError`），绝不静默拼合。
  两端**批次可以不同**（旧批次对新批次正是常态），两个批次身份都记录在结果里。

### 6.2 对账与整体结果

`reconcile_group(left_dir, right_dir)` 对每张表运行一次现有单表会话（同一引擎、同样的
摘要/明细/回退能力），逐表产出 `only_left` / `only_right` / `changed` 明细（规范字节、
按 row_id 排序），并给出绑定两端完整数据集身份的组级结果标识 `result_id`（由两端组身份、
各表承诺与逐表差异计数计算）。任何一张表未完成，整体结果即为未完成。

### 6.3 镜像与原子发布

`reconcile_and_publish(left_dir, right_dir, mirror_dir)` 把对端状态发布到本地镜像：

```
mirror/
  CURRENT                 # 当前代次指针（原子 rename 切换）
  generations/000001/     # 一个完整数据集组 + publish.json（发布清单）
  generations/000002/ ...
```

1. 在隔离的暂存目录中构建整组新代次（每张表 = 本地内容应用对端差异后的重建结果）。
2. 逐表校验：重建内容承诺 == 对端对应表承诺；再从暂存文件重新打开、重算承诺，
   要求整组身份 == 对端组身份。
3. 对**最终整组状态**做外键检查：同批新增的父行、同批删除的父子行、改指向另一父行的
   子行、可空关系形成的循环引用都按最终状态判定；中途暂时缺少目标不会误拒，最终状态里
   的悬空引用也不会被跳过。
4. 全部通过才把代次目录就位并原子切换 `CURRENT`；任何失败（表未完成、外键无目标、
   身份或承诺不符）都会清理暂存目录，旧代次仍是当前代次且内容不变，读者照常读取。
5. 发布清单 `publish.json` 记录：批次标识、各表承诺、关系清单版本与哈希、发布代次、
   上一代次、来源组身份与本次 `result_id`——可复核本次到底发布了哪组数据。

原始输入快照始终保持只读（文件权限 `0444`，协议进程以 `mode=ro` 打开；应答缓存写在工作
目录的 sidecar 文件中）。

### 6.4 运行与验证

```bash
python3 -m recon.cli --workdir /tmp/recon-group --fresh group --customers 2000 --orders 8000
```

输出（同时写入 `group_report.json`）：每表输入/输出行数、逐表差异（与独立逐行 oracle
对照）、每表与总计传输字节、全组 canonical 字节对照、组级结果身份、发布前后镜像代次、
发布清单。演示数据在同一批次里同时包含：新增父行+引用它的新增子行、同批删除父子行、
改值、子行改指向另一父行、可空循环引用变化。实测（2000 客户 / 8000 订单 / 128 对循环
节点，10 条差异）：传输 218 808 字节，约为右端全组 canonical 字节（619 939）的 35%，
镜像从代次 1 原子发布到代次 2。

---

## 7. 按显式 schema 迁移目录对账（跨版本）

两个站点升级节奏不同：一端仍产出旧字段，另一端已改名、增列或调整枚举值。第 6 节的
组对账要求两边表结构完全兼容；`recon/migrate.py` 在其上增加**按显式迁移目录对账**：
两端各自声明数据集 schema 版本，工具按目录把两端数据转换到用户指定的目标版本，
再复用既有紧凑摘要对账与镜像原子发布。

### 7.1 版本声明与迁移目录

* 数据集组的 `group.json` 可声明 `schema_version`（元数据标签，不进入组身份；
  目录会按实际表结构校验该标签，谎报即计划错误）。
* **迁移目录**（JSON，`recon-migration-catalog-v1`）声明：
  * `versions`：每个版本的全部表 schema（字段名/类型/可空性）及该版本生效的
    **关系清单**；
  * `steps`：有向版本步骤 `from -> to`，每步为数据集的**每张表**声明字段规则——
    目标字段的来源字段（保留/改名）、新增字段的**显式常量默认值**、文本枚举的
    **显式一对一改写**、`int64` 与十进制文本之间的**严格转换**。不再进入目标
    schema 的源字段即被删除。每一步的目标 schema 与生效清单即其目标版本的声明。
* 目录身份 `catalog_id` 是全部声明（版本、规则、清单）规范化后的内容哈希；
  同一目录内容必得同一身份。

### 7.2 计划：先验证，后转换

`plan_migrations()` 在**读取并转换任何数据之前**完成全部结构校验，出错即抛出
`MigrationPlanError`（带稳定 `code` 与可定位 `context`：版本/步骤/表/字段）：

* 目录必须**无环**（`catalog_cycle`），步骤端点必须是已声明版本，同一 `(from,to)`
  不得重复，每步必须**完整覆盖**数据集全部表（`table_coverage`）；
  每个版本的清单必须能映射到该版本的表结构（`manifest_unmappable`）。
* 字段映射必须完整且无歧义：目标字段重复（`field_mapping_duplicate`）、缺失
  （`field_mapping_missing`）、映射到目标 schema 之外的字段（`field_mapping_unknown`）、
  来源字段不存在（`source_field_unknown`）、来源与默认值同时出现
  （`source_and_default`）、对已有字段给常量默认（`default_on_existing_field`）。
  类型变化必须显式声明转换（`type_change_without_conversion`），转换的端点类型必须
  匹配（`conversion_type_mismatch`），枚举只能用于文本且映射必须一对一
  （`enum_on_non_text` / `enum_not_one_to_one`），默认值必须符合目标类型与可空性
  （`invalid_default`）。
* 输入组的版本标签必须存在于目录（`unknown_source_version` /
  `unknown_target_version` / `schema_version_missing`），组的实际表结构与清单必须
  与目录对该版本的声明一致（`version_schema_mismatch` /
  `version_manifest_mismatch`），数据集标识必须一致（`dataset_mismatch`）。
* 从各自版本到目标版本的路径必须**存在且唯一**：没有路径（`no_path`）或有多条
  （`ambiguous_path`）都是计划错误，工具**绝不自行挑一条路线继续**。一端已在目标
  版本时路径为空（零步），属合法。

### 7.3 转换：确定、严格、可定位

* 只覆盖实际迁移常见操作：保留/改名、带显式常量默认值的新增、删除、文本枚举一对一
  改写、`int64` <-> 十进制文本严格转换（可选 `-` 号 + 数字；越界即错；`int64` 到文本
  为规范十进制）。
* 转换确定且保留 `row_id`；**禁止**舍入、截断、隐式布尔/整数互转、静默吞掉 null
  （null 仅在目标可空时原样保留并计数）、把非法值改成默认值。每一步都按声明的输入
  类型（解码）与输出类型（重编码）验证。
* 对越界 int64（`int64_out_of_range`）、格式不符的十进制文本（`bad_decimal_format`）、
  未列出的枚举值（`enum_unmapped`）、null 违反非空约束（`null_violation`），
  抛出 `MigrationConversionError`，逐项报告**版本步骤、表、row_id、字段、错误类别**
  （同一步骤内跨表收集），绝不输出看似成功的差异结果。
* 原始快照始终保持只读；迁移结果写入**隔离的临时数据集组**，批次标签由来源批次、
  步骤目标版本与目录身份确定性派生。同一来源版本、目标版本与目录内容必得到相同的
  **转换身份**（`transform_id`，绑定目录身份与版本路径），迁移结果组身份也随之确定。
* 逐步迁移完成后，两端必须具有相同的目标表集合、schema 与关系清单（显式兼容性
  检查），才进入第 6 节的既有组对账；外键列改名或转换后，按**目标关系定义**对最终
  整组状态做外键检查（在镜像发布的暂存终态上，与既有行为一致），而不是在逐表转换的
  中间状态检查。
* 镜像仍须等全部表对账、内容承诺与外键检查通过后才整体发布。计划错误、任一行转换
  失败、对账未完成或最终关系无效时，不发布新代次，旧镜像保持可读且不变；临时数据
  清理后可修正目录重试。

### 7.4 转换报告

`MigrationRunResult` 绑定：两端**原始组身份**与批次、目标版本、**目录身份**、
实际**版本路径**与每端**转换身份**；按版本步骤和表列出输入/输出行数与逐字段转换
计数（copied/converted/mapped/defaulted/nulls）及失败位置；完成时关联既有组级对账
结果（`result_id`、逐表差异、传输字节）与镜像代次。测试不依赖产品自报计数：
独立参考逻辑从原始输入计算目标行并独立扫描外键，逐行比对转换结果与最终镜像。

### 7.5 运行

```bash
python3 -m recon.cli --workdir /tmp/recon-migrate --fresh migrate --customers 2000 --orders 8000
```

演示目录含三个版本：v1（`tier` 为文本、`amount_text`、`state` 枚举码、`legacy_note`）、
v2（`tier` 转 int64、`amount_text` 改名并转为 `amount_cents`、删除 `legacy_note`、
新增常量默认列 `channel`）、v3（`name` 改名 `full_name`、`tier` 转回文本 `tier_code`、
新增默认列 `segment`、外键列 `customer_id` 改名 `cust_id` 且清单跟随、`state` 枚举
改写为 `status`）。左端为 v1（路径 v1→v2→v3），右端为 v2（路径 v2→v3）。输出
（`migration_report.json`）含两端路径与转换身份、逐步逐表统计、逐表差异（与独立
oracle 对照）、组级结果身份与发布代次。实测（400 客户 / 1200 订单）：迁移后逐表
差异与独立参考完全一致，镜像从代次 1 原子发布到代次 2。

---

## 8. 代码结构

```
recon/
  encoding.py      schema 声明、规范编码/解码、内容承诺、域分离哈希
  store.py         不可变只读 SQLite 快照、打开时重算承诺、应答缓存 sidecar、数据集标签
  summary.py       位置/节点/区间、聚合摘要、cord、proof、本地索引、协商参数
  wire.py          帧编解码、信封校验、会话上下文、请求缓存键、协商逻辑
  initiator.py     发起方：握手、分层定位、probe/fallback、应用与承诺核验
  responder.py     应答方：握手、摘要/cord、分页 probe/fallback、重放缓存
  runner.py        进程内（线程）会话驱动 + 确定性重放会话
  worker.py        两端独立进程的入口（stdio 控制通道）
  twoprocess.py    双进程演练：纯字节中继 + 重复投递故障注入 + 精确字节统计
  oracle.py        独立逐行比对，生成期望差集
  data.py          确定性数据生成（与协议无关）
  dataset.py       数据集组：关系清单、组身份、兼容性检查、最终状态外键扫描
  grouprecon.py    组对账引擎：逐表会话、组级结果身份、重建与发布编排
  mirror.py        代次镜像：隔离暂存、逐表承诺核验、外键终态检查、原子发布
  groupdata.py     确定性演示数据组生成（与协议无关）
  migrate.py       迁移目录、计划器（唯一无环路径）、严格转换执行、迁移编排与报告
  migratedata.py   确定性跨版本演示数据与目录生成（与协议无关）
  cli.py           demo / benchmark / group / migrate 命令行
tests/
  run_tests.py     无第三方依赖的测试运行器
  support.py       独立应用差集、承诺计算等测试辅助
  test_encoding.py     编码/schema/快照/承诺（15）
  test_protocol.py     端到端对账与逐条核对（12）
  test_adversarial.py  坏摘要、证明、错身份、说谎明细、重放（10）
  test_twoprocess.py   真实双进程、隔离性、10 万行（5）
  test_scale.py        10 万行确切字节 + 全表应用核验、密集回退（3）
  test_ordering.py     乱序/重复应答归属、信封完整性（2）
  test_collisions.py   桶拥挤与真实哈希碰撞（3）
  test_group.py        数据集组对账、外键终态、发布失败与旧镜像完整性（16）
  test_migration.py    迁移目录/计划错误/严格转换/跨版本对账发布与重试（34）
```

合计 **100 个测试**，全部通过（`python3 tests/run_tests.py`）。

## 9. 安全与工程边界（明确不做的事）

* 不做节点认证、不做签名；身份仅靠快照 id/承诺绑定，且只在本会话内有效。
* 不做文件分片交付：明细传输是协议内分页（`probe` / `fallback`），不是文件级分片。
* 不算法定的“最多传输多少字节”保证：稀疏时节省、密集时如实传输，
  回退必须具名且记录，绝不静默全表传输。
* 摘要参数（`position_bits`、`max_gen`、`leaf_limit`、`cord_max_bytes`、
  `rows_per_chunk`、`max_nodes_per_query`、`max_key_splits`）都是**会话配置**，
  由发起方给定、应答方原样采用，并纳入每条消息的 `negotiated`；绝不由数据或已知差异推导。
* 迁移目录只做**声明式**字段级操作（保留/改名、常量默认新增、删除、枚举一对一改写、
  int64↔十进制文本严格转换）：不做表的增删（同一目录内所有版本表集合必须一致）、
  不做未声明的类型转换、不做行级过滤或连接、不在多条迁移路径间自动选择。
  目录未覆盖的语义（如跨表计算列）不属于本工具。
