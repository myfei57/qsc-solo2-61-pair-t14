# 舱室分区原子重排（Zone Reorg）

> 目标：分区表、环境告警路由、巡检路线三处口径作为**同一次重排**一起生效；
> 中途失败整体回滚；重排全程进审计；重排前后的告警都能按"当时分区"复核。

本文档固定两个曾经"两可"的决策，并描述落地实现（`tunnelnet/reorg/`）。

---

## 1. 固定决策

### 决策一：生效时机 —— 等当前巡检轮次结束，在**轮次边界**生效

重排提交后先**暂存（armed）**并完成全部校验，但**不**改变任何在线投影。
当当前巡检轮次结束（`POST /api/patrol/round/end`，由重排协调器执行）时，
在同一个原子提交里把旧轮按旧路由归档、把分区表/环境路由/巡检路线切到新版本，
并用新路线开启下一轮。

* **在跑的告警按出生版本钉死（birth-version pin）**：告警产生时冻结
  `birth_partition / birth_cabin / birth_version / at`，处置到关闭都按出生版本。
* **重排后新出生的告警**走新版本。
* armed 期间新产生的告警仍按旧版本路由。
* 因此告警处置中途**不存在**"前半截老分区、后半截新分区"的歧义。
* 当没有活跃巡检轮次时（或提交显式 `effective=immediate`），立即生效。

另一个选项（立即对在跑告警生效）被否决：它会让一条正在处置中的告警中途换舱，
口径在一次处置内部就不自洽。

### 决策二：回滚目标 —— 回到**上一个已确认（confirmed）的快照**

* 每个激活版本经过一轮验证后可 `confirm`；**只有 confirmed 版本是回滚锚点**。
* 回滚不是删除/覆盖，而是追加一个**补偿版本（origin=rollback）**，其内容等于
  最后已确认布局，`restores_version` 指向该锚点。历史版本与请求永不删除、不改写。
* **发布前**（armed 阶段、或激活提交过程中）失败：不产生已发布版本，
  在线投影整体回滚到提交前快照，请求标记 `failed` 或 `cancelled`。
* 已 confirmed 的版本不能作为"回滚自己"的目标（回滚锚点是它之前的快照）。

另一个选项（回到"重排前那一份"）被否决：若重排前那份本身未经确认、甚至就是一次
错误热修，以它为锚会把已知错误固化为唯一可恢复点。

---

## 2. 核心模型

### ZoneLayout（不可变版本，`reorg/model.py`）

一次重排的**完整**路由画面，三投影合一：

| 字段 | 含义 |
| --- | --- |
| `partition_routes` | `partition_id -> cabin_id`，环境告警路由依据 |
| `patrol_route` | 巡检路线（有序舱室列表） |
| `sensor_partitions` | `sensor_id -> partition_id` |
| `version` | 单调递增版本号（失败也占槽，append-only 不回收） |
| `based_on` | 基于哪个版本 |
| `activated_at` | 实际生效时刻（as-of 复核的边界） |
| `confirmed / confirmed_at` | 是否已确认（回滚锚点） |
| `origin` | `bootstrap | reorg | hotfix | rollback` |
| `restores_version` | 补偿版本恢复的锚点版本 |
| `fingerprint` | 三投影内容的 SHA-256（顺序无关） |

初始布局在启动播种后捕获为 **v1，confirmed，origin=bootstrap**，
生效时刻为播种时刻，使播种产生的历史告警能按 v1 复核。

### ReorgRequest（重排生命周期，append-only）

状态机：

```
            submit (有活跃轮次)
        ┌──────────────────────────► armed
 submit │                             │  /round/end 或 /activate
(无轮/immediate)                      ▼
        └──────────────────────► active ──confirm──► confirmed
              │  ▲                  │
              │  └── rollback ──────┤  (补偿版本, 回到最后 confirmed)
              │                     ▼
              └──────────────► rolled_back / cancelled / failed
```

### ReorgCoordinator（`reorg/coordinator.py`）

唯一允许"三投影一起动"的组件。持有：

* 一把可重入锁（序列化全部重排）；
* append-only 的 `ReorgLedger`（版本文档 + 请求文档 + current 指针，全部走
  文件原子写；指针最后写，崩溃也不会发布"文档没写全"的版本）；
* 当前版本指针、最后 confirmed 锚点；
* 每次状态迁移都向既有审计注册表写主事件 + `_ref` 孪生事件：
  `reorg_armed / reorg_activated / reorg_confirmed / reorg_rolled_back /
  reorg_cancelled / reorg_failed / patrol_round_closed / partition_hotfix`。

---

## 3. 一次重排的原子提交

`POST /api/reorg/submit`（请求体含 `request_id`、`manifest`、`effective`、
可选 `base_version`）：

1. 协调器加锁；`request_id` 命中已有请求 → **幂等返回**原记录（不重复生效）。
2. `base_version` 非当前版本 → `409`（乐观并发控制）。
3. 已有 armed/active 未结请求 → `409`（并发重排串行化）。
4. **全量**校验 manifest（见下），构造候选版本（仅入内存+落盘，不投影）。
5. 无活跃轮次或 `immediate` → 立即激活；否则保持 armed。

激活提交（`_activate`）：

1. 先抓三投影快照（mapping/revision、env 版本与传感器绑定、巡检 route）。
2. 轮次边界路径先 `patrol.archive_round()`（旧轮按**旧**路由归档）。
3. `_commit_projections`：`PartitionTable.replace_mapping` →
   `Environment.apply_layout` → 巡检开新轮并安装新路由。
4. 任一步抛错：`_safe_restore` 用快照恢复（恢复路径绕过业务入口，避免同一故障
   再次打断回滚），请求标记 `failed` 并审计，异常继续上抛。已归档的轮次会被
   `reopen_archived_round` 恢复，不会留下"没有活跃轮次"的中间态。
5. 全部成功才写版本文档、**最后**切换 current 指针、请求置 `active`、写审计。

**Manifest 校验**（`reorg/manifest.py`，必须是全量、三处自洽）：

* 非空；分区/舱室/传感器 id 非空；
* 巡检路线无重复舱；两个分区不能路由到同一舱；
* 路线停靠舱 = 分区路由覆盖舱（两个集合必须相等）；
* **不得丢掉任何在用（active 且未退役）舱室**；
* 传感器绑定的分区必须存在。

---

## 4. 历史告警复核（归属不可改写）

`Alarm` 在产生时冻结出生归属（`env/model.py`），之后任何重排**不重算**。

`GET /api/reorg/alarms?sensor=&at=`（`review_alarm`）对每条告警给出：

* `birth_version / birth_partition / birth_cabin`：告警自报的出生归属（不可变）；
* `birth_layout_cabin`：用**出生版本布局**重新解析该分区得到的舱；
* `birth_attribution_intact`：出生布局解析结果与出生自报一致；
* `current_cabin`：用**当前**布局解析同一生成分区；
* `routing_changed_since_birth`：当前路由与出生路由是否已漂移；
* `version_effective_at_alarm` / `effective_version`：按 `at` 用
  `layout_effective_at`（`activated_at <= at` 的最高版本）解析"当时生效版本"。

这样"重排前的告警按当时分区复核、重排后的告警按新版本复核"都可直接查询，
且历史告警的归属事实永不被后续重排改写。

---

## 5. 并发、幂等与时序

* **并发重排**：协调器单把 RLock 串行化；同一时刻最多一个 armed/active 请求；
  撞车返回 `409 Conflict`。已用 8 线程并发提交测试验证只有一个赢家。
* **幂等**：`request_id` 为重放键，重复提交返回同一 `request` 与同一候选版本，
  不产生第二次变更。
* **读时序**：下游读取期间三投影要么是完整旧版本、要么是完整新版本，看不到
  撕裂态（提交在同一把协调器锁内完成）。协调器状态、快照、健康检查都在该锁视角内。
* **锁序**：协调器锁 →（分区表 / environment / patrol 各自的内部锁），
  反向获取不存在，避免 AB-BA。
* **持久化**：布局/请求先各自原子落盘，current 指针最后写；重启时 `preload`
  恢复账本与版本指针，随后播种在正确版本上进行，再 `bootstrap` 对齐在线投影。

---

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/reorg` | 当前布局与重排状态（当前/已确认版本、pending、轮次） |
| GET | `/api/reorg/snapshot` | 当前完整 manifest（提交可用它改） |
| GET | `/api/reorg/list` | 全部历史版本与全部请求（审计视图） |
| GET | `/api/reorg/layout?version=n` | 单个历史版本 |
| POST | `/api/reorg/submit` | 暂存/立即激活一次原子重排（幂等键 `request_id`） |
| POST | `/api/reorg/activate` | 提前立即激活 armed 请求 |
| POST | `/api/reorg/confirm` | 确认当前激活版本（建立回滚锚点） |
| POST | `/api/reorg/rollback` | 撤销 armed，或补偿回滚到最后 confirmed |
| POST | `/api/reorg/round/end` | 结束巡检轮次并原子激活 armed 重排 |
| GET | `/api/reorg/alarms?sensor=&at=` | 按出生版本/当时版本复核告警 |
| POST | `/api/patrol/round/end` | 同上（巡检侧入口） |
| GET | `/api/patrol/rounds` | 已归档巡检轮次（各自路线与打卡） |
| POST | `/api/env/partition` | 兼容的单分区编辑，现也版本化（`origin=hotfix`） |

旧的 `POST /api/patrol/rebuild` 与舱室单字段编辑保留兼容；它们不做三投影联动，
统一口径应走 `/api/reorg/submit`。

---

## 7. 测试

* `tests/test_reorg.py`：manifest 校验、生命周期、轮次边界、确认/回滚锚点、
  幂等、并发冲突、告警出生钉死、as-of 复核、提交失败整体回滚、hotfix 版本化、
  指纹稳定性与版本往返。
* `tests/test_reorg_console.py`：全部 HTTP 端点的端到端语义（409/400/404、
  轮次边界切换、历史告警复核、回滚内容、重启无关）。

`python3 -m unittest discover -s tests` 全部通过。
