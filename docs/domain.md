# 领域约定

定义短剧候选镜头、连续性观察、承接依赖、返工工单和交付锁片事件。系统只管理元数据与决定，不处理音视频文件本身。

聚合对象包括 `scene_baseline`、`shot_candidate`、`continuity_issue`、`edit_release`。事件类型包括 `BASELINE_FROZEN`、`CANDIDATE_RECEIVED`、`OBSERVATION_RECORDED`、`CONTINUITY_EDGE_LINKED`、`CONSTRAINT_CONFIRMED`、`ISSUE_RECORDED`、`CANDIDATE_WITHDRAWN`、`TICKET_REOPENED`、`REGEN_REQUESTED`、`REGEN_APPROVED`、`REGEN_REJECTED`、`EDIT_OPENED`、`SHOT_LOCKED`、`REWORK_PROPAGATED`、`RELEASE_DELIVERED`。所有发生时间都必须携带时区，版本号从 1 开始递增，基础校验不会改写调用方输入。

## 角色与权限

- `observer`（观察者）：登记候选、观察与差异，不能确认跨镜头约束、不能批准重生成、不能锁片或交付。
- `continuity_lead`（连续性负责人）：确认跨镜头约束，可淘汰候选并触发沿承接边的精确返工。
- `producer`（制片人）：仅在剩余预算与交期范围内批准重生成请求。
- `editor`（剪辑）：在候选未被撤销/未交付的前提下锁定镜头；两个剪辑方案对同一候选的锁竞争必须原子裁决。

## 标识、幂等与隔离

- 每条命令携带客户端生成的幂等键（回执键）。同一键重放返回首次结果，不产生重复事件。
- 候选片段的稳定标识为 `clip_ref`；当 `clip_ref` 相同但 `content_hash` 或 `batch_ref` 不同时，视为互相隔离的不同候选，服务拒绝覆盖并要求隔离存储。
- 承接边 `handled_by: from_candidate -> to_candidate` 表达动作/空间方向的入点依赖；只沿可达边精确重开工单，不影响无关节点。

## 时间与恢复

- 服务使用可控时钟：挑片、复核、交付三类期限由时钟推进判定逾期；时钟不前进则不产生时间副作用。
- 命令以持久化队列方式执行；服务重启后继续未完成命令，已完成命令凭幂等键去重。

## 不可变交付与可解释性

- 交付版本一旦产生即不可变：后续撤销/返工只产生新版本，原版本保留原选择与修订说明。
- 任一最终镜头都可沿 `SHOT_LOCKED` / 承接边 / `TICKET_REOPENED` / `REGEN_*` 事件回溯出完整采用链：经过哪些候选、哪些人工判断与哪几轮连锁返工。

## 事件载荷

- `CANDIDATE_RECEIVED`：载荷还需包含 `batch_ref`, `content_hash`。
- `OBSERVATION_RECORDED`：载荷还需包含 `dimension`, `evidence`。
- `CONTINUITY_EDGE_LINKED`：载荷还需包含 `from_candidate_ref`, `to_candidate_ref`。
- `CONSTRAINT_CONFIRMED`：载荷还需包含 `constraint`。
- `ISSUE_RECORDED`：载荷还需包含 `dimension`, `evidence`。
- `CANDIDATE_WITHDRAWN`：载荷还需包含 `reason`。
- `TICKET_REOPENED`：载荷还需包含 `root_candidate_ref`。
- `REGEN_REQUESTED`：载荷还需包含 `estimated_cost`。
- `REGEN_APPROVED` / `REGEN_REJECTED`：载荷还需包含 `ticket_ref`（拒绝另需 `reason`）。
- `EDIT_OPENED`：载荷还需包含 `edit_ref`, `plan_ref`, `deadline`。
- `SHOT_LOCKED`：载荷还需包含 `edit_ref`, `candidate_ref`。
- `REWORK_PROPAGATED`：载荷还需包含 `root_candidate_ref`, `reopened_ticket_refs`。
- `RELEASE_DELIVERED`：载荷还需包含 `release_ref`, `selections`, `revision_note`。

相同事件标识的业务幂等、冲突隔离和状态推进由上层服务负责；本仓库只定义可稳定交换的基础事实。
