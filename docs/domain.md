# 领域约定

定义短剧候选镜头、连续性观察、承接依赖和交付锁片事件。

聚合对象包括`scene_baseline`、`shot_candidate`、`continuity_issue`、`edit_release`。事件类型包括`BASELINE_FROZEN`、`CANDIDATE_RECEIVED`、`ISSUE_RECORDED`、`SHOT_LOCKED`、`REWORK_PROPAGATED`。所有发生时间都必须携带时区，版本号从 1 开始递增，基础校验不会改写调用方输入。

## 事件载荷

- `CANDIDATE_RECEIVED`：载荷还需包含 `batch_ref`, `content_hash`。
- `ISSUE_RECORDED`：载荷还需包含 `dimension`, `evidence`。
- `SHOT_LOCKED`：载荷还需包含 `edit_ref`, `candidate_ref`。

相同事件标识的业务幂等、冲突隔离和状态推进由上层服务负责；本仓库只定义可稳定交换的基础事实。
