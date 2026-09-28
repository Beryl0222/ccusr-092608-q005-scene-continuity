# AI短剧镜头连续性工单

定义短剧候选镜头、连续性观察、承接依赖和交付锁片事件。

## 目录

- `contracts/domain.schema.json`：对象、事件和载荷字段约定。
- `data/sample.json`：可直接校验的联调样例。
- `src/scene_continuity/`：基础契约校验、事件溯源工单服务（`service.py`）、可控时钟（`clock.py`）、事件存储（`store.py`）、采用链解释（`provenance.py`）与命令行入口。
- `tests/`：信封、时间、版本和事件载荷测试，以及隔离/幂等、角色权限、沿边返工、原子锁片、预算交期、时钟恢复和最终镜头解释的全链路场景测试。
- `docs/domain.md`：领域对象与事件语义。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 编译检查

```bash
python3 -m compileall -q src tests
```

## 样例校验

```bash
PYTHONPATH=src python3 -m scene_continuity.cli contracts/domain.schema.json data/sample.json
```

样例有效时输出 `valid`；发现问题时逐行给出字段、代码和中文说明，并返回非零状态。
