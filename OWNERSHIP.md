# 迁移所有权说明

## 暂停时（任务未完成）
- 任务的源保护归任务自己所有：version 1 为 `legacy_cursors` 中的持久游标，version 2 为
  `snapshot_leases` 中 `owner = export/<task_id>` 的持久租约，绑定创建时快照与查询。
- `Store.close()` 不释放该保护；暂停、换表、进程重开都不改变任务绑定的快照。
- 已发布分段归队列所有，以 JSON 原文落库（v1 `export_segments`，v2 `segments_v2`）。
  分段不借用 SDK2 的批缓冲：每次 `read` 后立即取得行的独立所有权再序列化，
  该 Client 后续 `read` 覆盖借用批不会改变已发布分段。

## 切换时（migration.upgrade，维护窗口单工作者）
- 第一阶段先为每个未完成任务取得上述 SDK2 租约（源库各自提交、幂等），此时游标与租约
  双重保护，快照在任何时候都不失去最后保护；其他查询用户的游标与租约一律不动。
- 第二阶段单个队列事务：建 `tasks_v2`/`segments_v2`/`audit_v2`，逐字节复制任务进度与
  分段原文，`queue_meta.version` 置 2。`upgrade.before_commit` 前失败整体回滚，旧库可用；
  已取得的租约留给重试幂等复用。
- 第三阶段用 `sdk2.retire_cursor` 退役任务自己的旧游标。已完成任务不取租约，仅退役其
  遗留游标。若切换在提交点被终止，重开 Store 或重试 upgrade 会按 `tasks_v2` 中
  `legacy_cursor`/`lease` 标记收尾；重复升级无其他副作用。
- 旧 `export_tasks`/`export_segments`/`export_audit` 三表逐字节留作审计，切换后正常操作
  不再从旧表取任务、分段或结果。

## 完成时
- 分段与进度（`next_row`/`done`）在同一队列事务提交；提交后任务不再依赖来源，
  已发布进度永不回退。
- 完成提交后立即释放该任务租约；若在提交与释放之间终止，重新打开 Store 或重试
  step/upgrade 时收尾释放。收尾完成后该任务不遗留任何源保护，原快照在无其他保护时
  可被 `Source.gc()` 回收。
