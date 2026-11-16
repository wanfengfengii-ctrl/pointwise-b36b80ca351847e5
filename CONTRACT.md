# 固定快照导出旧系统与待迁移契约

## 提供的工程

这是完整的可运行旧实现，不声称来自外部产品。vendor/sdk1.py、vendor/sdk2.py 是本项目内置双代查询依赖的完整源码，不是网络服务替身或待下载的软件包；没有第三方安装步骤。source.py 是独立查询快照服务，exportqueue.py 是导出队列。两者通常分别使用一个 SQLite 文件；内存路径只用于基础示例。

源快照服务为其他查询用户发布和保留不可变快照，不能将其全部历史或其他用户的保护认作导出队列私有状态。队列向使用者发布稳定的分段及进度。Source.gc() 是显式业务操作，会删除既非当前、又无保护的快照；不自动按时间回收。

## 已有格式、输入和输出

Source(path=":memory:") 打开或新建源库；publish(rows) 替换当前表，返回新的不透明快照字符串。rows 的每项为 [rid,station,value]，rid、station 为非空字符串，value 为整数或 null，不接受布尔数。重复行及重复 rid 合法，按输入顺序保存，不能去重。current() 返回当前快照 ID；protections() 返回 {"legacy":[[cursor,snapshot],...],"leases":[[owner,snapshot],...]}，分别按保护名排序；gc() 返回本次删除的快照 ID 升序列表。close() 关闭连接。

旧查询只接受 "all" 或 "station=非空站点名"；后者按站点精确相等过滤，结果始终按原输入顺序排列。不支持任意 SQL、排序选项或查询参数扩展。

Store(queue_path, source) 接受队列路径及 Source 实例。
- create_export(query) 返回 task_id 字符串，任务绑定调用时的当前快照。
- step(task_id) 最多发布接下来的两行，返回 status；取到不足两行才标为完成，零行不新增空分段。恰好两行的末段还须下一次空读确认完成。
- status(task_id) 返回 {"task_id":字符串,"snapshot":创建时快照,"next_row":已发布行数,"done":布尔,"segments":已发布分段数}。
- segments(task_id) 返回按 segment_no 连续升序的分段数组，每段为有序行数组；只返回已发布内容。
- 未知任务抛 KeyError，非法查询抛 ValueError。已完成任务再次 step 不增分段或进度。
- close() 仅关闭队列连接，不删除任务，不释放未完成任务的来源保护。

源库完整旧 Schema 由 source.SCHEMA 定义：snapshots(sid)、current_snapshot(slot,sid)、source_rows(sid,pos,rid,station,value)、legacy_cursors(cid,sid,query)。每个旧游标持久绑定 sid 和 query，存在即保护其快照，直到 sdk1.Client.close(cursor)。客户端或进程退出不释放游标。snapshot_leases(owner,sid,query) 是已提供 SDK2 的原生持久表，旧系统不向它写入。

队列完整旧 Schema 在 exportqueue.SCHEMA：queue_meta(slot,version=1)、export_tasks(task_id,query,snapshot,cursor,next_row,done)、export_segments(task_id,segment_no,payload)、export_audit(seq,event,raw)。payload 为分段行数组的 JSON 文本，审计 raw 为 [task_id,query,snapshot,cursor] 的原始 JSON 文本。源码给出全部键、约束和序号规则。一个任务的分段内容和 next_row、done 已由旧队列在同一事务提交，不能把这项已有正确性算作本轮新能力。

## 完整兼容样例

samples.make(source_path,queue_path) 可在任意空路径构造旧库，缺省用两个独立内存库；返回 (Source,Store,任务字典)。ORIGINAL 的五行是：
[["r1","A",10],["r2","B",null],["r2","B",null],["r3","A",-2],["r4","B",0]]
第一个 all 任务已发布首两行，next_row=2、done=false、segments=1；另有尚未开始的 station=A 和 station=Z 任务。随后当前表已整体替换为：
[["new1","A",999],["new2","B",888],["new3","A",777]]
并已调用 gc。任务字典含 all、filtered、empty 三个任务 ID 及 original_snapshot。三个任务均仍绑定 ORIGINAL。

all 的完整分段须为 ORIGINAL 的 [0:2]、[2:4]、[4:5]，最终 next_row=5、done=true、segments=3；filtered 只有 [[r1,A,10],[r3,A,-2]] 一段，但第二次空读后才 done=true；empty 无分段、首次 step 即 done=true。所有 task_id、snapshot 和已发布 payload 原文均须保留。全部完成后不再有这三个任务的来源保护；无其他保护时原快照可被 gc 删除。

## SDK2 的公开语义

sdk2.Client(source) 不创建游标，不自动绑定当前表。
acquire(owner,snapshot,query) 返回 owner，提交持久快照租约；同 owner、同绑定幂等，异绑定抛 ValueError，不存在的快照抛 LookupError。租约不随客户端销毁或进程退出而消失，直至 release(owner)，重复释放无副作用。
read(lease,offset,limit) 返回该固定查询结果的借用批，offset 是过滤后结果的行位置，limit 为正整数；租约失效抛 LookupError。该 Client 的下一次 read 可覆盖外层列表及此前借出的内层行列表；读到空批也会使旧借用失效。保留旧批必须取得独立所有权，不能只保留列表引用。
retire_cursor(cursor) 幂等解除旧游标保护，不替调用方取得新保护。该接口仅用于退役旧游标，不是迁移事务。
SDK2 原语各自在源库提交；没有将源保护与队列状态一起提交的现成接口。不可直接改写 source_rows、legacy_cursors 或 snapshot_leases 绕过 SDK2 的业务操作；Source.publish/gc/protections 是独立业务及观察入口。

## 本轮迁移与兼容边界

实现 migration.upgrade(queue_path,source,hook=None)，成功返回 None。限定维护窗口单工作者，升级时没有其他写者或在途 step；不做在线迁移、多任务竞争、远端上传或吞吐优化。升级后 queue_meta 的 version 为 2，任务及分段须真实进入自行设计的目标 Schema；原有三张 export_* 表逐字节留作审计，切换后正常操作不能从这些旧表取任务、分段或结果。已经打开的 Store 也要立即走目标 Schema 和 SDK2，不能只保证新对象。

原任务必须从创建时快照继续，不能按查询名重新绑定当前表。迁移和任务完成必须交接源保护：未发布内容仍依赖源快照时不能解除最后保护；如果选择先转存任务自有完整结果，确认完整后即可解除源保护；任务完成且不再依赖来源后不得遗留该任务的保护。不能删除或接管其他使用者的游标或租约。不强制选择持久租约或完整结果副本，不要求二者同时实现。

迁移保全任务 ID、查询、snapshot、进度、分段原文及原始审计记录，失败保留可用旧库，重复升级无副作用。迁移失败后允许保留同时保护原快照的新租约供重试，但重试成功及任务完成后不得遗留额外保护。切换后不再调用 SDK1；SDK2 原生发布的后续当前快照可供新 create_export 使用，不能改变已有任务。

Store.hook(event) 及 upgrade 的 hook(event) 缺省为空操作。公开故障点为 upgrade.before_commit、upgrade.after_commit、step.before_commit、step.after_commit，分别紧邻队列版本切换或分段进度提交。before_commit 抛错须回滚本次队列变更；after_commit 抛错或进程终止可丢响应，但重开不得重复发布或丢行。迁移终止重试同理。完成提交后清理来源保护可在重新打开或重试 step/upgrade 时收尾，但收尾后必须全部解除；不得把已发布进度回退。create_export 中途终止不在本轮验收范围。

## 交付与运行

保留 verify.py 的既有业务断言，可增加核验。补充独立有序行核账、真实旧库升级后续导、源表替换及借用批覆盖核验，并用文件数据库和子进程在公开提交点真正终止重开。核对行内容、顺序和重复次数，不能仅核总数。附简短所有权说明，说明暂停、切换和完成时源保护及分段的归属。

.pointwise-runtime.json 是实际运行清单：version 为 1，kind 为 cli，setup 是准备命令的 argv 数组列表，verify、demo 各为一个 argv 数组。当前 verify 为 ["python3","verify.py"]，demo 为 ["python3","demo.py"]，setup 为空。基础核验核对旧队列、回滚和 SDK2 的借用/租约语义，演示打印真实暂停任务及续导行；它们不包含迁移答案或目标验收的实现。交付时让 verify 经真实升级核账，让 demo 展示升级后换表仍续导原行及完成后的保护释放。
