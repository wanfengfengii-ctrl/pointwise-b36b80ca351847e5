import copy
import json
import os
import sqlite3
import subprocess
import sys
import tempfile

from samples import make, ORIGINAL, REPLACEMENT
from source import Source
from exportqueue import Store, lease_name
from vendor.sdk2 import Client
import migration

HERE = os.path.dirname(os.path.abspath(__file__))
KILL_CODE = 42


def queue_version(path):
    db = sqlite3.connect(path)
    try:
        return db.execute("SELECT version FROM queue_meta WHERE slot=1").fetchone()[0]
    finally:
        db.close()


def table_dump(db, table):
    return [list(r) for r in db.execute("SELECT * FROM %s ORDER BY 1,2" % table)]


def flatten(segments):
    return [row for segment in segments for row in segment]


def expected_rows(source, snapshot, query):
    # Independent ordered-row reconciliation: recompute the expected ordered
    # row multiset straight from snapshot storage, not through the queue's
    # or the SDKs' read path.
    sql = "SELECT rid,station,value FROM source_rows WHERE sid=?"
    args = [snapshot]
    if query != "all":
        sql += " AND station=?"
        args.append(query[len("station="):])
    sql += " ORDER BY pos"
    return [list(r) for r in source.db.execute(sql, args)]


def crash(*args):
    return subprocess.run([sys.executable, os.path.join(HERE, "crashbox.py"), *args],
                          capture_output=True, text=True)


def finish_all(store, tasks):
    for name in ("all", "filtered", "empty"):
        while not store.status(tasks[name])["done"]:
            store.step(tasks[name])


def reconcile_all(source, store, tasks):
    original = tasks["original_snapshot"]
    actual = flatten(store.segments(tasks["all"]))
    assert actual == ORIGINAL
    assert actual == expected_rows(source, original, "all")
    assert actual.count(["r2", "B", None]) == 2
    assert flatten(store.segments(tasks["filtered"])) == expected_rows(source, original, "station=A")
    assert store.segments(tasks["empty"]) == []


def run():
    tmp = tempfile.mkdtemp(prefix="exportq-main-")
    src = os.path.join(tmp, "source.db")
    q = os.path.join(tmp, "queue.db")
    source, queue, tasks = make(src, q)
    task = tasks["all"]
    original = tasks["original_snapshot"]

    # --- existing business assertions on the legacy queue ---
    assert queue.status(task)["next_row"] == 2
    assert queue.segments(task) == [ORIGINAL[:2]]
    assert source.current() != original
    before = queue.status(task)
    old_segments = queue.segments(task)

    def fail(event):
        if event == "step.before_commit":
            raise RuntimeError("injected")

    queue.hook = fail
    try:
        queue.step(task)
    except RuntimeError as exc:
        assert str(exc) == "injected"
    else:
        raise AssertionError("injection not reached")
    assert queue.status(task) == before
    assert queue.segments(task) == old_segments
    queue.hook = lambda event: None

    # --- failed upgrade (raise at upgrade.before_commit): rollback, old library usable ---
    def fail_upgrade(event):
        if event == "upgrade.before_commit":
            raise RuntimeError("injected-upgrade")

    try:
        migration.upgrade(q, source, hook=fail_upgrade)
    except RuntimeError as exc:
        assert str(exc) == "injected-upgrade"
    else:
        raise AssertionError("upgrade injection not reached")
    assert queue_version(q) == 1
    assert queue.status(task) == before
    progressed = queue.step(task)  # legacy SDK1 step still works after the failure
    assert progressed["next_row"] == 4 and not progressed["done"]
    assert queue.segments(task) == [ORIGINAL[:2], ORIGINAL[2:4]]

    # freeze the legacy tables; nothing may write them after the switch
    legacy_dump = {name: table_dump(queue.db, name)
                   for name in ("export_tasks", "export_segments", "export_audit")}

    # --- real upgrade; repeating it has no side effects ---
    assert migration.upgrade(q, source) is None
    assert queue_version(q) == 2
    assert migration.upgrade(q, source) is None
    prot = source.protections()
    assert prot["legacy"] == []
    assert sorted(prot["leases"]) == sorted(
        [[lease_name(tasks[name]), original] for name in ("all", "filtered", "empty")])
    for name, rows in legacy_dump.items():
        assert table_dump(queue.db, name) == rows  # old tables byte-for-byte audit
    assert table_dump(queue.db, "segments_v2") == legacy_dump["export_segments"]  # payload 原文

    # --- the same already-open Store now works via SDK2 and the target tables ---
    before = queue.status(task)
    old_segments = queue.segments(task)
    queue.hook = fail
    try:
        queue.step(task)
    except RuntimeError as exc:
        assert str(exc) == "injected"
    else:
        raise AssertionError("post-upgrade injection not reached")
    assert queue.status(task) == before
    assert queue.segments(task) == old_segments

    def fail_after(event):
        if event == "step.after_commit":
            raise RuntimeError("injected-after")

    queue.hook = fail_after
    try:
        queue.step(task)  # commits the final segment and done, then loses the response
    except RuntimeError as exc:
        assert str(exc) == "injected-after"
    else:
        raise AssertionError("after_commit injection not reached")
    queue.hook = lambda event: None
    done_status = queue.status(task)
    assert done_status["done"] and done_status["next_row"] == 5 and done_status["segments"] == 3
    # the lease release was skipped after the commit; the retried step finalizes it
    assert [lease_name(task), original] in source.protections()["leases"]
    assert queue.step(task) == done_status  # no duplicate publish, no lost rows
    assert [lease_name(task), original] not in source.protections()["leases"]
    assert queue.segments(task) == [ORIGINAL[:2], ORIGINAL[2:4], ORIGINAL[4:5]]

    for name in ("filtered", "empty"):
        while not queue.status(tasks[name])["done"]:
            queue.step(tasks[name])
    actual = flatten(queue.segments(task))
    assert actual == ORIGINAL
    assert queue.segments(tasks["filtered"]) == [[ORIGINAL[0], ORIGINAL[3]]]
    assert queue.segments(tasks["empty"]) == []
    again = queue.status(task)
    assert queue.step(task) == again
    assert source.protections() == dict(legacy=[], leases=[])

    # --- independent ordered-row reconciliation: content, order, duplicates ---
    assert actual == expected_rows(source, original, "all")
    assert flatten(queue.segments(tasks["filtered"])) == expected_rows(source, original, "station=A")
    assert actual.count(["r2", "B", None]) == 2
    assert [r[0] for r in actual] == ["r1", "r2", "r2", "r3", "r4"]
    for name, rows in legacy_dump.items():
        assert table_dump(queue.db, name) == rows  # still byte-for-byte

    # --- post-switch create_export binds the current snapshot via an SDK2 lease ---
    current = source.current()
    post = queue.create_export("all")
    assert queue.status(post)["snapshot"] == current

    def raw_payloads(tid):
        return [r[0] for r in queue.db.execute(
            "SELECT payload FROM segments_v2 WHERE task_id=? ORDER BY segment_no", (tid,))]

    queue.step(post)
    first_segment = queue.segments(post)[0]
    first_raw = raw_payloads(post)
    queue.step(post)  # this read overwrites the client buffer the first batch borrowed
    assert queue.status(post)["done"]
    assert queue.segments(post)[0] == first_segment  # borrowed-batch overwrite
    assert raw_payloads(post)[:1] == first_raw       # never rewrites published bytes
    assert flatten(queue.segments(post)) == REPLACEMENT == expected_rows(source, current, "all")
    assert source.protections() == dict(legacy=[], leases=[])
    audit = table_dump(queue.db, "audit_v2")
    assert len(audit) == 1 and audit[0][1] == "create" and json.loads(audit[0][2])[0] == post

    # --- source protection fully released: the original snapshot can be collected ---
    collected = source.gc()
    assert original in collected
    assert current not in collected

    # --- existing SDK2 borrow/lease semantics ---
    client = Client(source)
    assert client.acquire("probe", current, "all") == "probe"
    assert client.acquire("probe", current, "all") == "probe"
    borrowed = client.read("probe", 0, 2)
    saved = copy.deepcopy(borrowed)
    nested = borrowed[0]
    next_batch = client.read("probe", 2, 2)
    assert borrowed is next_batch
    assert nested == ["OVERWRITTEN"]
    assert saved == [["new1", "A", 999], ["new2", "B", 888]]
    source.publish([["later", "Z", 1]])
    assert current not in source.gc()
    assert Client(source).read("probe", 0, 1) == [saved[0]]
    try:
        client.acquire("probe", source.current(), "all")
    except ValueError:
        pass
    else:
        raise AssertionError("owner conflict accepted")
    client.release("probe")
    client.release("probe")
    assert current in source.gc()
    print(json.dumps(dict(old_export=actual, empty=queue.segments(tasks["empty"]),
                          upgraded=True, version=queue_version(q),
                          post_switch_export=flatten(queue.segments(post)),
                          protections=source.protections(), sdk2_saved=saved), ensure_ascii=False))
    queue.close()
    source.close()


def make_case():
    tmp = tempfile.mkdtemp(prefix="exportq-kill-")
    src = os.path.join(tmp, "source.db")
    q = os.path.join(tmp, "queue.db")
    proc = crash("make", src, q)
    assert proc.returncode == 0, proc.stderr
    return src, q, json.loads(proc.stdout)


def kill_upgrade_before_commit():
    src, q, tasks = make_case()
    proc = crash("upgrade", src, q, "upgrade.before_commit", "1")
    assert proc.returncode == KILL_CODE, proc.stderr
    assert queue_version(q) == 1  # rolled back
    source = Source(src)
    store = Store(q, source)  # opens on the still-legacy library
    assert len(source.protections()["legacy"]) == 3  # cursors intact
    store.step(tasks["all"])  # old library still usable
    assert store.status(tasks["all"])["next_row"] == 4
    assert migration.upgrade(q, source) is None  # retry succeeds
    assert queue_version(q) == 2
    finish_all(store, tasks)  # the pre-upgrade Store object switched to v2
    reconcile_all(source, store, tasks)
    assert source.protections() == dict(legacy=[], leases=[])
    assert tasks["original_snapshot"] in source.gc()
    store.close()
    source.close()
    print(json.dumps(dict(kill="upgrade.before_commit", version=queue_version(q))))


def kill_upgrade_after_commit():
    src, q, tasks = make_case()
    proc = crash("upgrade", src, q, "upgrade.after_commit", "1")
    assert proc.returncode == KILL_CODE, proc.stderr
    assert queue_version(q) == 2  # committed before the kill
    source = Source(src)
    prot = source.protections()
    assert len(prot["legacy"]) == 3 and len(prot["leases"]) == 3  # handoff unfinished
    assert migration.upgrade(q, source) is None  # retry only finalizes the handoff
    assert source.protections()["legacy"] == []
    assert migration.upgrade(q, source) is None  # repeat: no side effects
    store = Store(q, source)
    finish_all(store, tasks)
    reconcile_all(source, store, tasks)
    assert source.protections() == dict(legacy=[], leases=[])
    store.close()
    source.close()
    print(json.dumps(dict(kill="upgrade.after_commit", version=queue_version(q))))


def kill_step_before_commit():
    src, q, tasks = make_case()
    source = Source(src)
    assert migration.upgrade(q, source) is None
    source.close()
    proc = crash("step", src, q, "step.before_commit", "1", tasks["all"])
    assert proc.returncode == KILL_CODE, proc.stderr
    source = Source(src)
    store = Store(q, source)
    assert store.status(tasks["all"])["next_row"] == 2  # rolled back
    assert store.segments(tasks["all"]) == [ORIGINAL[:2]]
    finish_all(store, tasks)
    reconcile_all(source, store, tasks)
    assert source.protections() == dict(legacy=[], leases=[])
    rows = flatten(store.segments(tasks["all"]))
    store.close()
    source.close()
    print(json.dumps(dict(kill="step.before_commit", rows=rows)))


def kill_step_after_commit_filtered():
    src, q, tasks = make_case()
    source = Source(src)
    assert migration.upgrade(q, source) is None
    source.close()
    # filtered task: step 1 publishes its only segment, step 2 commits done;
    # the kill lands right after that commit, before the lease release
    proc = crash("step", src, q, "step.after_commit", "2", tasks["filtered"])
    assert proc.returncode == KILL_CODE, proc.stderr
    source = Source(src)
    leaked = [lease_name(tasks["filtered"]), tasks["original_snapshot"]]
    assert leaked in source.protections()["leases"]  # release was skipped
    store = Store(q, source)  # reopen finalizes the cleanup
    assert leaked not in source.protections()["leases"]
    assert store.status(tasks["filtered"])["done"]
    assert store.segments(tasks["filtered"]) == [[ORIGINAL[0], ORIGINAL[3]]]  # no dup/loss
    finish_all(store, tasks)
    reconcile_all(source, store, tasks)
    assert source.protections() == dict(legacy=[], leases=[])
    assert tasks["original_snapshot"] in source.gc()
    store.close()
    source.close()
    print(json.dumps(dict(kill="step.after_commit", task="filtered")))


def kill_step_after_commit_all():
    src, q, tasks = make_case()
    source = Source(src)
    assert migration.upgrade(q, source) is None
    source.close()
    # all task: the second step commits the final segment and done together
    proc = crash("step", src, q, "step.after_commit", "2", tasks["all"])
    assert proc.returncode == KILL_CODE, proc.stderr
    source = Source(src)
    leaked = [lease_name(tasks["all"]), tasks["original_snapshot"]]
    assert leaked in source.protections()["leases"]
    store = Store(q, source)  # reopen finalizes the lease
    assert leaked not in source.protections()["leases"]
    assert store.status(tasks["all"])["done"]
    assert flatten(store.segments(tasks["all"])) == ORIGINAL
    finish_all(store, tasks)
    reconcile_all(source, store, tasks)
    assert source.protections() == dict(legacy=[], leases=[])
    assert tasks["original_snapshot"] in source.gc()
    store.close()
    source.close()
    print(json.dumps(dict(kill="step.after_commit", task="all")))


def run_kill_tests():
    kill_upgrade_before_commit()
    kill_upgrade_after_commit()
    kill_step_before_commit()
    kill_step_after_commit_filtered()
    kill_step_after_commit_all()


if __name__ == "__main__":
    run()
    run_kill_tests()
