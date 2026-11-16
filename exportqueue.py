import json
import sqlite3
import uuid
from source import query_text
from vendor import sdk1, sdk2

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_meta(slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL);
INSERT OR IGNORE INTO queue_meta VALUES(1,1);
CREATE TABLE IF NOT EXISTS export_tasks(task_id TEXT PRIMARY KEY,query TEXT NOT NULL,snapshot TEXT NOT NULL,cursor TEXT NOT NULL,next_row INTEGER NOT NULL,done INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS export_segments(task_id TEXT NOT NULL,segment_no INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(task_id,segment_no));
CREATE TABLE IF NOT EXISTS export_audit(seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL,raw TEXT NOT NULL);
"""

# Target schema (queue_meta.version=2). The three legacy export_* tables stay
# byte-for-byte as audit; post-switch operations never read tasks, segments or
# results from them.
V2_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS tasks_v2("
    "task_id TEXT PRIMARY KEY,query TEXT NOT NULL,snapshot TEXT NOT NULL,"
    "lease TEXT,legacy_cursor TEXT,next_row INTEGER NOT NULL,done INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS segments_v2("
    "task_id TEXT NOT NULL,segment_no INTEGER NOT NULL,payload TEXT NOT NULL,"
    "PRIMARY KEY(task_id,segment_no))",
    "CREATE TABLE IF NOT EXISTS audit_v2("
    "seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL,raw TEXT NOT NULL)",
)


def lease_name(task_id):
    """Deterministic SDK2 lease owner for a task's own source protection."""
    return "export/" + task_id


def finalize_v2(queue_db, client):
    """Finish pending source-protection handoff/cleanup, idempotently.

    - legacy_cursor still recorded: retire it through SDK2 (migration phase 3
      was interrupted between the version commit and cursor retirement).
    - done task with a lease still recorded (or left over from a failed
      upgrade attempt under the same deterministic name): release it; the
      task no longer depends on the source.
    The source action commits before its queue marker is cleared, so a crash
    in between simply retries a no-op on the next pass.
    """
    rows = queue_db.execute(
        "SELECT task_id,legacy_cursor,done,lease FROM tasks_v2").fetchall()
    for task_id, cursor, done, lease in rows:
        if cursor is not None:
            client.retire_cursor(cursor)
            with queue_db:
                queue_db.execute(
                    "UPDATE tasks_v2 SET legacy_cursor=NULL WHERE task_id=?", (task_id,))
        if done:
            client.release(lease_name(task_id))  # no-op if not held
            if lease is not None:
                with queue_db:
                    queue_db.execute(
                        "UPDATE tasks_v2 SET lease=NULL WHERE task_id=?", (task_id,))


class Store:
    def __init__(self, path, source):
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        self.source = source
        self.client = sdk1.Client(source)    # legacy (version 1) path only
        self.client2 = sdk2.Client(source)   # target (version 2) path
        self.hook = lambda event: None
        self._v2_finalized = False
        self._mode()

    def _version(self):
        row = self.db.execute("SELECT version FROM queue_meta WHERE slot=1").fetchone()
        return row[0] if row else 1

    def _mode(self):
        # Re-read per operation: an upgrade commits on another connection, and
        # already-open Store objects must switch to the target schema at once.
        version = self._version()
        if version == 2 and not self._v2_finalized:
            finalize_v2(self.db, self.client2)
            self._v2_finalized = True
        return version

    def create_export(self, query):
        if self._mode() == 2:
            return self._create_v2(query)
        cursor, snapshot = self.client.open(query)
        task = uuid.uuid4().hex
        try:
            with self.db:
                self.db.execute("INSERT INTO export_tasks VALUES(?,?,?,?,0,0)",
                                (task, query, snapshot, cursor))
                self.db.execute("INSERT INTO export_audit(event,raw) VALUES('create',?)",
                                (json.dumps([task, query, snapshot, cursor]),))
        except BaseException:
            self.client.close(cursor)
            raise
        return task

    def _create_v2(self, query):
        query_text(query)
        snapshot = self.source.current()
        task = uuid.uuid4().hex
        lease = lease_name(task)
        self.client2.acquire(lease, snapshot, query)
        try:
            with self.db:
                self.db.execute("INSERT INTO tasks_v2 VALUES(?,?,?,?,NULL,0,0)",
                                (task, query, snapshot, lease))
                self.db.execute("INSERT INTO audit_v2(event,raw) VALUES('create',?)",
                                (json.dumps([task, query, snapshot, lease]),))
        except BaseException:
            self.client2.release(lease)
            raise
        return task

    def _task(self, task):
        row = self.db.execute("SELECT query,snapshot,cursor,next_row,done FROM export_tasks WHERE task_id=?",
                              (task,)).fetchone()
        if row is None:
            raise KeyError(task)
        return row

    def _task_v2(self, task):
        row = self.db.execute("SELECT query,snapshot,lease,next_row,done FROM tasks_v2 WHERE task_id=?",
                              (task,)).fetchone()
        if row is None:
            raise KeyError(task)
        return row

    def step(self, task):
        if self._mode() == 2:
            return self._step_v2(task)
        _, _, cursor, position, done = self._task(task)
        if done:
            self.client.close(cursor)
            return self.status(task)
        rows = []
        for i in range(2):
            row = self.client.row(cursor, position + i)
            if row is None:
                break
            rows.append(row)
        finished = len(rows) < 2
        with self.db:
            if rows:
                n = self.db.execute("SELECT count(*) FROM export_segments WHERE task_id=?", (task,)).fetchone()[0]
                self.db.execute("INSERT INTO export_segments VALUES(?,?,?)", (task, n, json.dumps(rows)))
            self.db.execute("UPDATE export_tasks SET next_row=?,done=? WHERE task_id=?",
                            (position + len(rows), int(finished), task))
            self.hook("step.before_commit")
        self.hook("step.after_commit")
        if finished:
            self.client.close(cursor)
        return self.status(task)

    def _step_v2(self, task):
        _, _, lease, position, done = self._task_v2(task)
        if done:
            finalize_v2(self.db, self.client2)
            return self.status(task)
        batch = self.client2.read(lease, position, 2)
        # The batch is borrowed: this client's next read overwrites it. Take
        # independent ownership before anything else can read again.
        rows = [list(r) for r in batch]
        finished = len(rows) < 2
        with self.db:
            if rows:
                n = self.db.execute("SELECT count(*) FROM segments_v2 WHERE task_id=?", (task,)).fetchone()[0]
                self.db.execute("INSERT INTO segments_v2 VALUES(?,?,?)", (task, n, json.dumps(rows)))
            self.db.execute("UPDATE tasks_v2 SET next_row=?,done=? WHERE task_id=?",
                            (position + len(rows), int(finished), task))
            self.hook("step.before_commit")
        self.hook("step.after_commit")
        if finished:
            # Completion is committed; the task no longer depends on the
            # source. Release now, or finalize_v2 releases on reopen/retry.
            finalize_v2(self.db, self.client2)
        return self.status(task)

    def status(self, task):
        if self._mode() == 2:
            _, snapshot, _, position, done = self._task_v2(task)
            n = self.db.execute("SELECT count(*) FROM segments_v2 WHERE task_id=?", (task,)).fetchone()[0]
            return dict(task_id=task, snapshot=snapshot, next_row=position, done=bool(done), segments=n)
        _, snapshot, _, position, done = self._task(task)
        n = self.db.execute("SELECT count(*) FROM export_segments WHERE task_id=?", (task,)).fetchone()[0]
        return dict(task_id=task, snapshot=snapshot, next_row=position, done=bool(done), segments=n)

    def segments(self, task):
        if self._mode() == 2:
            self._task_v2(task)
            return [json.loads(r[0]) for r in self.db.execute(
                "SELECT payload FROM segments_v2 WHERE task_id=? ORDER BY segment_no", (task,))]
        self._task(task)
        return [json.loads(r[0]) for r in self.db.execute(
            "SELECT payload FROM export_segments WHERE task_id=? ORDER BY segment_no", (task,))]

    def close(self):
        self.db.close()
