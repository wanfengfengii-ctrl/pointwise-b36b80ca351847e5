import json
import sqlite3
import uuid
from vendor import sdk1

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_meta(slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL);
INSERT OR IGNORE INTO queue_meta VALUES(1,1);
CREATE TABLE IF NOT EXISTS export_tasks(task_id TEXT PRIMARY KEY,query TEXT NOT NULL,snapshot TEXT NOT NULL,cursor TEXT NOT NULL,next_row INTEGER NOT NULL,done INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS export_segments(task_id TEXT NOT NULL,segment_no INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(task_id,segment_no));
CREATE TABLE IF NOT EXISTS export_audit(seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL,raw TEXT NOT NULL);
"""

class Store:
    def __init__(self, path, source):
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        self.source = source
        self.client = sdk1.Client(source)
        self.hook = lambda event: None

    def create_export(self, query):
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

    def _task(self, task):
        row = self.db.execute("SELECT query,snapshot,cursor,next_row,done FROM export_tasks WHERE task_id=?",
                              (task,)).fetchone()
        if row is None:
            raise KeyError(task)
        return row

    def step(self, task):
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

    def status(self, task):
        _, snapshot, _, position, done = self._task(task)
        n = self.db.execute("SELECT count(*) FROM export_segments WHERE task_id=?", (task,)).fetchone()[0]
        return dict(task_id=task, snapshot=snapshot, next_row=position, done=bool(done), segments=n)

    def segments(self, task):
        self._task(task)
        return [json.loads(r[0]) for r in self.db.execute(
            "SELECT payload FROM export_segments WHERE task_id=? ORDER BY segment_no", (task,))]

    def close(self):
        self.db.close()
