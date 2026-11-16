"""Maintenance-window upgrade of the export queue from SDK1 cursors to SDK2 leases.

Single worker, no other writers or in-flight steps while upgrading. The switch
is atomic on the queue database; source protection is handed off around it so
the creation snapshot of every unfinished task is never left unprotected:

  phase 1  acquire a durable SDK2 lease (owner export/<task_id>) for every
           unfinished task, while its legacy cursor still protects the snapshot
  phase 2  one queue transaction: create the v2 schema, copy tasks (progress
           included) and segment payload bytes verbatim, set version=2
  phase 3  retire the tasks' own legacy cursors through SDK2

Failure before the phase-2 commit rolls the queue change back and leaves the
legacy library usable; leases already acquired may remain and are reused
idempotently by the retry. A kill between the commit and phase 3 is finished
by finalize_v2 on the next Store open or upgrade retry. Other users' cursors
and leases are never touched.
"""

import sqlite3

from exportqueue import V2_SCHEMA, finalize_v2, lease_name
from vendor import sdk2


def upgrade(queue_path, source, hook=None):
    hook = hook or (lambda event: None)
    client = sdk2.Client(source)
    db = sqlite3.connect(queue_path)
    try:
        row = db.execute("SELECT version FROM queue_meta WHERE slot=1").fetchone()
        if row is None:
            raise LookupError("not an export queue")
        if row[0] >= 2:
            # Already switched: only finish an interrupted protection handoff.
            finalize_v2(db, client)
            return None
        tasks = db.execute(
            "SELECT task_id,query,snapshot,cursor,next_row,done "
            "FROM export_tasks ORDER BY task_id").fetchall()
        # Phase 1: lease before releasing anything (source commits, idempotent).
        for task_id, query, snapshot, cursor, next_row, done in tasks:
            if not done:
                client.acquire(lease_name(task_id), snapshot, query)
        # Phase 2: atomic queue switch.
        db.execute("BEGIN IMMEDIATE")
        try:
            for stmt in V2_SCHEMA:
                db.execute(stmt)
            for task_id, query, snapshot, cursor, next_row, done in tasks:
                lease = lease_name(task_id) if not done else None
                db.execute("INSERT INTO tasks_v2 VALUES(?,?,?,?,?,?,?)",
                           (task_id, query, snapshot, lease, cursor, next_row, done))
            db.execute(
                "INSERT INTO segments_v2(task_id,segment_no,payload) "
                "SELECT task_id,segment_no,payload FROM export_segments")
            db.execute("UPDATE queue_meta SET version=2 WHERE slot=1")
            hook("upgrade.before_commit")
            db.commit()
        except BaseException:
            db.rollback()
            raise
        hook("upgrade.after_commit")
        # Phase 3: handoff completion (also runs on reopen/retry if killed here).
        finalize_v2(db, client)
        return None
    finally:
        db.close()
