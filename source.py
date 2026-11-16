import json
import sqlite3
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots(sid TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS current_snapshot(slot INTEGER PRIMARY KEY CHECK(slot=1),sid TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_rows(sid TEXT NOT NULL,pos INTEGER NOT NULL,rid TEXT NOT NULL,station TEXT NOT NULL,value INTEGER,PRIMARY KEY(sid,pos));
CREATE TABLE IF NOT EXISTS legacy_cursors(cid TEXT PRIMARY KEY,sid TEXT NOT NULL,query TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshot_leases(owner TEXT PRIMARY KEY,sid TEXT NOT NULL,query TEXT NOT NULL);
"""

def query_text(query):
    if query == "all":
        return query
    if isinstance(query, str) and query.startswith("station=") and len(query) > 8:
        return query
    raise ValueError("invalid query")

class Source:
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)

    def publish(self, rows):
        rows = list(rows)
        for row in rows:
            if (not isinstance(row, (list, tuple)) or len(row) != 3
                    or not isinstance(row[0], str) or not row[0]
                    or not isinstance(row[1], str) or not row[1]
                    or (row[2] is not None and type(row[2]) is not int)):
                raise ValueError("invalid row")
        sid = uuid.uuid4().hex
        with self.db:
            self.db.execute("INSERT INTO snapshots VALUES(?)", (sid,))
            self.db.executemany("INSERT INTO source_rows VALUES(?,?,?,?,?)",
                                [(sid, i, *row) for i, row in enumerate(rows)])
            self.db.execute("INSERT OR REPLACE INTO current_snapshot VALUES(1,?)", (sid,))
        return sid

    def current(self):
        row = self.db.execute("SELECT sid FROM current_snapshot WHERE slot=1").fetchone()
        if row is None:
            raise LookupError("no current snapshot")
        return row[0]

    def _check(self, sid):
        if not self.db.execute("SELECT 1 FROM snapshots WHERE sid=?", (sid,)).fetchone():
            raise LookupError("snapshot gone")

    def _rows(self, sid, query, offset, limit):
        self._check(sid)
        query_text(query)
        if type(offset) is not int or offset < 0 or type(limit) is not int or limit < 1:
            raise ValueError("invalid position or limit")
        sql = "SELECT rid,station,value FROM source_rows WHERE sid=?"
        args = [sid]
        if query != "all":
            sql += " AND station=?"
            args.append(query[8:])
        sql += " ORDER BY pos LIMIT ? OFFSET ?"
        return [list(r) for r in self.db.execute(sql, (*args, limit, offset))]

    def gc(self):
        with self.db:
            dead = [r[0] for r in self.db.execute(
                "SELECT sid FROM snapshots WHERE sid NOT IN (SELECT sid FROM current_snapshot) "
                "AND sid NOT IN (SELECT sid FROM legacy_cursors) "
                "AND sid NOT IN (SELECT sid FROM snapshot_leases)")]
            for sid in dead:
                self.db.execute("DELETE FROM source_rows WHERE sid=?", (sid,))
                self.db.execute("DELETE FROM snapshots WHERE sid=?", (sid,))
        return sorted(dead)

    def protections(self):
        return {
            "legacy": [list(r) for r in self.db.execute("SELECT cid,sid FROM legacy_cursors ORDER BY cid")],
            "leases": [list(r) for r in self.db.execute("SELECT owner,sid FROM snapshot_leases ORDER BY owner")]
        }

    def close(self):
        self.db.close()
