import uuid
from source import query_text

class Client:
    """Internal row SDK: durable cursor identity implicitly retains a snapshot."""
    def __init__(self, source):
        self.source = source

    def open(self, query):
        query_text(query)
        cid = uuid.uuid4().hex
        sid = self.source.current()
        with self.source.db:
            self.source.db.execute("INSERT INTO legacy_cursors VALUES(?,?,?)", (cid, sid, query))
        return cid, sid

    def row(self, cursor, position):
        item = self.source.db.execute(
            "SELECT sid,query FROM legacy_cursors WHERE cid=?", (cursor,)).fetchone()
        if item is None:
            raise LookupError("cursor gone")
        rows = self.source._rows(*item, position, 1)
        return rows[0] if rows else None

    def close(self, cursor):
        with self.source.db:
            self.source.db.execute("DELETE FROM legacy_cursors WHERE cid=?", (cursor,))
