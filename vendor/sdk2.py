from source import query_text

class Client:
    """Internal batch SDK. Leases persist; returned lists are borrowed."""
    def __init__(self, source):
        self.source = source
        self.buffer = []

    def acquire(self, owner, snapshot, query):
        if not isinstance(owner, str) or not owner:
            raise ValueError("invalid owner")
        query_text(query)
        with self.source.db:
            self.source._check(snapshot)
            old = self.source.db.execute(
                "SELECT sid,query FROM snapshot_leases WHERE owner=?", (owner,)).fetchone()
            if old is not None and old != (snapshot, query):
                raise ValueError("owner conflict")
            self.source.db.execute(
                "INSERT OR IGNORE INTO snapshot_leases VALUES(?,?,?)", (owner, snapshot, query))
        return owner

    def read(self, lease, offset, limit):
        binding = self.source.db.execute(
            "SELECT sid,query FROM snapshot_leases WHERE owner=?", (lease,)).fetchone()
        if binding is None:
            raise LookupError("lease gone")
        rows = self.source._rows(*binding, offset, limit)
        for old in self.buffer:
            old[:] = ["OVERWRITTEN"]
        self.buffer[:] = rows
        return self.buffer

    def release(self, lease):
        with self.source.db:
            self.source.db.execute("DELETE FROM snapshot_leases WHERE owner=?", (lease,))

    def retire_cursor(self, cursor):
        """Compatibility handoff primitive; does not acquire a replacement lease."""
        with self.source.db:
            self.source.db.execute("DELETE FROM legacy_cursors WHERE cid=?", (cursor,))
