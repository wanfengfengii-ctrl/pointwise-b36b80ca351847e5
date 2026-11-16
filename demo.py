import json
import os
import tempfile

from samples import make, ORIGINAL
import migration


def run():
    tmp = tempfile.mkdtemp(prefix="exportq-demo-")
    queue_path = os.path.join(tmp, "queue.db")
    source, queue, tasks = make(os.path.join(tmp, "source.db"), queue_path)
    task = tasks["all"]
    print(json.dumps(dict(paused=queue.status(task), segments=queue.segments(task),
                          current_snapshot=source.current(),
                          protections=source.protections()), ensure_ascii=False))
    # maintenance window: upgrade the live queue file from SDK1 cursors to SDK2 leases
    migration.upgrade(queue_path, source)
    print(json.dumps(dict(upgraded=True, protections=source.protections()),
                     ensure_ascii=False))
    # The source table was replaced after the tasks were created, and this Store
    # object was opened before the upgrade; the paused tasks still continue from
    # their creation snapshot, now through SDK2 lease batch reads.
    for name in ("all", "filtered", "empty"):
        while not queue.status(tasks[name])["done"]:
            queue.step(tasks[name])
    continued = [row for segment in queue.segments(task) for row in segment]
    print(json.dumps(dict(completed=queue.status(task), segments=queue.segments(task),
                          continued_rows=continued,
                          from_replaced_table=continued == ORIGINAL
                          and source.current() != tasks["original_snapshot"],
                          protections=source.protections(), collected=source.gc()),
                     ensure_ascii=False))
    queue.close()
    source.close()


if __name__ == "__main__":
    run()
