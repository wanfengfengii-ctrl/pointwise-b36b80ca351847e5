from source import Source
from exportqueue import Store

ORIGINAL = [["r1", "A", 10], ["r2", "B", None], ["r2", "B", None],
            ["r3", "A", -2], ["r4", "B", 0]]
REPLACEMENT = [["new1", "A", 999], ["new2", "B", 888], ["new3", "A", 777]]

def make(source_path=":memory:", queue_path=":memory:"):
    source = Source(source_path)
    first = source.publish(ORIGINAL)
    queue = Store(queue_path, source)
    all_task = queue.create_export("all")
    queue.step(all_task)
    filtered_task = queue.create_export("station=A")
    empty_task = queue.create_export("station=Z")
    source.publish(REPLACEMENT)
    source.gc()
    return source, queue, dict(all=all_task, filtered=filtered_task, empty=empty_task,
                               original_snapshot=first)
