import copy
import json
import sys
from samples import make, ORIGINAL
from vendor.sdk2 import Client

def run():
    source, queue, tasks = make()
    task = tasks["all"]
    assert queue.status(task)["next_row"] == 2
    assert queue.segments(task) == [ORIGINAL[:2]]
    assert source.current() != tasks["original_snapshot"]
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
    for name in ("all", "filtered", "empty"):
        while not queue.status(tasks[name])["done"]:
            queue.step(tasks[name])
    actual = [r for segment in queue.segments(task) for r in segment]
    assert actual == ORIGINAL
    assert queue.segments(tasks["filtered"]) == [[ORIGINAL[0], ORIGINAL[3]]]
    assert queue.segments(tasks["empty"]) == []
    again = queue.status(task)
    assert queue.step(task) == again
    assert source.protections() == dict(legacy=[], leases=[])

    current = source.current()
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
                          protections=source.protections(), sdk2_saved=saved), ensure_ascii=False))
    queue.close()
    source.close()

if __name__ == "__main__":
    run()
