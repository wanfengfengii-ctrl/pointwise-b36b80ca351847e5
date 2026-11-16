import json
from samples import make

def run():
    source, queue, tasks = make()
    task = tasks["all"]
    print(json.dumps(dict(paused=queue.status(task), segments=queue.segments(task),
                          current_snapshot=source.current()), ensure_ascii=False))
    for name in ("all", "filtered", "empty"):
        while not queue.status(tasks[name])["done"]:
            queue.step(tasks[name])
    print(json.dumps(dict(completed=queue.status(task), segments=queue.segments(task),
                          protections=source.protections(), collected=source.gc()),
                     ensure_ascii=False))
    queue.close()
    source.close()

if __name__ == "__main__":
    run()
