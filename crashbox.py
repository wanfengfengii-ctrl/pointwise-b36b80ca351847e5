"""Subprocess helper for kill/reopen tests at the public commit points.

Dies hard via os._exit(42) when the chosen hook event fires for the Nth time,
simulating process termination: no cleanup, no response to the caller.

Usage:
  crashbox.py make <source> <queue>                     build the sample, print task ids
  crashbox.py upgrade <source> <queue> <event> <count>  run migration.upgrade with kill hook
  crashbox.py step <source> <queue> <event> <count> <task_id>...
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from source import Source
from exportqueue import Store
from samples import make
import migration

KILL_CODE = 42


def kill_hook(event, count):
    state = {"seen": 0}

    def hook(ev):
        if ev == event:
            state["seen"] += 1
            if state["seen"] == count:
                os._exit(KILL_CODE)

    return hook


def main(argv):
    mode = argv[1]
    if mode == "make":
        source, queue, tasks = make(argv[2], argv[3])
        print(json.dumps(tasks))
        queue.close()
        source.close()
        return 0
    if mode == "upgrade":
        source = Source(argv[2])
        migration.upgrade(argv[3], source, hook=kill_hook(argv[4], int(argv[5])))
        source.close()
        print("upgrade-returned")
        return 0
    if mode == "step":
        source = Source(argv[2])
        store = Store(argv[3], source)
        store.hook = kill_hook(argv[4], int(argv[5]))
        for task_id in argv[6:]:
            while not store.status(task_id)["done"]:
                store.step(task_id)
        print("steps-returned")
        return 0
    raise SystemExit("unknown mode " + mode)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
