import json
from threading import Thread

from socketer import run_socket, log_queue_worker
from sql import create_systems_table

with open("config.json") as config_file:
    queue_worker_thread_count = json.load(config_file)["Worker Thread Count"]


"""
Various journal events are used to log the creation of several bodies. Here is a general breakdown of these events:
    journal/FSDJump - Fired when a player arrives from an FSD jump. Used to log system info.
    journal/Location - Fired when a player is revived, taken into custody, or loads a game. Used to log body info.
    journal/Scan - Fired when a scan is run implicitly or explicitly. Used to log body info.
    Commodity - Fired when commodity data is opened. Used to log commodity prices.
"""


def run():
    workers = [
        {
            "function": run_socket,
            "count": 1
        },
        {
            "function": log_queue_worker,
            "count": queue_worker_thread_count
        }
    ]

    create_systems_table()

    processes = []

    for worker in workers:
        for _ in range(worker["count"]):
            process = Thread(target=worker["function"])
            process.start()
            processes.append(process)

    for process in processes:
        process.join()


if __name__ == "__main__":
    run()
