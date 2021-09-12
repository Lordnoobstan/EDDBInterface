import json
import queue
import zlib
import zmq

import loggers
import schema_validation
import sql

with open("config.json") as config_file:
    queue_worker_thread_count = json.load(config_file)["Worker Thread Count"]

relay = "tcp://eddn.edcd.io:9500"

context = zmq.Context()

socket = context.socket(zmq.SUB)
socket.connect(relay)
socket.set(zmq.SUBSCRIBE, b"")

system_logging_queue = queue.Queue() # 1000)


def handle_task(task_name: str, message_json: json) -> None:
    is_valid_message, error = schema_validation.validate_message(message_json)

    status = ""
    meta_message = ["An unknown error occurred"]
    system_of_interest = ""
    body_of_interest = ""

    if is_valid_message:
        function = None

        if task_name == "Commodity":
            function = loggers.handle_commodity
        elif task_name == "journal/FSDJump":
            function = loggers.handle_fsd_jump_journal
        elif task_name == "journal/Location":
            function = loggers.handle_location_journal
        elif task_name == "journal/Scan":
            function = loggers.handle_scan_journal
        else:
            status, meta_message = "Ignored", ["Invalid task name provided"]
            print(f"Got invalid task name {task_name}")

        if function is not None:
            status, meta_message, system_of_interest, body_of_interest = function(message_json, sql.connection_pool)
    else:
        status, meta_message = "Ignored", [f"{error}"]
        print(f"Schema rejected: {error}")

    sql.insert_log_row(status, meta_message, task_name, str(message_json), system_of_interest, body_of_interest,
                       pool=sql.connection_pool)


def log_queue_worker() -> None:
    while True:
        if not system_logging_queue.empty():
            task_name, message_json = system_logging_queue.get()

            handle_task(task_name, message_json)


def run_socket() -> None:
    try:
        while True:
            if not system_logging_queue.full():
                message_binary = bytes(socket.recv())
                message_text = zlib.decompress(message_binary)
                message_json = json.loads(message_text)

                if message_json["$schemaRef"] == "https://eddn.edcd.io/schemas/commodity/3":
                    system_logging_queue.put(("Commodity", message_json))
                elif message_json["$schemaRef"] == "https://eddn.edcd.io/schemas/journal/1":
                    payload = message_json["message"]

                    event_type = payload["event"]

                    if event_type == "FSDJump":
                        system_logging_queue.put(("journal/FSDJump", message_json))
                    elif event_type == "Location":
                        system_logging_queue.put(("journal/Location", message_json))
                    elif event_type == "Scan":
                        system_logging_queue.put(("journal/Scan", message_json))
    except zmq.ZMQError:
        socket.disconnect(relay)
