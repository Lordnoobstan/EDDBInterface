import jsonschema
import json
import os

schemas = {}

for _, _, schema_filenames in os.walk("schemas"):
    for schema_filename in schema_filenames:
        with open(f"schemas/{schema_filename}") as schema_file:
            schema_json = json.load(schema_file)

            schemas[schema_json["id"]] = schema_json


def validate_message(message_json: json) -> tuple[bool, str]:
    try:
        jsonschema.validate(instance=message_json, schema=schemas[message_json["$schemaRef"]])
    except jsonschema.ValidationError as e:
        return False, e.message

    return True, ""
