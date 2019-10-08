from collections import OrderedDict

import fastavro
from genson import SchemaBuilder


JSON_TO_AVRO_TYPES = {
        "null" : "null",
        "boolean" : "boolean",
        "integer" : "int",
        "number" : "double",
        "string" : "string",
        "object" : "record",
        "array" : "array",
        }


def _avro_type(json_type):
    if isinstance(json_type, list):
        return [_avro_type(jt) for jt in json_type]
    return JSON_TO_AVRO_TYPES.get(json_type)


def _json_to_avro_schema(json_schema, field_names=None, name="Root"):
    if json_schema["type"] == "object":
        if "properties" not in json_schema:
            return {"type" : "record", "name": name}
        fields = []
        for prop in json_schema["properties"]:
            avro_type = _json_to_avro_schema(json_schema["properties"][prop],
                    "{}_type".format(prop))
            if isinstance(avro_type, list):
                avro_type = ["null",] + avro_type
            else:
                avro_type = ["null", avro_type]
            fields.append({"name": prop, "type": avro_type})
        if field_names is not None:
            field_order = dict((field, i)
                    for i, field in enumerate(field_names))
            fields = sorted(fields, key=lambda f: field_order.get(f["name"]))
        return {"type": "record", "name": name, "fields": fields}
    elif json_schema["type"] == "array":
        return {
                "type" : "array",
                "items": _json_to_avro_schema(
                    json_schema["items"],
                    "{}_item_type".format(name),
                    ),
                }
    return _avro_type(json_schema["type"])


def _infer_schema(records, field_names=None):
    builder = SchemaBuilder()
    count = 0
    for record in records:
        builder.add_object(record)
        count += 1
    if count == 0:
        return {
                "type": "record",
                "name" : "Root",
                }
    #print(builder.to_json(indent=2))
    schema = _json_to_avro_schema(builder.to_schema(), field_names)
    #print(json.dumps(schema, indent=True))
    return schema


def _is_iterator(obj):
    if hasattr(obj, '__iter__') and \
            hasattr(obj, '__next__') and \
            callable(obj.__iter__) and \
            obj.__iter__() is obj:
        return True
    else:
        return False


class JSON2AvroRecords(object):
    """A wrapper reader class for reading JSON data (deserialized as Python
    dict objects) with schema inference."""

    def __init__(self, json_records, field_names=None, head=25000):
        """Initializes from reading JSON records with schema
        inference.

        Args:
            json_records: an iterator of JSON records (deserialized as Python
                dict objects).
            field_names: a list of field names used to order the Avro fields,
                optional.
            head: the number of records in the beginning to use for schema
                inference.
        """
        if not _is_iterator(json_records):
            raise ValueError("json_records must be an iterator.")
        self._json_records = json_records
        self._head = [r for _, r in zip(range(head), self._json_records)]
        self._schema = _infer_schema(self._head, field_names)

    @property
    def schema(self):
        return self._schema

    def get(self):
        for record in self._head:
            yield record
        for record in self._json_records:
            yield record


def avro2json(fileobj_binary):
    reader = fastavro.reader(fileobj_binary)
    schema = reader.writer_schema
    field_names = [f["name"] for f in schema["fields"]]
    for record in reader:
        yield OrderedDict((f, record[f]) for f in field_names)

