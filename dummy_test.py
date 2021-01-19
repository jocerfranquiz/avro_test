from io import BytesIO
from fastavro import schemaless_reader, parse_schema, schemaless_writer
import json

try:
    with open('schema.avsc','r') as f:
        schema = json.loads(f.read())

except Exception as err:
    print(err)
    
event = {"name": "Rea", "favorite_number": 256}

rb = BytesIO()
schemaless_writer(rb, schema, event)
print(rb.getvalue())  # b'\x06Rea\x80\x04'

rb = BytesIO(b'\x06Rea\x80\x04')
data = schemaless_reader(rb, schema)
print(data)
