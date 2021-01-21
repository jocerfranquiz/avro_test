import json
import logging
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Login config
logging.basicConfig(
    filename='writer.log', 
    filemode='w', 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
    )

# read records from sample_data and schema

raw_data = []
try:
    with open('raw_data.txt','r') as txt:
        for raw_record in txt.readlines():
            record = raw_record.strip()
            raw_data.append(record)
except Exception as err:
    logging.error(err)

# Load schema

try:
    schema = avro.schema.parse(open("schema.avsc").read())
except Exception as err:
    logging.error(err)
    raise

# Write records

try:
    with open('encoded_data.avro', 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), schema)
        for row in raw_data:
            try:
                writer.append(json.loads(row))
            except Exception as err:
                logging.error(err)
                logging.info(row.strip())
        writer.close()
except Exception as err:
    logging.error(err)
    raise
