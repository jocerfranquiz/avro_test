import json
import logging
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

logging.basicConfig(
    filename='read.log', 
    filemode='w', 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
    )


try:
    schema = avro.schema.parse(open("schema.avsc").read())
except Exception as err:
    logging.error(err)
    raise

# read only good records

try:
    reader = DataFileReader(open("encoded_data.avro", "rb"), DatumReader())
    for row in reader:
        try:
            print(row)
        except Exception as err:
            logging.error(err)
            logging.info(row.strip())
    reader.close()
except Exception as err:
    logging.error(err)
    

