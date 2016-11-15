import os
import sys
import io
import uuid
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from kafka import KafkaProducer
from py_sample.item import Item
from py_sample.main.traverse import traverse

producer = KafkaProducer(
    bootstrap_servers='174.129.180.214:9092'
)
l_created = []


def send(buf, level):
    reader = DataFileReader(buf, DatumReader())
    for msg in reader:
        print('Reading: ' + str(msg) + ' on level ' + str(level))
    reader.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('Please, provide the path')
    p = sys.argv[1]
    if not os.path.exists(p):
        raise Exception('Path ' + p + ' does not exist')
    traverse(p, send)
