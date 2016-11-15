import os
import sys

# from avro.datafile import DataFileReader
# from avro.io import DatumReader
from kafka import KafkaProducer

from py_sample.main.traverse import traverse

producer = KafkaProducer(
    bootstrap_servers='174.129.180.214:9092'
)
l_created = []


def send(buf, level):
    producer.send('level_1', value=buf.getvalue(), key=bytes('msg', 'utf-8'))
    # reader = DataFileReader(buf, DatumReader())
    # for msg in reader:
    #     print('Reading: ' + str(msg) + ' on level ' + str(level))
    producer.flush()
    # reader.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('Please, provide the path')
    p = sys.argv[1]
    if not os.path.exists(p):
        raise Exception('Path ' + p + ' does not exist')
    traverse(p, send, producer)
    producer.close()
