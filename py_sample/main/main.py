import os
import sys

from kafka import KafkaProducer

from py_sample.main.traverse import traverse

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)
l_created = []


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('Please, provide the path')
    p = sys.argv[1]
    if not os.path.exists(p):
        raise Exception('Path ' + p + ' does not exist')
    traverse(p, producer)
    producer.flush()
    producer.close()
