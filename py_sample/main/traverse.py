import io
import os

import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from kafka.errors import KafkaError

from py_sample.item import Item

sch = avro.schema.Parse(open('../../message.avsc').read())


def traverse(path: str, producer):
    sp = path.split(sep='/')
    lp = len(sp)
    for root, dirs, files in os.walk(path, topdown=True):
        sr = root.split('/')
        level = len(sr) - lp
        prepare(producer, dirs, root, level)
        prepare(producer, files, root, level)


def prepare(producer, arr, root, level):
    for it in arr:
        buf = io.BytesIO()
        writer = DataFileWriter(buf, DatumWriter(), sch)
        item = Item(root, it, False)
        writer.append(item.get_dict())
        writer.flush()
        send(buf, level, producer)


def send(buf, level, producer):
    lv = 'level_' + str(level)
    future = producer.send(
        topic=lv,
        value=buf.getvalue(),
        key=bytes('msg', 'utf-8')
    )
    # await async send
    try:
        metadata = future.get(timeout=1000)
        print(metadata)
    except KafkaError:
        print("Couldn't send message to level " + str(lv))
