import io
import os

import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from py_sample.item import Item


def traverse(path: str, func, producer=None):
    sp = path.split(sep='/')
    lp = len(sp)
    sch = avro.schema.Parse(open('../../message.avsc').read())
    for root, dirs, files in os.walk(path, topdown=True):
        sr = root.split('/')
        buf = io.BytesIO()
        writer = DataFileWriter(buf, DatumWriter(), sch)

        level = len(sr) - lp
        prepare(dirs, writer, root)
        prepare(files, writer, root)
        writer.flush()
        if producer is None:
            func(buf, level)
        else:
            func(buf, level)


def prepare(arr, writer, root):
    for it in arr:
        item = Item(root, it, False)
        writer.append(item.get_dict())
