import os
import sys
import io
import uuid
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='174.129.180.214:9092'
)
l_created = []


class Item:
    def __init__(self, root: str, name: str, folderish: bool):
        self.path = root + '/' + name
        self.name = name
        self.folderish = folderish


def traverse(path: str):
    sp = path.split(sep='/')
    lp = len(sp)
    sch = avro.schema.Parse(open('../message.avsc').read())
    for root, dirs, files in os.walk(path, topdown=True):
        sr = root.split('/')
        buf = io.BytesIO()
        writer = DataFileWriter(buf, DatumWriter(), sch)

        level = len(sr) - lp
        prepare(dirs, writer, root)
        prepare(files, writer, root)
        writer.flush()
        if buf.getbuffer().nbytes > 0:
            send(buf, level)


def send(buf, level):
    reader = DataFileReader(buf, DatumReader())
    for msg in reader:
        print('Reading: ' + str(msg) + ' on level ' + str(level))
    reader.close()


def prepare(arr, writer, root):
    for it in arr:
        item = Item(root, it, False)
        t = 'Folder' if item.folderish else 'File'
        writer.append({
            'title': item.name,
            'path': item.path,
            'folderish': item.folderish,
            'properties': {

            },
            'hash': str(uuid.uuid1()),
            'parent': 'parent',
            'type': t,
        })


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception('Please, provide the path')
    p = sys.argv[1]
    if not os.path.exists(p):
        raise Exception('Directory ' + p + ' does not exist')
    traverse(p)
