import os
import unittest

from avro.datafile import DataFileReader
from avro.io import DatumReader

from py_sample.item import Item
from py_sample.main.traverse import traverse

dir_path = os.path.dirname(os.path.realpath(__file__))


def func(buf, level):
    assert buf.getbuffer().nbytes > 0

def func_read(buf, level):
    reader = DataFileReader(buf, DatumReader())
    for msg in reader:
        assert msg is not None
    reader.close()

class AvroTestSuit(unittest.TestCase):
    """ Testing Avro parser """

    def test_avro_append(self):
        traverse(dir_path, func)

    def test_avro_read(self):
        traverse(dir_path, func_read)


if __name__ == '__main__':
    unittest.main()
