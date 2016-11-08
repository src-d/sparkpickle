import os
import sys
import unittest

import sparkpickle


class SparkPickleTests(unittest.TestCase):
    def test_load(self):
        with open(os.path.join(os.path.dirname(__file__),
                               "test.%d.bin" % sys.version_info[0]),
                  "rb") as fin:
            objs = sparkpickle.load(fin)
        self.assertEqual(objs, list(range(200)))

if __name__ == "__main__":
    unittest.main()
