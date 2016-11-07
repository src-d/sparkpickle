[![Build Status](https://travis-ci.org/src-d/sparkpickle.svg?branch=master)](https://travis-ci.org/src-d/sparkpickle) [![PyPI](https://img.shields.io/pypi/v/sparkpickle.svg)](https://pypi.python.org/pypi/sparkpickle)

SparkPickle
===========

Pure Python implementation of reading SequenceFile-s with pickles written by
Spark's [saveAsPickleFile()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.saveAsPickleFile).
This is needed if you store the results from Spark in the efficient binary pickle
format and want to load them locally on your computer, without any Spark installation,
given only the actual files.

[Article about creating this project.](https://blog.sourced.tech/post/reading_pyspark_pickles_locally)

Installation
------------
```
pip install sparkpickle
```
Supports Python 2.7 and 3.x.

Usage
-----
View the contents of the file via command line:
```
python -m sparkpickle /path/to/file
```

Code:
```
import sparkpickle

for obj in sparkpickle.load_gen("/path/to/file"):
    print(obj)
```

API
---
There are 3 functions: `load()`, `loads()` and `load_gen()`. The first two
are similar to those found in "pickle" package, whereas the last one is the
generator which yields deserialized objects and thus provides the minimal
memory footprint.

License
-------
Apache 2.0.
