"""
Provides functions for reading SequenceFile-s with Python pickles. Such files
are usually created with spark.rdd.RDD.saveAsPickleFile().
No PySpark installation is required, no external dependencies.

References:
    https://blog.sourced.tech/post/reading_pyspark_pickles_locally
    https://wiki.apache.org/hadoop/SequenceFile
    http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-common/2.7.1/org/apache/hadoop/io/SequenceFile.java#SequenceFile
    https://www.safaribooksonline.com/library/view/hadoop-the-definitive/9781449328917/ch04.html#id3960971
    https://docs.oracle.com/javase/7/docs/platform/serialization/spec/protocol.html#10258
    http://www.javaworld.com/article/2072752/the-java-serialization-algorithm-revealed.html

:authors: Vadim Markovtsev
:license: Apache License 2.0
:version: 1.0
:status: Alpha

..

    Copyright 2016 source{d}

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

from io import BytesIO
import pickle
import struct

from .javaobj import load as javaobj_load

__all__ = ("load", "load_gen", "loads", "FormatError")


HEADER = b"\x53\x45\x51\x06\x21\x6F\x72\x67\x2E\x61\x70\x61\x63\x68\x65\x2E" \
         b"\x68\x61\x64\x6F\x6F\x70\x2E\x69\x6F\x2E\x4E\x75\x6C\x6C\x57\x72" \
         b"\x69\x74\x61\x62\x6C\x65\x22\x6F\x72\x67\x2E\x61\x70\x61\x63\x68" \
         b"\x65\x2E\x68\x61\x64\x6F\x6F\x70\x2E\x69\x6F\x2E\x42\x79\x74\x65" \
         b"\x73\x57\x72\x69\x74\x61\x62\x6C\x65\x00\x00\x00\x00\x00\x00"


class FormatError(Exception):
    """
    Represents any errors related to sparkpickle.
    """
    pass


def load_gen(file, progress_callback=None):
    """
    Loads all the objects from the specified Spark SequenceFile with pickles
    (generator version of load()).
    The file is expected to be created with saveAsPickleFile() in PySpark.
    All the imported Python classes must be present in the current environment.
    :param file: File object which is open in binary mode ("rb") and
                 must be able to read(), seek() and tell().
    :param progress_callback: Optional callable to report the loading progress.
                              It must accept a single argument which is the
                              current file position.
    :return: The generator object. Every object is yield-ed while reading.

    Example:
    >>> with open("/path/to/file", "rb") as f:
    ...     for obj in sparkpickle.load_gen(f):
    ...         print(obj)
    """
    header = file.read(len(HEADER))
    if header != HEADER:
        raise FormatError("Header validation failed.")
    mark = file.read(16)  # sync mark
    record_flag = None
    while True:
        if record_flag is None and not file.read(4):
            break
        record_flag = None
        if file.read(4) != b"\x00\x00\x00\x00":
            raise FormatError("Record validation failed.")
        object_size = file.read(4)
        try:
            object_size = struct.unpack(">I", object_size)[0]
        except ValueError:
            raise FormatError("Failed to parse BytesWritable.")
        object_start_pos = file.tell()
        batches = []

        def callback(_, size):
            pos = file.tell()
            batches.append(pickle.load(file))
            if file.tell() - pos != size:
                raise FormatError("Object stream parsing integrity error.")
            if progress_callback is not None:
                progress_callback(pos + size)

        javaobj_load(file, ignore_remaining_data=True, bytes_callback=callback)
        if file.tell() - object_start_pos != object_size:
            raise FormatError("Object stream parsing integrity error.")
        for batch in batches:
            for obj in batch:
                yield obj
        del batches[:]
        probe = file.read(4)
        if probe == b"\xFF\xFF\xFF\xFF":
            if file.read(16) != mark:
                raise FormatError("Object stream parsing integrity error.")
        elif not probe:
            break
        else:
            record_flag = probe


def load(file, progress_callback=None):
    """
    Loads all the objects from the specified Spark SequenceFile with pickles.
    The file is expected to be created with saveAsPickleFile() in PySpark.
    All the imported Python classes must be present in the current environment.
    :param file: File object which is open in binary mode ("rb") and
                 must be able to read(), seek() and tell().
    :param progress_callback: Optional callable to report the loading progress.
                              It must accept a single argument which is the
                              current file position.
    :return: The list with the loaded objects. Internal batches are flattened.
    """
    return list(load_gen(file, progress_callback=progress_callback))


def loads(buffer, progress_callback=None):
    """
    Loads all the objects from the specified Spark SequenceFile with pickles.
    The file is expected to be created with saveAsPickleFile() in PySpark.
    All the imported Python classes must be present in the current environment.
    :param buffer: The contents of the file (bytes).
    :param progress_callback: Optional callable to report the loading progress.
                              It must accept a single argument which is the
                              current file position.
    :return: The list with the loaded objects. Internal batches are flattened.
    """
    return load(BytesIO(buffer), progress_callback=progress_callback)
