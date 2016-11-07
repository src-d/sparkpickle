from setuptools import setup


setup(
    name="sparkpickle",
    description="Provides functions for reading SequenceFile-s with Python "
                "pickles.",
    version="1.0.0",
    license="Apache 2.0",
    author="Vadim Markovtsev",
    author_email="vadim@sourced.tech",
    url="https://github.com/src-d/sparkpickle",
    download_url='https://github.com/src-d/sparkpickle',
    packages=["sparkpickle"],
    package_dir={"sparkpickle": "."},
    keywords=["spark", "pyspark", "hadoop", "rdd", "pickle"],
    install_requires=[],
    package_data={"": ["LICENSE", "README.md"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Software Development :: Libraries"
    ]
)