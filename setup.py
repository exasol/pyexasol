import io
import re

from setuptools import setup

with io.open('pyexasol/version.py', 'rt', encoding='utf8') as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)

setup(
    name='pyexasol',
    version=version,
    description='Exasol python driver with extra features',
    long_description="""
Exasol python driver with low overhead, fast HTTP transport and compression.

Main features
------

-  Based on WebSocket client-server protocol;
-  Optimized for minimum overhead;
-  Easy integration with pandas via HTTP transport;
-  Compression to reduce network bottleneck;

PyEXASOL requires Python 3.6+.

Driver does not use ODBC. Driver does not strictly follow DB-API 2.0 in favor of Exasol-specific features.

Please read "Best practices" manual page to learn how to use PyEXASOL with maximum efficiency.
    """,
    url='https://github.com/badoo/pyexasol',
    author='Vitaly Markov',
    author_email='wild.desu@gmail.com',

    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database',
    ],

    keywords='exasol sql database performance websocket import export',

    packages=[
        'pyexasol',
        'pyexasol.db2',
    ],

    install_requires=[
        'websocket-client>=0.47.0',
        'rsa',
    ],

    python_requires='>=3.6',
)
