from setuptools import setup
import pathlib

__version__ = None
version_path = pathlib.Path(__file__).parent / 'pyexasol/version.py'
exec(version_path.read_text())

setup(
    name='pyexasol',
    version=__version__,
    description='Exasol python driver with extra features',
    long_description="""
Exasol python driver with low overhead, fast HTTP transport and compression.

Main features:

-  Based on WebSocket protocol;
-  Optimized for minimum overhead;
-  Easy integration with pandas via HTTP transport;
-  Compression to reduce network bottleneck;

PyEXASOL requires Python 3.6+.

Driver does not use ODBC. Driver does not strictly follow DB-API 2.0 in favor of Exasol-specific features.

Please read "Best practices" manual page to learn how to use PyEXASOL with maximum efficiency.
    """,
    long_description_content_type='text/markdown',
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

    extras_require={
        'pandas': ['pandas>=0.22,!=0.23.1'],
        'encrypt': ['pyopenssl>=17.5.0'],
        'ujson': ['ujson'],
        'rapidjson': ['python-rapidjson'],
        'pproxy': ['pproxy'],
    },

    python_requires='>=3.6',
)
