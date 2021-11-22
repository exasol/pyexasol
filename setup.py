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
Exasol driver with low overhead, fast HTTP transport and compression. It is implemented in Python 3.6+.

Main features:

-  Based on WebSocket protocol;
-  Optimized for minimum overhead;
-  Easy integration with pandas via HTTP transport;
-  Compression to reduce network bottleneck;

PyEXASOL does not follow DB-API 2.0 in favor of Exasol-specific features.

Please read "Best practices" manual page to learn how to use PyEXASOL with maximum efficiency.
    """,
    long_description_content_type='text/markdown',
    url='https://github.com/exasol/pyexasol',
    author='Vitaly Markov',
    author_email='wild.desu@gmail.com',

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Database',
    ],

    keywords='exasol sql database performance websocket import export',

    packages=[
        'pyexasol',
        'pyexasol.db2',
        'pyexasol_utils',
    ],

    install_requires=[
        'websocket-client>=1.0.1',
        'pyopenssl',
        'rsa',
    ],

    extras_require={
        'pandas': ['pandas'],
        'ujson': ['ujson'],
        'rapidjson': ['python-rapidjson'],
        'orjson': ['orjson>=3.6'],
        'examples': ['pproxy'],
    },

    python_requires='>=3.6',
)
