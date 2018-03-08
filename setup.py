from setuptools import setup
from pyexasol.version import __version__

setup(
    name='pyexasol',
    version=__version__,
    description='Exasol python driver',
    long_description='Exasol python driver with low overhead, fast HTTP transport and compression.',
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
)
