"""
Benchmarks for Postgres Round Trip Client-Side Copy
---------------------------------------------------

Functions for talk at PGCon 2017.
"""


from setuptools import setup, find_packages

setup(
    name='pyrtcbench',
    version='0.1',
    url='https://github.com/dgerlanc/pyrtcbench',
    license='(c) 2017 Daniel J. Gerlanc',
    author='Daniel Gerlanc',
    author_email='dgerlanc@enplusadvisors.com',
    description='Benchmarks for Postgres Round Trip Client-Side Copy',
    long_description=__doc__,
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]
    ),
    zip_safe=True,
    include_package_data=True,
    install_requires=[
      'click',
      'psycopg2'
    ],
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
