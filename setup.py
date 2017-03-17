#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    'psycopg2',
    'numpy',
    'pandas',
    'PyYAML',
    'dedupe',
    'dedupe-variable-name',
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='superdeduper',
    version='0.1.2',
    description="A simple interface to datamade/dedupe to make probabilistic record linkage easy.",
    long_description=readme + '\n\n' + history,
    author="DSaPP Researchers",
    author_email='datascifellows@gmail.com',
    url='https://github.com/dssg/superdeduper',
    packages=[
        'superdeduper',
    ],
    package_dir={'superdeduper':
                 'superdeduper'},
    entry_points={
        'console_scripts': [
            'superdeduper=superdeduper.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='superdeduper',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
