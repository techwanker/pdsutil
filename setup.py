#!/usr/bin/env python
# -- Content-Encoding: UTF-8 --
from setuptools import setup
import unittest
"""
Installation script

:authors:   Jim Schmidt
:copyright: Copyright 2017, Jim Schmidt
:license: Apache License 2.0
:version: 0.3.0

..

    Copyright 2017 Jim Schmidt

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
#https://docs.python.org/2/distutils/setupscript.html
# Module version
__version_info__ = (0, 0, 3)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# ------------------------------------------------------------------------------

setup(
    name="pdsutil",
    version=__version__,
    license="Apache License 2.0",
    author="Jim Schmidt",
    author_email="james.joseph.schmidt+github@gmail.com",
    url="http://github.com/pacificdataservices/",
    description=
    "This project supports dataset object and support extensions for"
    "sqlite and postgres.",
    long_description=open("docs/README.rst").read(),
    packages=["pdsutil"],
    classifiers=[
        'Development Status :: 5 - Experimental/Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        #'Programming Language :: Python :: 2.7',
        #'Programming Language :: Python :: 3',
        #'Programming Language :: Python :: 3.3',
        #'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'],
    setup_requires=['pytest-runner'],
    tests_require=['PyTest'],
    package_dir = {'pdsutil': '.'},
    test_suite='tests',
)


# def test_suite():
#     test_loader = unittest.TestLoader()
#     test_suite = test_loader.discover('tests', pattern='*.py')
#     return test_suite