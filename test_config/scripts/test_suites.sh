#!/usr/bin/env bash

#python3 -W ignore:ResourceWarning -m unittest optimization_platform/tests/test_sample.py
#python3 -W ignore:ResourceWarning -m unittest optimization_platform/tests/test_s3_data_store.py
#python3 -W ignore:ResourceWarning -m unittest
coverage run -m unittest
#coverage run -m unittest tests/test_server.py
#coverage report --omit="*/lib*,*__init__.py"
coverage html --omit="*/lib*,*__init__.py"
#coverage report --omit="*/lib*,*__init__.py"