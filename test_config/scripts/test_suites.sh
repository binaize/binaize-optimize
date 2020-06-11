#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# keep the container up for debugging for local development
# --------------------------------------------------------------------------------------------------

#touch /tmp/a.txt
#tail -f /tmp/a.txt

# --------------------------------------------------------------------------------------------------
# check code coverage for local development
# --------------------------------------------------------------------------------------------------

#coverage run -m unittest
#coverage report --omit="*/lib*,*__init__.py"
#coverage html --omit="*/lib*,*__init__.py"

# --------------------------------------------------------------------------------------------------
# test for docker using unittest
# --------------------------------------------------------------------------------------------------

python3 -W ignore:ResourceWarning -m unittest
#python3 -W ignore:ResourceWarning -m unittest tests/test_date_utils.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_s3_data_store.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_rds_data_store.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_client_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_dashboard_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_event_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_experiment_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_variation_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_order_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_product_agent.py
#python3 -W ignore:ResourceWarning -m unittest tests/test_server.py
