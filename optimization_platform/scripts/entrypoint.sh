#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------


#gunicorn --pythonpath / -b 0.0.0.0:$SERVICE_PORT -k gevent -t $SERVICE_TIMEOUT -w $WORKER_COUNT app:app
#export PYTHONPATH=`pwd`
#gunicorn --pythonpath / -b 0.0.0.0:$SERVICE_PORT -k gevent -t $SERVICE_TIMEOUT -w $WORKER_COUNT optimization_platform.deployment.server:app -k uvicorn.workers.UvicornWorker
#gunicorn --pythonpath / -b 0.0.0.0:6006 -k gevent -t 300 -w 2 optimization_platform.deployment.server:app -k uvicorn.workers.UvicornWorker
gunicorn --certfile=myserver-dev.crt --keyfile=myserver-dev.key --pythonpath / -b 0.0.0.0:$SERVICE_PORT -k gevent -t $SERVICE_TIMEOUT -w $WORKER_COUNT optimization_platform.deployment.server:app -k uvicorn.workers.UvicornWorker
#gunicorn --certfile=myserver-dev.crt --keyfile=myserver-dev.key --pythonpath / -b 0.0.0.0:6006 -k gevent -t 300 -w 2 optimization_platform.deployment.server:app -k uvicorn.workers.UvicornWorker

#http --verify=myserver-dev.crt https://0.0.0.0:6006/
# --------------------------------------------------------------------------------------------------
# to make the container alive for indefinite time
# --------------------------------------------------------------------------------------------------
#touch /tmp/a.txt
#tail -f /tmp/a.txt

# --------------------------------------------------------------------------------------------------
# run directly
# ------------------------------------------
#export PYTHONPATH=`pwd`