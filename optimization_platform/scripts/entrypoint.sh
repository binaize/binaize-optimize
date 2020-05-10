#!/usr/bin/env bash

# --------------------------------------------------------------------------------------------------
# generate key and cert for https using openssl
# --------------------------------------------------------------------------------------------------

openssl req -newkey rsa:2048 -nodes -keyout /myserver-dev.key -x509 -out /myserver-dev.crt -subj "/C=US/ST=TEXAS/L=AUSTIN/O=IT/CN="$HOST_PUBLIC_IP_ADDR

# --------------------------------------------------------------------------------------------------
# start web service to provide rest end points for this container
# --------------------------------------------------------------------------------------------------

gunicorn --certfile=myserver-dev.crt --keyfile=myserver-dev.key --pythonpath / -b 0.0.0.0:$SERVICE_PORT -k gevent -t $SERVICE_TIMEOUT -w $WORKER_COUNT optimization_platform.deployment.server:app -k uvicorn.workers.UvicornWorker
#gunicorn --certfile=myserver-dev.crt --keyfile=myserver-dev.key --pythonpath / -b 0.0.0.0:6006 -k gevent -t 300 -w 2 optimization_platform.deployment.server:app -k uvicorn.workers.UvicornWorker


# --------------------------------------------------------------------------------------------------
# to make the container alive for indefinite time
# --------------------------------------------------------------------------------------------------

#touch /tmp/a.txt
#tail -f /tmp/a.txt

# --------------------------------------------------------------------------------------------------
# run directly
# ------------------------------------------

#export PYTHONPATH=`pwd`