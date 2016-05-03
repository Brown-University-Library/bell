#!/bin/bash

# Note:
# When an app setting is changed for code called by a worker, restart the worker from the `bell_code` dir.
#
# Example of killing and restarting worker...
#
# $ ps aux | grep "rq"
# ( examine output to find out worker PID for bell project)
#
# $ cd /path/to/bell_code/
# $ source ../env/bin/activate
# $ kill PID
# $ bash ./utils/queue_utils/worker_start.sh
#
# $ ps aux | grep "rq"
# (to confirm new worker is running)


WORKER_NAME=bell_worker_$RANDOM
LOG_FILENAME=$BELL_LOG_DIR/$WORKER_NAME.log

echo "worker name: " $WORKER_NAME
echo "log filename: " $LOG_FILENAME
echo "queue name: " $BELL_QUEUE_NAME

rqworker --name $WORKER_NAME $BELL_QUEUE_NAME >> $LOG_FILENAME 2>&1 &
# rqworker --name $WORKER_NAME $BELL_QUEUE_NAME
