#!/bin/bash


WORKER_NAME=bell_worker_$RANDOM
LOG_FILENAME=$BELL_LOG_DIR/$WORKER_NAME.log
QUEUE_NAME="bell_work"

echo "worker name: " $WORKER_NAME
echo "log filename: " $LOG_FILENAME
echo "queue name: " $QUEUE_NAME

rqworker --name $WORKER_NAME $QUEUE_NAME >> $LOG_FILENAME 2>&1 &