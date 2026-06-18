#!/bin/bash
# Wait for JobManager to be ready, then submit the PyFlink job

JOBMANAGER_HOST="flink-jobmanager"
JOBMANAGER_PORT="8081"

echo "Waiting for Flink JobManager at $JOBMANAGER_HOST:$JOBMANAGER_PORT..."
until curl -sf "http://$JOBMANAGER_HOST:$JOBMANAGER_PORT/overview" > /dev/null; do
    echo "JobManager not ready yet, retrying in 5s..."
    sleep 5
done
echo "JobManager is ready. Submitting PyFlink job..."

# Submit via flink run — this connects to the cluster properly
/opt/flink/bin/flink run \
    --jobmanager "$JOBMANAGER_HOST:8081" \
    --python /app/flink_job.py \
    --pyFiles /app/flink_job.py
