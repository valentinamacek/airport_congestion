# Single image for the whole Flink side, mirroring the lab setup.
# The base image already bundles Flink + PyFlink, so the SAME image can run as:
#   - the JobManager   (docker-compose command: jobmanager)
#   - the TaskManager  (docker-compose command: taskmanager)
#   - the job submitter (default CMD below: flink run ...)
ARG FLINK_IMAGE=fracorco/pyflink:1.18.1-scala_2.12-java11
FROM ${FLINK_IMAGE}

# Install Python deps first (better Docker cache), then copy the source.
# psycopg2-binary is needed by the TaskManager because the PostgreSQL sink
# runs there — that is why all three services build from this same image.
WORKDIR /app
COPY requirements.txt .
RUN pip install -U pip && \
    pip install -r requirements.txt
COPY . .

# Default command = submit the job to the session cluster.
#   PROCESSOR_JOBMANAGER     host:port of the JobManager REST endpoint
#   PROCESSOR_IMPLEMENTATION the -py entrypoint script
#   PROCESSOR_ARGS           extra CLI args forwarded to the job (e.g. --rewind)
# Overridden by `command: jobmanager` / `command: taskmanager` in docker-compose.
CMD flink run \
    -m ${PROCESSOR_JOBMANAGER:-flink-jobmanager:8081} \
    -pyfs kafka_utils.py \
    -py ${PROCESSOR_IMPLEMENTATION:-flink_job.py} \
    $PROCESSOR_ARGS
