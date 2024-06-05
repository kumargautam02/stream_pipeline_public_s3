FROM spark:3.5.0-scala2.12-java17-ubuntu

WORKDIR /WORKING_DIR

USER root


# COPY C:/Users/Admin/Downloads/stream_task/Processing/main.py main.py 
# COPY C:/Users/Admin/Downloads/stream_task/Processing/stream_processing_script.py stream_processing_script.py 
# COPY C:/Users/Admin/Downloads/stream_task/Processing/stream_processing_script.py stream_processing_script.py
ADD  logger_config.py logger_config.py

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*
USER spark