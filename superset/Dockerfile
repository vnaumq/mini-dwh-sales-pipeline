FROM apache/superset:4.1.2
USER root
# Set the working directory
WORKDIR /app

# Copy and install local requirements
COPY requirements-local.txt /app/requirements-local.txt
RUN pip install -r requirements-local.txt


ENV ADMIN_USERNAME=${ADMIN_USERNAME}
ENV ADMIN_EMAIL=${ADMIN_EMAIL}
ENV ADMIN_PASSWORD=${ADMIN_PASSWORD}
ENV EXECUTOR=Local

COPY ./superset-init.sh /superset-init.sh
COPY superset_config.py /app/
COPY superset.db /app/superset_home/superset.db
ENV SUPERSET_LOAD_EXAMPLES=no
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]