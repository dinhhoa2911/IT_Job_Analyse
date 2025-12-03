FROM apache/superset:pr-35460-1567b2d-ci

USER root
# cài thêm driver Trino cho SQLAlchemy
RUN pip install --no-cache-dir sqlalchemy-trino trino[sqlalchemy] \
    && pip check || true

USER superset
