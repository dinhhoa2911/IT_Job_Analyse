FROM bde2020/hive:2.3.2-postgresql-metastore

COPY start-metastore.sh /usr/local/bin/start-metastore.sh
RUN chmod +x /usr/local/bin/start-metastore.sh

ENTRYPOINT ["/usr/local/bin/start-metastore.sh"]