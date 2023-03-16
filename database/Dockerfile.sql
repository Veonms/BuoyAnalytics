FROM mysql/mysql-server

WORKDIR /tmp

COPY *.sql /tmp/

COPY schema.sql /docker-entrypoint-initdb.d

ENV MYSQL_ROOT_PASSWORD example
ENV MYSQL_ROOT_HOST=%