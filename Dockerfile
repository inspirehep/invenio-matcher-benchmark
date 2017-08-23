FROM inspirehep/elasticsearch

RUN mkdir /data && chown -R elasticsearch:elasticsearch /data && echo 'es.path.data: /data' >> config/elasticsearch.yml && echo 'path.data: /data' >> config/elasticsearch.yml

ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/e1f115e4ca285c3c24e847c4dd4be955e0ed51c2/wait-for-it.sh /utils/wait-for-it.sh

COPY ./demo_records /demo_records
COPY ./scripts /scripts

RUN /docker-entrypoint.sh elasticsearch -p /tmp/epid & /bin/bash /utils/wait-for-it.sh -t 0 localhost:9200 -- /scripts/load_records; kill $(cat /tmp/epid) && wait $(cat /tmp/epid); exit 0;
