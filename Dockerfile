FROM akariv/dgp-app:03c483c9155954ce90c775f4115ffb1f2152e163

USER root
RUN apt-get install -y curl gnupg wget unzip build-essential libsqlite3-dev zlib1g-dev

RUN wget https://github.com/mapbox/tippecanoe/archive/refs/tags/1.36.0.zip && \
    unzip 1.36.0.zip && rm 1.36.0.zip
RUN cd tippecanoe-1.36.0 && make -j && make install

USER etl

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY configuration.json dags/
COPY logo.png ui/dist/ui/en/assets/logo.png
COPY favicons/* ui/dist/ui/

COPY logo.png site/static/assets/img/logo.png

COPY events dags/events
COPY operators dags/operators/
COPY srm_tools srm_tools
COPY conf conf

ENV AIRFLOW__CORE__LOG_FORMAT="%(asctime)s:%(levelname)-8s:%(name)s:%(message)s"

COPY srm_etl_entrypoint.sh /app/
ENTRYPOINT ["/app/srm_etl_entrypoint.sh"]
