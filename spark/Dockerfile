FROM eclipse-temurin:11-jdk

LABEL org.opencontainers.image.title="graphsense-spark"
LABEL org.opencontainers.image.maintainer="contact@ikna.io"
LABEL org.opencontainers.image.url="https://www.ikna.io/"
LABEL org.opencontainers.image.description="The GraphSense Transformation Pipeline reads raw block and transaction data and computes the transformed keyspace holding aggregate data and statistics."
LABEL org.opencontainers.image.source="https://github.com/graphsense/graphsense-spark"

ARG UID=10000

ARG SPARK_UI_PORT=4040

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg && \
    install -d -m 0755 /etc/apt/keyrings && \
    curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
        | gpg --dearmor -o /etc/apt/keyrings/sbt.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/sbt.gpg] https://repo.scala-sbt.org/scalasbt/debian all main" \
        | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb [signed-by=/etc/apt/keyrings/sbt.gpg] https://repo.scala-sbt.org/scalasbt/debian /" \
        | tee /etc/apt/sources.list.d/sbt_old.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends python3-pip python3-setuptools python3-wheel sbt && \
    useradd -m -d /home/dockeruser -r -u $UID dockeruser

# install Spark
RUN mkdir -p /opt/graphsense && \
    wget https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-without-hadoop.tgz -O - | tar -xz -C /opt && \
    ln -s /opt/spark-3.5.8-bin-without-hadoop /opt/spark && \
    wget https://dlcdn.apache.org/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz -O - | tar -xz -C /opt && \
    ln -s /opt/hadoop-2.10.2 /opt/hadoop && \
    echo "#!/usr/bin/env bash\nexport SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)" >> /opt/spark/conf/spark-env.sh && \
    chmod 755 /opt/spark/conf/spark-env.sh


ENV SPARK_HOME="/opt/spark"
ENV HADOOP_HOME="/opt/hadoop"
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

WORKDIR /opt/graphsense

RUN mkdir bin && cd bin && curl -OL https://downloads.datastax.com/dsbulk/dsbulk-1.10.tar.gz && tar -xzvf dsbulk-1.10.tar.gz
ENV PATH="$PATH:/opt/graphsense/bin/dsbulk-1.10.0/bin"


ADD src/ ./src
ADD Makefile .
ADD project/build.properties ./project/build.properties
ADD project/plugins.sbt ./project/plugins.sbt
ADD .scalafix.conf .
ADD .scalafmt.conf .
ADD build.sbt .
RUN sbt package && \
    chown -R dockeruser /opt/graphsense && \
    rm -rf /root/.ivy2 /root/.cache /root/.sbt && \
    cp target/scala-2.12/graphsense-spark*.jar graphsense-spark.jar

ADD docker/ .
RUN mv log4j2.properties /opt/spark/conf && cp /opt/spark/conf/log4j2.properties /opt/spark/conf/log4j.properties && cp /opt/spark/conf/log4j2.properties /opt/spark/conf/log4j2.default

USER dockeruser

EXPOSE $SPARK_UI_PORT 
