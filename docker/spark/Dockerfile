FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Sao_Paulo

# Install Java 17 and system dependencies
RUN apt-get update && \
    apt-get install -y tzdata openjdk-17-jdk curl wget software-properties-common unzip && \
    ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python 3.12
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-venv python3.12-dev && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12 && \
    ln -s /usr/bin/python3.12 /usr/bin/python

# Install Python packages
RUN pip install pyspark pandas pyarrow boto3

# Install Spark 3.5.1
ENV SPARK_VERSION=3.5.1
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Clean Spark jars to avoid conflicts
RUN find $SPARK_HOME/jars -name "hadoop-*.jar" -delete && \
    find $SPARK_HOME/jars -name "aws-java-sdk-*.jar" -delete

# Add Hadoop 3.3.6 core jars
RUN mkdir -p /opt/hadoop && \
    wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/hadoop --strip-components=1 && \
    rm hadoop-3.3.6.tar.gz

RUN cp /opt/hadoop/share/hadoop/common/*.jar $SPARK_HOME/jars/ && \
    cp /opt/hadoop/share/hadoop/common/lib/*.jar $SPARK_HOME/jars/ && \
    cp /opt/hadoop/share/hadoop/hdfs/*.jar $SPARK_HOME/jars/ && \
    cp /opt/hadoop/share/hadoop/hdfs/lib/*.jar $SPARK_HOME/jars/ && \
    cp /opt/hadoop/share/hadoop/tools/lib/*.jar $SPARK_HOME/jars/

# Add Hadoop AWS and MapReduce jars
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -o $SPARK_HOME/jars/hadoop-aws.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.603/aws-java-sdk-bundle-1.12.603.jar -o $SPARK_HOME/jars/aws-java-sdk-bundle.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.6/hadoop-mapreduce-client-core-3.3.6.jar -o $SPARK_HOME/jars/hadoop-mapreduce-client-core.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-common/3.3.6/hadoop-mapreduce-client-common-3.3.6.jar -o $SPARK_HOME/jars/hadoop-mapreduce-client-common.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/3.3.6/hadoop-hdfs-3.3.6.jar -o $SPARK_HOME/jars/hadoop-hdfs.jar

# Add PostgreSQL JDBC driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -o $SPARK_HOME/jars/postgresql.jar

WORKDIR /opt/spark-jobs

CMD ["/bin/bash"]
