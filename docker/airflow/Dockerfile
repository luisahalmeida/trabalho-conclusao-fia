# Base image with Python 3.10
FROM apache/airflow:3.1.0-python3.10

USER root

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-17-jdk curl unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark


ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN mkdir -p $SPARK_HOME/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -o $SPARK_HOME/jars/hadoop-aws.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.603/aws-java-sdk-bundle-1.12.603.jar -o $SPARK_HOME/jars/aws-java-sdk-bundle.jar && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -o $SPARK_HOME/jars/postgresql.jar

RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/ && \
    rm hadoop-3.3.6.tar.gz


RUN cp /opt/hadoop-3.3.6/share/hadoop/common/*.jar /opt/spark-3.5.1-bin-hadoop3/jars/ && \
    cp /opt/hadoop-3.3.6/share/hadoop/common/lib/*.jar /opt/spark-3.5.1-bin-hadoop3/jars/ && \
    cp /opt/hadoop-3.3.6/share/hadoop/hdfs/lib/*.jar /opt/spark-3.5.1-bin-hadoop3/jars/ && \
    cp /opt/hadoop-3.3.6/share/hadoop/tools/lib/*.jar /opt/spark-3.5.1-bin-hadoop3/jars/


# Install required Python packages
COPY requirements.txt /
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Optional: copy DAGs and Spark job scripts
# COPY ./dags /opt/airflow/dags
# COPY ./spark-jobs /opt/spark-jobs

USER airflow
