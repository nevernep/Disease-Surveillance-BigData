FROM apache/airflow:2.10.5

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# S3A connector for Spark -> S3-compatible data lake. Versions must match the
# Hadoop client bundled with pyspark 3.5.3 (Hadoop 3.3.4).
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
RUN mkdir -p /opt/spark-jars \
    && curl -fsSL -o /opt/spark-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL -o /opt/spark-jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

ENV SPARK_JARS=/opt/spark-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar,/opt/spark-jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

USER airflow

RUN pip install --no-cache-dir \
    boto3==1.35.99 \
    openpyxl \
    pandas \
    pyspark==3.5.3 \
    pytest
