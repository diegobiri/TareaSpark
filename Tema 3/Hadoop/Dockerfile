FROM python:3.10-bullseye

USER root

# --------------------------------------------------------
# JAVA
# --------------------------------------------------------
RUN apt-get update 
RUN apt-get install -y --no-install-recommends openjdk-11-jdk
# For AMD based architecture use
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk/
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/

# --------------------------------------------------------
# HADOOP
# --------------------------------------------------------
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_URL=https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV PATH $HADOOP_PREFIX/bin/:$PATH
ENV PATH $HADOOP_HOME/bin/:$PATH

ENV HADOOP_COMMON_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_MAPRED_HOME=/opt/hadoop-$HADOOP_VERSION
ENV SQOOP_HOME=/usr/local/sqoop
ENV PATH /usr/local/sqoop/bin/:$PATH
ENV HADOOP_COMMON_LIB_NATIVE_DIR=/opt/hadoop-$HADOOP_VERSION/lib/native
ENV CLASSPATH=/tmp
ENV HBASE_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HCAT_HOME=/opt/hadoop-$HADOOP_VERSION
ENV ACCUMULO_HOME=/opt/hadoop-$HADOOP_VERSION
ENV ZOOKEEPER_HOME=/opt/hadoop-$HADOOP_VERSION

ENV FLUME_HOME=/usr/local/apache-flume-1.11.0-bin
ENV PATH /usr/local/apache-flume-1.11.0-bin/bin:$PATH
ENV JAVA_OPTS="-Xms400m -Xmx400m -Dcom.sun.management.jmxremote"

ENV PIG_HOME=/usr/local/pig-0.17.0
ENV PATH /usr/local/pig-0.17.0/bin:$PATH
ENV PIG_CLASSPATH=$HADOOP_CONF_DIR

RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz*

RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop
RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs
RUN mkdir /hadoop-data

RUN wget http://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz \
&&  tar -xf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz \
&&  mv sqoop-1.4.7.bin__hadoop-2.6.0 /usr/local/sqoop

RUN wget https://dlcdn.apache.org//commons/lang/binaries/commons-lang-2.6-bin.tar.gz \
&& tar -xf commons-lang-2.6-bin.tar.gz \
&& cp commons-lang-2.6/commons-lang-2.6.jar /usr/local/sqoop/lib

RUN wget http://ftp.ntu.edu.tw/MySQL/Downloads/Connector-J/mysql-connector-java-5.1.49.tar.gz \
&& tar -zxf mysql-connector-java-5.1.49.tar.gz \
&& cd mysql-connector-java-5.1.49 \
&& mv mysql-connector-java-5.1.49-bin.jar /usr/local/sqoop/lib 

RUN wget https://dlcdn.apache.org/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz \
&& tar xzf apache-flume-1.11.0-bin.tar.gz \
&& mv apache-flume-1.11.0-bin/ /usr/local/

RUN wget https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz \
&& tar -xzf pig-0.17.0.tar.gz \
&& cp -r pig-0.17.0/ /usr/local

USER root

COPY conf/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY conf/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY conf/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml