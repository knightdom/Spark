FROM ubuntu
LABEL maintainer="kinghtdom@outlook.com"

ENV TZ "Asia/Shanghai"
ENV LANG C.UTF-8

#设置时区 替换阿里源 安装软件
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
&& sed -i "s@http://deb.debian.org@http://mirrors.aliyun.com@g" /etc/apt/sources.list && rm -Rf /var/lib/apt/lists/* \
&& apt update \
&& apt install -y \
    openjdk-8-jdk \
    scala \
    vim \
    net-tools \
    openssh-server \
    openssh-client \
    systemctl \
&& apt purge -y --auto-remove \
&& ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
&& systemctl enable ssh

ADD ./hadoop-3.3.2-src.tar.gz /usr/local/
ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"
ENV HADOOP_HOME "/usr/local/hadoop"
ENV HADOOP_CONF_DIR "${HADOOP_HOME}/etc/hadoop"

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile \
&& echo "export JRE_HOME=${JAVA_HOME}/jre" >> /etc/profile \
&& echo "export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib" >> /etc/profile \
&& echo "export PATH=${JAVA_HOME}/bin:$PATH" >> /etc/profile \
&& echo "export HADOOP_HOME=${HADOOP_HOME}" >> /etc/profile \
&& echo "export PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin" >> /etc/profile \
&& echo "export HADOOP_COMMON_HOME=${HADOOP_HOME}" >> /etc/profile \
&& echo "export HADOOP_HDFS_HOME=${HADOOP_HOME}" >> /etc/profile \
&& echo "export HADOOP_MAPRED_HOME=${HADOOP_HOME}" >> /etc/profile \
&& echo "export HADOOP_YARN_HOME=${HADOOP_HOME}" >> /etc/profile \
&& echo "export HADOOP_INSTALL=${HADOOP_HOME}" >> /etc/profile \
&& echo "export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native" >> /etc/profile \
&& echo "export HADOOP_CONF_DIR=${HADOOP_HOME}" >> /etc/profile \
&& echo "export HADOOP_LIBEXEC_DIR=${HADOOP_HOME}/libexec" >> /etc/profile \
&& echo "export JAVA_LIBRARY_PATH=${HADOOP_HOME}/lib/native:$JAVA_LIBRARY_PATH" >> /etc/profile \
&& echo "export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop" >> /etc/profile \
&& echo "export HDFS_DATANODE_USER=root" >> /etc/profile \
&& echo "export HDFS_DATANODE_SECURE_USER=root" >> /etc/profile \
&& echo "export HDFS_SECONDARYNAMENODE_USER=root" >> /etc/profile \
&& echo "export HDFS_NAMENODE_USER=root" >> /etc/profile \
&& echo "export YARN_RESOURCEMANAGER_USER=root" >> /etc/profile \
&& echo "export YARN_NODEMANAGER_USER=root" >> /etc/profile \
&& mkdir -p /data/hadoop/tmp \
&& mkdir -p /data/hadoop/hdfs/name \
&& mkdir /data/hadoop/hdfs/data \
&& cd /usr/local \
&& mv hadoop-3.3.2-src hadoop

ADD ./hadoop-env.sh $HADOOP_CONF_DIR
ADD ./core-site.xml $HADOOP_CONF_DIR
ADD ./hadoop-env.sh $HADOOP_CONF_DIR
ADD ./hdfs-site.xml $HADOOP_CONF_DIR
ADD ./mapred-site.xml $HADOOP_CONF_DIR
ADD ./workers $HADOOP_CONF_DIR
ADD ./yarn-site.xml $HADOOP_CONF_DIR
ADD ./autoservice.sh /data/
RUN chmod 0777 /data/autoservice.sh

WORKDIR /www

ENTRYPOINT [ "/bin/sh", "/data/autoservice.sh" ]