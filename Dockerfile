FROM ubuntu:22.04 


RUN apt-get update -y \
    && export DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
        sudo \
        curl \
        ssh \
    && apt-get clean

RUN useradd -m alfa 
RUN echo "alfa:supergroup" | chpasswd 
RUN adduser alfa sudo
Run echo "alfa     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && cd /usr/bin/


WORKDIR /home/alfa
USER alfa

RUN echo "Y" | sudo apt upgrade
RUN echo "Y" | sudo apt install openjdk-8-jdk
RUN echo "" | echo "" | echo "" | ssh-keygen -t rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 640 ~/.ssh/authorized_keys
RUN sudo apt-get install wget -y


WORKDIR /home/alfa/downloads
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xvzf hadoop-3.3.6.tar.gz
RUN wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3-scala2.13.tgz
RUN tar -xvzf spark-3.5.1-bin-hadoop3-scala2.13.tgz

RUN sudo chown -R alfa:alfa /opt/
RUN mv hadoop-3.3.6 /opt/hadoop
RUN mv spark-3.5.1-bin-hadoop3-scala2.13 /opt/spark


RUN sudo mkdir -p /home/hadoop/hdfs/namenode
RUN sudo mkdir -p /home/hadoop/hdfs/datanode
RUN sudo mkdir -p /home/zookeeper

RUN sudo chown -R alfa:alfa /home/hadoop/
RUN sudo chown -R alfa:alfa /home/zookeeper/


RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >> /opt/hadoop/etc/hadoop/hadoop-env.sh
RUN sed -i 's/<\/configuration>/<property>\n<name>fs.defaultFS<\/name>\n<value>hdfs:\/\/localhost:9000<\/value>\n<\/property>\n<property>\n<name>dfs.permissions.enable<\/name>\n<value>false<\/value>\n<\/property>\n<property>\n<name>hadoop.http.staticuser.user<\/name>\n<value>alfa<\/value>\n<\/property>\n<property>\n<name>hbase.zookeeper.property.dataDir<\/name>\n<value>\/home\/zookeeper<\/value>\n<\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/core-site.xml
RUN sed -i 's/<\/configuration>/<property>\n<name>dfs.replication<\/name>\n<value>1<\/value>\n<\/property>\n<property>\n<name>dfs.name.dir<\/name>\n<value>file:\/\/\/home\/hadoop\/hdfs\/namenode<\/value>\n<\/property>\n<property>\n<name>dfs.data.dir<\/name>\n<value>file:\/\/\/home\/hadoop\/hdfs\/datanode<\/value>\n<\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/hdfs-site.xml
RUN sed -i 's/<\/configuration>/<property>\n<name>mapreduce.framework.name<\/name>\n<value>yarn<\/value>\n<\/property>\n<property>\n<name>mapreduce.application.classpath<\/name>\n<value>$HADOOP_MAPRED_HOME\/share\/hadoop\/mapreduce\/*:$HADOOP_MAPRED_HOME\/share\/hadoop\/mapreduce\/lib\/*<\/value>\n<\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/mapred-site.xml
RUN sed -i 's/<\/configuration>/<property>\n<name>yarn.nodemanager.aux-services<\/name>\n<value>mapreduce_shuffle<\/value>\n<\/property>\n<property>\n<name>yarn.nodemanager.env-whitelist<\/name>\n<value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME<\/value><\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/yarn-site.xml

RUN /opt/hadoop/bin/hdfs namenode -format

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_INSTALL=$HADOOP_HOME
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

WORKDIR /opt/spark/conf
COPY spark-defaults.conf .
COPY spark-env.sh .

EXPOSE 22
EXPOSE 9870
EXPOSE 8088
EXPOSE 9000
EXPOSE 9866
EXPOSE 9864


RUN echo "Y" | sudo apt install python3-pip
RUN pip install jupyterlab
RUN pip install numpy
RUN pip install pyspark
RUN pip install hdfs

ENV PATH=$PATH:/home/alfa/.local/bin

EXPOSE 8888

WORKDIR /opt/hadoop/bin
COPY docker-entrypoint.sh .

RUN sudo mkdir -p /home/alfa/jupyter/source
RUN sudo mkdir -p /home/alfa/jupyter/data
RUN sudo chown -R alfa:alfa /home/alfa/jupyter

RUN sudo chmod 777 -R /home/alfa/jupyter
WORKDIR /home/alfa/jupyter

ENTRYPOINT ["/opt/hadoop/bin/docker-entrypoint.sh"]