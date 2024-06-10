# Use the official Ubuntu 22.04 as the base image
FROM ubuntu:22.04 

# Update the package list and install necessary packages
RUN apt-get update -y \
    && export DEBIAN_FRONTEND=noninteractive && apt-get install -y\
        sudo \
        curl \
        ssh \
		openjdk-8-jdk \
		wget \
		python3-pip \
    && apt-get clean

# Create a new superuser 'alfa' and allow 'alfa' to use sudo without a password
RUN useradd -m alfa
RUN echo "alfa:supergroup" | chpasswd
RUN adduser alfa sudo
RUN echo "alfa     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Set the working directory to 'alfa's home	and switch to user 'alfa'
WORKDIR /home/alfa
USER alfa

# Generate an SSH key and add the SSH key to authorized_keys
RUN echo "" | echo "" | echo "" | ssh-keygen -t rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 640 ~/.ssh/authorized_keys

# Change the wdir to 'downloads'
WORKDIR /home/alfa/downloads
# Download and extract Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xvzf hadoop-3.3.6.tar.gz
# Download and extract Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3-scala2.13.tgz
RUN tar -xvzf spark-3.5.1-bin-hadoop3-scala2.13.tgz

# Change ownership of /opt to 'alfa'
RUN sudo chown -R alfa:alfa /opt/
# Move extracted Hadoop and Spark to /opt directory
RUN mv hadoop-3.3.6 /opt/hadoop
RUN mv spark-3.5.1-bin-hadoop3-scala2.13 /opt/spark

# Create directories for HDFS namenode, datanode, and Zookeeper
RUN sudo mkdir -p /home/hadoop/hdfs/namenode
RUN sudo mkdir -p /home/hadoop/hdfs/datanode
RUN sudo mkdir -p /home/zookeeper

# Change ownership of HDFS and Zookeeper directories to 'alfa'
RUN sudo chown -R alfa:alfa /home/hadoop/
RUN sudo chown -R alfa:alfa /home/zookeeper/

# Configure Hadoop
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >> /opt/hadoop/etc/hadoop/hadoop-env.sh
RUN sed -i 's/<\/configuration>/<property>\n<name>fs.defaultFS<\/name>\n<value>hdfs:\/\/localhost:9000<\/value>\n<\/property>\n<property>\n<name>dfs.permissions.enable<\/name>\n<value>false<\/value>\n<\/property>\n<property>\n<name>hadoop.http.staticuser.user<\/name>\n<value>alfa<\/value>\n<\/property>\n<property>\n<name>hbase.zookeeper.property.dataDir<\/name>\n<value>\/home\/zookeeper<\/value>\n<\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/core-site.xml
RUN sed -i 's/<\/configuration>/<property>\n<name>dfs.replication<\/name>\n<value>1<\/value>\n<\/property>\n<property>\n<name>dfs.name.dir<\/name>\n<value>file:\/\/\/home\/hadoop\/hdfs\/namenode<\/value>\n<\/property>\n<property>\n<name>dfs.data.dir<\/name>\n<value>file:\/\/\/home\/hadoop\/hdfs\/datanode<\/value>\n<\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/hdfs-site.xml
RUN sed -i 's/<\/configuration>/<property>\n<name>mapreduce.framework.name<\/name>\n<value>yarn<\/value>\n<\/property>\n<property>\n<name>mapreduce.application.classpath<\/name>\n<value>$HADOOP_MAPRED_HOME\/share\/hadoop\/mapreduce\/*:$HADOOP_MAPRED_HOME\/share\/hadoop\/mapreduce\/lib\/*<\/value>\n<\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/mapred-site.xml
RUN sed -i 's/<\/configuration>/<property>\n<name>yarn.nodemanager.aux-services<\/name>\n<value>mapreduce_shuffle<\/value>\n<\/property>\n<property>\n<name>yarn.nodemanager.env-whitelist<\/name>\n<value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME<\/value><\/property>\n<\/configuration>/g' /opt/hadoop/etc/hadoop/yarn-site.xml

# Format the HDFS namenode
RUN /opt/hadoop/bin/hdfs namenode -format

# Set environment variables for Hadoop and Spark
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

# Change the working directory to Spark configuration directory and copy Spark configuration files
WORKDIR /opt/spark/conf
COPY spark-defaults.conf .
COPY spark-env.sh .

# Expose necessary ports
# SSH
EXPOSE 22
# Hadoop NameNode Web UI
EXPOSE 9870
# Hadoop YARN ResourceManager Web UI
EXPOSE 8088
# Hadoop HDFS NameNode default port
EXPOSE 9000
# Hadoop DataNode Web UI
EXPOSE 9866
# Hadoop DataNode default port
EXPOSE 9864

# Create directories for Jupyter source and data
RUN sudo mkdir -p /home/alfa/jupyter/source
RUN sudo mkdir -p /home/alfa/jupyter/data
RUN sudo chown -R alfa:alfa /home/alfa/jupyter
RUN sudo chmod 777 -R /home/alfa/jupyter
 
# Install JupyterLab along with some Python packages
RUN pip install jupyterlab
RUN pip install numpy
RUN pip install pyspark
RUN pip install hdfs
RUN pip install mrjob

# Add local bin to PATH
ENV PATH=$PATH:/home/alfa/.local/bin

# Expose JupyterLab default port
EXPOSE 8888

# Change working directory to local bin and copy entrypoint script
WORKDIR /home/alfa/.local/bin
COPY docker-entrypoint.sh .

# Set the working directory for Jupyter
WORKDIR /home/alfa/jupyter
	
# Set the entrypoint to the entrypoint script
ENTRYPOINT ["/home/alfa/.local/bin/docker-entrypoint.sh"]