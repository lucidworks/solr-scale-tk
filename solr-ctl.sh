#!/bin/bash

ZK_CLIENT_TIMEOUT="20000"
GC_LOG_OPTS="-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution"
SOLR_JAVA_OPTS="-server -XX:+HeapDumpOnOutOfMemoryError -DzkClientTimeout=$ZK_CLIENT_TIMEOUT $GC_LOG_OPTS \
-XX:-UseSuperWord \
-XX:NewRatio=3 \
-XX:SurvivorRatio=4 \
-XX:TargetSurvivorRatio=90 \
-XX:MaxTenuringThreshold=8 \
-XX:+UseConcMarkSweepGC \
-XX:+CMSScavengeBeforeRemark \
-XX:PretenureSizeThreshold=64m \
-XX:CMSFullGCsBeforeCompaction=1 \
-XX:+UseCMSInitiatingOccupancyOnly \
-XX:CMSInitiatingOccupancyFraction=70 \
-XX:CMSTriggerPermRatio=80 \
-XX:CMSMaxAbortablePrecleanTime=6000 \
-XX:+CMSParallelRemarkEnabled \
-XX:+ParallelRefProcEnabled \
-XX:+UseLargePages \
-XX:+AggressiveOpts"

#SOLR_JAVA_OPTS="-server -XX:-UseSuperWord -XX:+UseG1GC -XX:MaxGCPauseMillis=5000 -XX:+HeapDumpOnOutOfMemoryError -DzkClientTimeout=$ZK_CLIENT_TIMEOUT $GC_LOG_OPTS"
REMOTE_JMX_OPTS="-Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"

SCRIPT_DIR=/home/ec2-user/cloud
MODE=$1

if [ "$MODE" != "upconfig" ] && [ "$MODE" != "zk-init" ] && [ "$MODE" != "kill" ] && [ "$MODE" != "setup" ] &&  [ "$MODE" != "stop" ] && [ "$MODE" != "restart" ]; then
  echo "MODE parameter $MODE not valid!"
  echo " "
  echo "Usage: ./solr-ctl.sh <MODE>"
  echo " "
  echo "  where MODE is one of: upconfig, zk-init, kill, setup, stop, restart"
  echo " "
  echo "  Example: Restart Solr running on port 8984: ./solr-ctl.sh restart 84"
  echo " "
  exit 1
fi

if [ ! -f $SCRIPT_DIR/solr-ctl-env.sh ]; then
  echo "Required environment script $SCRIPT_DIR/solr-ctl-env.sh not found!"
  echo "At a minimum, you need to create this file to define ZK_HOST"
  exit 1
fi

source $SCRIPT_DIR/solr-ctl-env.sh

if [ "$MODE" == "zk-init" ]; then
  ZK_DIR=$2
  if [ ! -d $ZK_DIR ]; then
    ZK_DIR=/home/ec2-user/zookeeper-3.4.5
  fi 
  $ZK_DIR/bin/zkServer.sh stop || true
  echo "removing zookeeper data from $ZK_DIR/data/*"
  rm -rf $ZK_DIR/data/*
  $ZK_DIR/bin/zkServer.sh start
  sleep 1
  exit 0
fi

if [ "$MODE" == "kill" ]; then
  SOLR_PORT="$2"

  cd $SOLR_TOP/cloud$SOLR_PORT
  java -jar start.jar STOP.PORT=79$SOLR_PORT STOP.KEY=key --stop || true
  sleep 5

  for ID in `ps waux | grep start.jar | grep 89$SOLR_PORT | awk '{print $2}' | sort -r`
    do
      kill -9 $ID
      echo "Killed process $ID"
  done

  sleep 1

  for ID in `ps waux | grep start.jar | grep 89$SOLR_PORT | awk '{print $2}' | sort -r`
    do
      echo "Failed to kill previous java process $ID ... script fails."
      exit 1
  done

  exit 0
fi

if [ "$ZK_HOST" == "" ]
then
  echo "ZK_HOST environment variable not set! Please update your solr-ctl-env.sh script."
  exit 1
fi

if [ "$MODE" == "upconfig" ]; then
  UPCONFIG_NAME="$2"  
  chmod +x $CLOUD_SCRIPTS_DIR/zkcli.sh
  cd $CLOUD_SCRIPTS_DIR
  ./zkcli.sh -zkhost $ZK_HOST -cmd upconfig -confdir $SCRIPT_DIR/tmp/$UPCONFIG_NAME/conf -confname $UPCONFIG_NAME
  exit 0
fi

SOLR_PORT="$2"

if [ "$SOLR_PORT" == "" ]; then
  echo "No Solr port provided!"
  exit 1
fi

cd $SOLR_TOP/cloud$SOLR_PORT
java -jar start.jar STOP.PORT=79$SOLR_PORT STOP.KEY=key --stop || true
sleep 5

for ID in `ps waux | grep start.jar | grep 89$SOLR_PORT | awk '{print $2}' | sort -r`
  do
    kill -9 $ID
    echo "Killed process $ID"
done

sleep 1

for ID in `ps waux | grep start.jar | grep 89$SOLR_PORT | awk '{print $2}' | sort -r`
  do
    echo "Failed to kill previous java process $ID ... script fails."
    exit 1
done

sleep 2 

# backup the log files
mv $SOLR_TOP/cloud$SOLR_PORT/logs/solr.log $SOLR_TOP/cloud$SOLR_PORT/logs/solr_log_`date +"%Y%m%d_%H%M"`
mv $SOLR_TOP/cloud$SOLR_PORT/logs/gc.log $SOLR_TOP/cloud$SOLR_PORT/logs/gc_log_`date +"%Y%m%d_%H%M"`

if [ "$MODE" == "stop" ]; then
  exit 0
fi

if [ "$MODE" == "setup" ]; then

  VOL_INDEX=$3
  if [ "$VOL_INDEX" == "" ]; then
    echo "ERROR: Must specify instance store volume index!"
    exit 1
  fi
  
  # determine if instance store volumes were mounted
  if [ -d /vol0 ]; then
    echo "/vol0 exists"
    if [ ! -d /vol0/cloud84 ]; then
      echo "/vol0/cloud84 does not exist, creating ..."
      mv $SOLR_TOP/cloud84 /vol0/
      ln -s /vol0/cloud84 $SOLR_TOP/cloud84
      mkdir -p /vol0/cloud84/solr/backups
    fi
  fi

  CLOUD_84=$SOLR_TOP/cloud84
  if [ -d /vol0/cloud84 ]; then
    CLOUD_84="/vol0/cloud84"
  fi  
  
  # the AMI only has the cloud84 directory in place
  # clone it for any others and put on a dedicated disk if avail
  if [ "$SOLR_PORT" != "84" ]; then
    echo "rm -rf $SOLR_TOP/cloud$SOLR_PORT"
    rm -rf $SOLR_TOP/cloud$SOLR_PORT
    if [ -d /vol$VOL_INDEX ]; then
      rm -rf /vol$VOL_INDEX/cloud$SOLR_PORT
      cp -r -f $CLOUD_84 /vol$VOL_INDEX/cloud$SOLR_PORT
      ln -s /vol$VOL_INDEX/cloud$SOLR_PORT $SOLR_TOP/cloud$SOLR_PORT
      mkdir -p /vol$VOL_INDEX/cloud$SOLR_PORT/solr/backups      
    else
      cp -r $CLOUD_84 $SOLR_TOP/cloud$SOLR_PORT    
      mkdir -p $SOLR_TOP/cloud$SOLR_PORT/solr/backups      
    fi            
  fi

  rm -rf $SOLR_TOP/cloud$SOLR_PORT/logs/*
  rm -rf $SOLR_TOP/cloud$SOLR_PORT/solr/*shard*
  
  cp ~/rabbitmq/jars/*.jar $SOLR_TOP/cloud$SOLR_PORT/lib/ext/
  
  exit 0
fi

if [ "$SOLR_JAVA_MEM" == "" ]; then
  SOLR_JAVA_MEM="-Xms512m -Xmx512m -XX:MaxPermSize=256m -XX:PermSize=256m"
fi

# Get the public-hostname from the AWS metadata service
echo "Calling out to AWS metadata service to get hostname"
MYHOST=`curl -s http://169.254.169.254/latest/meta-data/public-hostname`
#EC2_INSTANCE_TYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type`

echo "Starting Solr server(s) from $SOLR_TOP using:"
echo "    MYHOST          = $MYHOST"
echo "    ZK_HOST         = $ZK_HOST"
echo "    SOLR_JAVA_HOME  = $SOLR_JAVA_HOME"
echo "    SOLR_PORT       = 89$SOLR_PORT"
echo "    SOLR_JAVA_OPTS  = $SOLR_JAVA_OPTS"
echo "    SOLR_JAVA_MEM   = $SOLR_JAVA_MEM"
echo "    REMOTE_JMX_OPTS = $REMOTE_JMX_OPTS"

cd $SOLR_TOP/cloud$SOLR_PORT
nohup $SOLR_JAVA_HOME/bin/java -Xss256k $SOLR_JAVA_OPTS $SOLR_JAVA_MEM $REMOTE_JMX_OPTS \
  -Djava.rmi.server.hostname=$MYHOST \
  -Dcom.sun.management.jmxremote.port=10$SOLR_PORT \
  -Dcom.sun.management.jmxremote.rmi.port=10$SOLR_PORT \
  -Xloggc:$SOLR_TOP/cloud$SOLR_PORT/logs/gc.log -XX:OnOutOfMemoryError="$SCRIPT_DIR/oom_solr.sh $SOLR_PORT %p" \
  -Dlog4j.debug -Dhost="$MYHOST" -Dlog4j.configuration=file:///$SCRIPT_DIR/log4j.properties -DzkHost="$ZK_HOST" \
  -DSTOP.PORT=79$SOLR_PORT -DSTOP.KEY=key -Djetty.port=89$SOLR_PORT \
  -Dsolr.solr.home=$SOLR_TOP/cloud$SOLR_PORT/solr \
  -Duser.timezone="UTC" \
  -jar start.jar 1>$SOLR_TOP/cloud$SOLR_PORT.log 2>&1 &
  
echo "Started Solr server with zkHost=$ZK_HOST on $MYHOST:89$SOLR_PORT."
