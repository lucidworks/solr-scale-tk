#!/bin/bash

THIS_SCRIPT="$0"
while [ -h "$THIS_SCRIPT" ] ; do
  ls=`ls -ld "$THIS_SCRIPT"`
  # Drop everything prior to ->
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    THIS_SCRIPT="$link"
  else
    THIS_SCRIPT=`dirname "$THIS_SCRIPT"`/"$link"
  fi
done

SCRIPT_DIR=`dirname "$THIS_SCRIPT"`
SCRIPT_DIR=`cd "$SCRIPT_DIR"; pwd`

MODE=$1

if [ "$MODE" != "upconfig" ] && [ "$MODE" != "zk-init" ] && [ "$MODE" != "setup" ]; then
  echo "MODE parameter $MODE not valid!"
  echo " "
  echo "Usage: ./solr-ctl.sh <MODE>"
  echo " "
  echo "  where MODE is one of: upconfig, zk-init, setup"
  echo " "
  echo "  Example: Setup node running on port 8984: ./solr-ctl.sh setup 84"
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
    ZK_DIR=/home/ec2-user/zookeeper-3.4.6
  fi 
  $ZK_DIR/bin/zkServer.sh stop || true
  echo "removing zookeeper data from $ZK_DIR/data/*"
  rm -rf $ZK_DIR/data/*
  $ZK_DIR/bin/zkServer.sh start
  sleep 1
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

if [ "$MODE" == "setup" ]; then

  if [ "$SOLR_PORT" == "84" ]; then
    # if the cloud84 directory does not exist, clone the server directory
    if [ ! -d "$SOLR_TOP/cloud84" ]; then
      cp -r $SOLR_TOP/server $SOLR_TOP/cloud84
    fi
  fi

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
  
  cp ~/rabbitmq/jars/*.jar $SOLR_TOP/cloud$SOLR_PORT/lib/ext/ || true
  
  exit 0
fi
