#!/bin/bash
JAVA=/home/ec2-user/jdk1.7.0_25
ZK=/home/ec2-user/zookeeper-3.4.5
$JAVA/bin/java -cp $ZK/zookeeper-3.4.5.jar:$ZK/lib/*:$ZK/conf org.apache.zookeeper.server.PurgeTxnLog /vol0/data /vol0/data -n 3
