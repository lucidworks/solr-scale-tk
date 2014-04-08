#!/bin/bash
SDIR=`pwd`
java -Dlog4j.configuration=file:///$SDIR/log4j.properties -jar target/solr-scale-tk-0.1-exe.jar $*
