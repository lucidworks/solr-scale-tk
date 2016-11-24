#!/bin/bash
FUSION=/home/ec2-user/fusion/3.0.0
if [ -d /vol0 ]; then
    if [ ! -d /vol0/fusion-data ]; then
      mkdir /vol0/fusion-data || true
      mv $FUSION/data /vol0/fusion-data/data
      ln -s /vol0/fusion-data/data $FUSION/data
      mv $FUSION/var /vol0/fusion-data/var
      ln -s /vol0/fusion-data/var $FUSION/var
    fi
fi
