#!/bin/bash
if [ -d /vol0 ]; then
    if [ ! -d /vol0/fusion-data ]; then
      mkdir /vol0/fusion-data || true
      mv /home/ec2-user/fusion/3.0.0/data /vol0/fusion-data/data
      ln -s /vol0/fusion-data/data /home/ec2-user/fusion/3.0.0/data
      mv /home/ec2-user/fusion/3.0.0/var /vol0/fusion-data/var
      ln -s /vol0/fusion-data/var /home/ec2-user/fusion/3.0.0/var
    fi
fi
