#!/bin/bash

# to run with Batch Start Task: /bin/bash -c './mount-fscache.sh'
cacheip=10.5.0.5

sudo yum -y install nfs-utils

sudo mkdir /mnt/fusenfs
sudo chmod 777 /mnt/fusenfs

printf 'INFO: Mounting export from the cache server\n'
printf 'IP address of cache server: %s\n' "${cacheip}"

sudo umount /mnt/fusenfs
sudo mount -t nfs $cacheip:/mnt/fusenfs /mnt/fusenfs
