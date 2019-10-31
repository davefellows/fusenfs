#!/bin/bash
nfsip=10.4.0.4

sudo yum -y install nfs-utils
sudo yum -y install fuse
sudo yum -y install fuse-devel

sudo mkdir /mnt/fusenfs
sudo chmod 777 /mnt/fusenfs

mv fusenfs.log fusenfs-$(date +%Y%m%d-%H:%M).log

ip="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ' ' -f 10)";

printf 'INFO: Mounting NFS share /mnt/data\n'
sudo mkdir /mnt/data
sudo umount /mnt/data
sudo mount -t nfs $nfsip:/mnt/data /mnt/data
sudo fusermount -u /mnt/fusenfs

printf 'INFO: Setting up NFS export for cache dir\n'

cp /mnt/data/fusenfs .

printf 'INFO: Starting FUSE cache @ /mnt/fusenfs thisip=%s\n' "${ip}"

nohup ./fusenfs -nfs-mount=/mnt/data -mount-point=/mnt/fusenfs -fscachepath=/mnt/resource/fscache -memlimitmb=50000 -log=fusenfs.log > /dev/null 2>&1 &


echo '/mnt/fusenfs *(rw,fsid=1,sync,no_subtree_check)' > /etc/exports
sleep 5
exportfs -a
sudo systemctl enable nfs-server.service
sudo systemctl restart nfs-server.service
