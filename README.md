
## To Build

Requires Golang 1.10 to build the cgofuse dependency.

*Linux*
* Required dependencies: libfuse-dev (Ubuntu) or fuse-devel (CentOS/Fedora), gcc

*Windows*
* Required dependencies: [WinFSP](https://github.com/billziss-gh/winfsp)


## To Run

Example command line:

`./fusenfs -nfs-mount=/mnt/nfs -mount-point=/mnt/fusenfs -remoteips=10.1.0.5 -thisip=10.1.0.4 -memlimitmb=2048`

*Where:*
* nfs-mount = Path to where the NFS export has been mounted locally.
* mount-point = Path where you wish this FUSE file system to be mounted.
* remoteips = Comma separated list of IPs of remote cache servers in this cluster.
* thisip = IP address of this node to use for incoming cache requests (RPC).
* memlimitmb = (Optional) Specify a memory limit for the in-memory cache (recommended).
