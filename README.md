
## To Build

Requires Golang 1.10 to build the cgofuse dependency.

*Linux*
* Required dependencies: libfuse-dev, gcc

*Windows*
* Required dependencies: [WinFSP](https://github.com/billziss-gh/winfsp)


## To Run

Example command line:

`./fusenfs -mount-point=/home/dafell/fn -remoteips=10.1.0.5 -thisip=10.1.0.4 -memlimitmb=2048`