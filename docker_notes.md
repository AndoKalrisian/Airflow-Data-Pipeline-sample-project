# Docker

### Install on Ubuntu
```
$ sudo apt-get update
$ sudo apt-get remove docker docker-engine docker.io
$ sudo apt-get install docker.io
```
Set docker to run at startup:
```
$ sudo systemctl start docker
$ sudo systemctl enable docker

