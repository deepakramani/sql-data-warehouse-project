#!/bin/bash

cd ~
sudo apt install docker.io -y
mkdir soft && cd soft 
wget https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64 -O docker-compose
chmod +x docker-compose
echo 'export PATH="/home/'$USER'/soft:$PATH"' >> ~/.bashrc
sudo groupadd docker
sudo usermod -aG docker $USER
cd ~/sql-data-warehouse-project
