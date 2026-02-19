#!/bin/bash
# setup.sh
cp .env.example .env
echo "Changing permissions"
mkdir -p logs dags data tests
sudo chown -R $USER:0 .
sudo chmod -R 775 .
echo "Done!"