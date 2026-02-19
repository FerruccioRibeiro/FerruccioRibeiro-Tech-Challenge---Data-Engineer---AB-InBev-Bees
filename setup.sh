#!/bin/bash
# setup.sh
cp .env.example .env
sudo chown -R :0 .
sudo chmod -R g+rwx .
echo "Setup complete! Now run: docker-compose up -d"