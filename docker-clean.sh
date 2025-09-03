#!/bin/bash
echo "🔹 Stopping all containers..."
sudo docker stop $(sudo docker ps -aq) 2>/dev/null

echo "🔹 Removing all containers..."
sudo docker rm -f $(sudo docker ps -aq) 2>/dev/null

echo "🔹 Removing all images..."
sudo docker rmi -f $(sudo docker images -q) 2>/dev/null

echo "🔹 Removing all volumes..."
sudo docker volume prune -f

echo "🔹 Removing all networks..."
sudo docker network prune -f

echo "✅ Docker environment is now clean."
