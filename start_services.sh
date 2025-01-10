#!/bin/bash

# This script will start the services for the dataset.

cd ai
# Kill any existing qdrant processes
pkill -f qdrant

# Kill any existing python processes for backend and webChat
pkill -f backend.py
pkill -f webChat.py
echo "Starting qdrant..."
qdrant > qdrant.log 2>&1 &
sleep 2

# Check if Qdrant is ready
until curl -s http://localhost:6333/healthz > /dev/null; do
    echo "Waiting for Qdrant to be ready..."
    sleep 1
done

echo "Starting backend..."
python backend.py > backend.log 2>&1 &

echo "Starting frontend..."
python webChat.py > webChat.log 2>&1 &
