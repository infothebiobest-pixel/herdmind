#!/bin/bash
# =====================================================================
# HerdMind-X: Enterprise Orchestration Flawless Morning Boot Script
# =====================================================================

echo "🛑 Stopping zombie containers and clearing old network states..."
docker compose down --remove-orphans

echo "🧹 Flushing cached telemetry volume states to prevent authority locks..."
sudo rm -rf ./infra/influxdb/data/* ./infra/influxdb/config/*

echo "🔌 Pruning dangling network bridge channels..."
docker network prune -f

echo "🚀 Cold-building and spinning up the 16/16 clean stack..."
docker compose up -d --build

echo "⏳ Waiting for master database engines to clear internal fsync boot recoveries..."
sleep 10

echo "📋 Operational Status Check Matrix:"
docker compose ps
