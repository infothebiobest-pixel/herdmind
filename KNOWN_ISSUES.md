
## Recovery Drill Findings (2026-06-21)
- ai_service: confirmed auto-recovery on kill, ~3s downtime, MQTT reconnects cleanly
- mosquitto: restart:unless-stopped did not auto-trigger after `docker kill` in WSL environment
  - Workaround: manual `docker compose up -d mqtt` if broker container exits unexpectedly
  - Suspected WSL/Docker Desktop specific; needs verification on native Linux host
