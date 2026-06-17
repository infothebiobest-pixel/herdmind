FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the local structure directly 
COPY . .

ENV PYTHONPATH=/app

# FIXED: Explicitly targeting app/main.py relative to WORKDIR
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
