FROM python:3.11-slim

WORKDIR /app

# Explicit environment configurations
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run directly from the /app context space
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
