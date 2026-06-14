FROM python:3.11-slim
WORKDIR /app

# 1. Copy only requirements first to lock the dependency layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the code files afterward so code edits don't re-trigger pip install
COPY . .

# 3. Explicitly declare pythonpath to prevent runtime import crashes
ENV PYTHONPATH=/app

CMD ["python", "app/main.py"]
