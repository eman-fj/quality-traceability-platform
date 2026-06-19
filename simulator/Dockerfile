FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY inspection_simulator.py .
# -u = unbuffered stdout so events show up live in `docker compose logs`
CMD ["python", "-u", "inspection_simulator.py"]
