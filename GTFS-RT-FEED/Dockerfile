FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY .env* ./ 

EXPOSE 8004

CMD ["sh", "-c", "if [ \"$DEV_MODE\" = \"true\" ]; then uvicorn main:app --host 0.0.0.0 --port 8004 --reload; else uvicorn main:app --host 0.0.0.0 --port 8004; fi"] 