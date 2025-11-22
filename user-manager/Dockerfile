FROM python:3.14-slim
LABEL authors="urzim"
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]