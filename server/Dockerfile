FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /server

COPY . /server

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "word_count_server.py"]
