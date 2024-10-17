FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /client

COPY . /client

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "word_count_client.py", "keyword", "filename"]