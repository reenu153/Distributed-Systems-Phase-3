FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /load-balancer

COPY . /load-balancer

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "load_balancer.py"]