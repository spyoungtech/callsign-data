FROM python:3.12-slim
WORKDIR /opt/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY callsigns /opt/app/callsigns/

ENTRYPOINT ["python", "-m", "callsigns.builder"]
