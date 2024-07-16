FROM python:3.10
WORKDIR /app
COPY requirements.txt requirements.txt
COPY restart_handler.py restart_handler.py
RUN python3 -m pip install -r requirements.txt