FROM python:3

RUN pip install opcua

WORKDIR /app

COPY . .

CMD ["python", "app/opcua_reader.py"]
