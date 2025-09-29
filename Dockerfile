FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install pandas jsonschema
ENTRYPOINT ["python", "-m", "cli.main"]