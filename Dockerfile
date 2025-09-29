FROM python:3.11-slim
WORKDIR /app
COPY . /app
# faster builds: install system deps for pyarrow/fastparquet if needed
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir .
ENTRYPOINT ["telemachus"]