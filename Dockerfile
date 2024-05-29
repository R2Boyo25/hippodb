FROM python:3.11.4-slim-bookworm
RUN mkdir /hippodb/
COPY . /hippodb/
RUN pip install --no-cache-dir /hippodb

WORKDIR /hippodb/
EXPOSE 8000/tcp
VOLUME /data 
ENV HIPPODB_DIR="/data"
CMD uvicorn --log-level warning --host 0.0.0.0 hippodb:app