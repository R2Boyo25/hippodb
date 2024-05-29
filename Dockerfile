FROM python:3.11.4-slim-bookworm
RUN mkdir /hippodb/
COPY . /hippodb/
RUN python3 -m pip install --no-cache-dir /hippodb

WORKDIR /hippodb/
EXPOSE 8000/tcp
VOLUME /data 
ENV HIPPODB_DIR="/data"
CMD [ "uvicorn" "hippodb:app" ]