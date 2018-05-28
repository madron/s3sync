FROM madron/pyinstaller-alpine:3.6.5 as builder

COPY requirements.txt /src/
RUN pip3 install -r /src/requirements.txt

COPY s3sync /src/s3sync
RUN cd /src ; pyinstaller --onefile -n s3sync s3sync/main.py

FROM alpine:3.7
COPY --from=builder /src/dist/s3sync /usr/local/bin/s3sync
