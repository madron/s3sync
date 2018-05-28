FROM madron/pyinstaller-alpine:3.6.5 as builder

COPY requirements /src/requirements
RUN pip3 install -r /src/requirements/test.txt

COPY main.py /src
COPY s3sync /src/s3sync
RUN cd /src ; pyinstaller --onefile -n s3sync main.py

FROM alpine:3.7
COPY --from=builder /src/dist/s3sync /usr/local/bin/s3sync
