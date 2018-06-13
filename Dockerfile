FROM madron/pyinstaller-alpine:3.6.5 as builder

COPY requirements /src/requirements
RUN pip3 install -r /src/requirements/test.txt

COPY main.py /src
COPY s3sync /src/s3sync
# Tests
RUN cd /src; coverage run -m unittest discover
RUN cd /src; coverage report --skip-covered
RUN cd /src ; pyinstaller --hidden-import configparser --onefile -n s3sync main.py

FROM alpine:3.7
COPY --from=builder /src/dist/s3sync /usr/local/bin/s3sync
