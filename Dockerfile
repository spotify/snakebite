FROM ravwojdyla/snakebite_test:base

ADD . /snakebite
WORKDIR /snakebite
RUN pip install -r requirements-dev.txt
