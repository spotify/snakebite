FROM dockerfile/java:oracle-java7

# Push setup_env script
ADD ./ci/setup_env.sh /setup_env.sh
# Download tarballs for both hdp and cdh
RUN /setup_env.sh --only-download --distro hdp
RUN /setup_env.sh --only-download --distro cdh
# since py2.6 is deprecated in Ubuntu image, we need to add
# ppa to install it
RUN add-apt-repository --yes ppa:deadsnakes/ppa
RUN apt-get update
RUN apt-get install --yes python2.7 python2.6
# Fetch pip for python requirments
RUN curl -SL 'https://bootstrap.pypa.io/get-pip.py' | python2.7
WORKDIR /
