FROM docker.devlab.local/java8-libmesos:0.28
MAINTAINER IFM

ADD brigade.sh /
ADD brigade*tar.gz /

RUN chmod +x /brigade.sh

ENTRYPOINT ["/brigade.sh"]

