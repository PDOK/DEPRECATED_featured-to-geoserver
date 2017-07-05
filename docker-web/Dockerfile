FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured-to-geoserver"
LABEL nl.pdok.version=$version

COPY artifacts/featured-to-geoserver-web.jar /opt
COPY run.sh /opt
RUN chmod 0500 /opt/run.sh
WORKDIR /opt
CMD ["./run.sh", "featured-to-geoserver-web.jar"]
