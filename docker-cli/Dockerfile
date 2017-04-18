FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured-to-geoserver"
LABEL nl.pdok.version=$version

COPY artifacts/featured-to-geoserver-cli.jar /opt
WORKDIR /opt
ENTRYPOINT ["java", "-jar", "featured-to-geoserver-cli.jar"]
