FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured-to-geoserver"
LABEL nl.pdok.version=$version

COPY artifacts/featured-to-geoserver-web.jar /opt
WORKDIR /opt
CMD ["java", "-jar", "featured-to-geoserver-web.jar"]