#!/bin/bash

set -ev
export VERSION=$(printf $(cat VERSION))

docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

cp artifacts/featured-to-geoserver-$VERSION-standalone.jar artifacts/featured-to-geoserver-cli.jar
docker build --build-arg version=$VERSION . -f docker-cli/Dockerfile -t pdok/featured-to-geoserver-cli:$VERSION
docker push pdok/featured-to-geoserver-cli:$VERSION

cp artifacts/featured-to-geoserver-$VERSION-web.jar artifacts/featured-to-geoserver-web.jar
docker build --build-arg version=$VERSION . -f docker-web/Dockerfile -t pdok/featured-to-geoserver-web:$VERSION
docker push pdok/featured-to-geoserver-web:$VERSION
