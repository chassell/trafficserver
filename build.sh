#!/bin/sh

NAME="git describe --long"
TAG=`${NAME} | cut -d '-' -f 1`
DISTANCE=`${NAME} | cut -d '-' -f 2`
COMMIT=`${NAME} | cut -d '-' -f 3 | sed 's/g//'`

rpmbuild -bb --define "tag ${TAG}" --define "distance ${DISTANCE}" --define "commit ${COMMIT}" trafficserver.spec
