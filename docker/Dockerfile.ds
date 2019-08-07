# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

# First stage: the build environment
FROM alpine:3.8 AS builder
MAINTAINER Apache NiFi <dev@nifi.apache.org>

# Install the system dependencies needed for a build
RUN apk --update --no-cache upgrade && apk --update --no-cache add gcc \
	g++ \
	make \
	bison \
	flex \
	flex-dev \
	autoconf \
	libtool \
	wget \
	gdb \
	musl-dev \
	boost-dev \
	vim \
	util-linux-dev \
	curl-dev \
	cmake \
	git \
	nss \
	nss-dev \
	unzip \
	gpsd-dev \
	libressl-dev \
	zlib-dev \
	bzip2-dev \
	python3-dev

ENV USER minificpp
ENV MINIFI_BASE_DIR /opt/minifi

# Setup minificpp user
RUN addgroup -g 1000 $USER && adduser -u 1000 -D -G $USER -g "" $USER
RUN mkdir -p $MINIFI_BASE_DIR 
ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH $PATH:/usr/lib/jvm/default-jvm/bin

ADD . /usr/local/src/minifi
RUN chown -R $USER:$USER /usr/local/src/minifi
RUN chown -R $USER:$USER ${MINIFI_BASE_DIR}

USER $USER

# Perform the build
RUN cd /usr/local/src/minifi \
	&& mkdir build \
	&& cd build \
	&& cmake -DOPENSSL_FORCE_SHARED=true -DDISABLE_JEMALLOC=ON -DSTATIC_BUILD= -DSKIP_TESTS=true -DENABLE_DATASCIENCE=ON .. \
	&& make -j8 package \
	&& tar -xzvf /usr/local/src/minifi/build/nifi-minifi-cpp-0.7.0-bin.tar.gz -C $MINIFI_BASE_DIR

# Second stage: the runtime image
# Edge required for rocksdb
FROM alpine:3.8

# Add testing repo for rocksdb
RUN echo 'http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk --update --no-cache upgrade && apk add --update --no-cache \
	util-linux \
	curl \
	unzip \
	gpsd \
	nss \
	nss-dev \
	libressl \
	python3 \
	zlib

# Start MiNiFi CPP in the foreground
ENV USER minificpp
ENV MINIFI_BASE_DIR /opt/minifi
ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH $PATH:/usr/lib/jvm/default-jvm/bin
RUN addgroup -g 1000 $USER && adduser -u 1000 -D -G $USER -g "" $USER
RUN mkdir -p $MINIFI_BASE_DIR 

# Copy built minifi distribution from builder
COPY --from=builder ${MINIFI_BASE_DIR}/nifi-minifi-cpp-0.7.0 ${MINIFI_BASE_DIR}
RUN chown -R ${USER}:${USER} /opt/minifi

USER $USER

VOLUME /opt/minifi/flow
WORKDIR /opt/minifi/flow
CMD ${MINIFI_BASE_DIR}/bin/minifi
