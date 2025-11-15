FROM openjdk:17.0.1-jdk-slim

RUN apt-get update \
    && apt-get install -y \
        curl \
        libxrender1 \
        libjpeg62-turbo \
        fontconfig \
        libxtst6 \
        xfonts-75dpi \
        xfonts-base \
        xz-utils

COPY cb-ext-course-service-0.0.1-SNAPSHOT.jar /opt/
CMD ["/bin/bash", "-c", "java -XX:+PrintFlagsFinal $JAVA_OPTIONS -XX:+UnlockExperimentalVMOptions -jar /opt/cb-ext-course-service-0.0.1-SNAPSHOT.jar"]
