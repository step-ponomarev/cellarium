FROM openjdk:21-jdk

WORKDIR /app

COPY ./build/libs/cellarium.jar cellarium.jar
COPY log4j.properties log4j.properties

RUN mkdir conf

CMD ["java", "-jar", "cellarium.jar"]
