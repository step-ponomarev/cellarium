FROM openjdk:17-jdk
COPY ./build/libs/cellarium.jar app.jar
COPY log4j.properties log4j.properties

RUN mkdir /conf

CMD ["java", "--add-modules", "jdk.incubator.foreign", "-jar", "app.jar"]
