FROM openjdk:17-jdk
COPY ./build/libs/cellarium.jar app.jar
RUN mkdir /conf

CMD ["java", "--add-modules", "jdk.incubator.foreign", "-jar", "app.jar"]
