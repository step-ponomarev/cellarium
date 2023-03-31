FROM openjdk:17-jdk
COPY ./build/libs/cellarium.jar app.jar
CMD ["java", "--add-modules", "jdk.incubator.foreign", "-jar", "app.jar"]
