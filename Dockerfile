# Use an official OpenJDK image with JDK for compilation
FROM openjdk:8-jdk-alpine

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the Java application files into the container at the working directory
COPY src/main/java/myapps/*.java .
COPY src/main/resources/kafkastreams.properties .

# Compile the Java application
RUN javac -cp "target/" -d . KafkaApp.java

# Specify the command to run on container startup
CMD ["java", "-cp", ".:path-to-kafka-streams-lib/*", "myapps.KafkaApp"]

