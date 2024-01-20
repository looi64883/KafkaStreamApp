# Use an official OpenJDK runtime as a parent image
FROM openjdk:8-jre-alpine

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the Java application files into the container at the working directory
COPY src/main/java/myapps/*.java .
COPY src/main/resources/kafkastreams.properties .

# Compile the Java application
RUN javac -cp "path-to-kafka-streams-lib/*" -d . KafkaApp.java

# Specify the command to run on container startup
CMD ["java", "-cp", ".:path-to-kafka-streams-lib/*", "myapps.KafkaApp"]
