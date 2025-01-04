# Use a minimal openjdk image as a base
FROM openjdk:17-jdk-slim

# Set working directory inside the container
WORKDIR /app

# Copy the Maven build jar to the container (assuming it's built with `mvn clean package`)
COPY target/rabbitmq-hc-1.0-SNAPSHOT.jar /app/app.jar

# Expose the port your application will run on
EXPOSE 7000

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
