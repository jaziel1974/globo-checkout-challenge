FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/checkout-worker-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
