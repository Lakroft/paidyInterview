# Multi-stage build for Scala SBT project
FROM eclipse-temurin:17-jdk AS builder

# Install SBT
RUN apt-get update && \
    apt-get install -y curl && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY project project
COPY build.sbt .
COPY src src

# Build the application and create fat JAR
RUN sbt clean assembly

# Runtime stage - minimal JRE image
FROM eclipse-temurin:17-jre

# Install basic utilities
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r forex && useradd -r -g forex forex

# Set working directory
WORKDIR /app

# Copy the JAR from builder stage
COPY --from=builder /app/target/scala-2.13/forex-mtl.jar app.jar

# Change ownership to non-root user
RUN chown forex:forex app.jar
USER forex

# Expose port (from application.conf)
EXPOSE 8080

# Run the application
CMD ["java", "-jar", "app.jar"]