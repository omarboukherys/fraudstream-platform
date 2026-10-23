# Étape 1 : build du fat-jar avec sbt (tests unitaires inclus)
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.13_11_1.10.5_2.12.20 AS build
WORKDIR /build
COPY project ./project
COPY build.sbt .
RUN sbt -batch update
COPY src ./src
RUN sbt -batch "testOnly io.fraudstream.streaming.RiskScorerSpec" assembly

# Étape 2 : image Spark officielle (Scala 2.12, Java 17) qui lance le job en mode local[*]
# Sur Kubernetes, le même jar est soumis via spark-submit en mode cluster (voir k8s/).
FROM apache/spark:3.5.3-java17
USER root
RUN mkdir -p /data/lake /data/checkpoints && chown -R spark:spark /data
USER spark
COPY --from=build /build/target/scala-2.12/fraud-streaming-assembly.jar /opt/app/fraud-streaming.jar
ENV SPARK_DRIVER_MEMORY=1g
EXPOSE 4040
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--class", "io.fraudstream.streaming.FraudStreamingJob", \
  "--conf", "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
  "/opt/app/fraud-streaming.jar"]
