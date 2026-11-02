# Misma receta para los dos servicios: docker build --build-arg MODULE=producer|processor .
# Compila con el JDK y ejecuta con solo el JRE: imagen final más pequeña.
FROM eclipse-temurin:21-jdk AS build
ARG MODULE
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY events/pom.xml events/
COPY producer/pom.xml producer/
COPY processor/pom.xml processor/
RUN ./mvnw -B -q -pl "$MODULE" -am dependency:go-offline -Dmaven.main.skip || true
COPY events/src events/src
COPY "$MODULE/src" "$MODULE/src"
RUN ./mvnw -B -q -pl "$MODULE" -am package -DskipTests && cp "$MODULE"/target/*.jar app.jar

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 1001 app && mkdir -p /app/state && chown -R app /app
USER app
WORKDIR /app
COPY --from=build /src/app.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
