FROM gradle:8.10-jdk21-alpine AS builder
WORKDIR /app
COPY . .
RUN gradle build -x test

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar app.jar
# 컨테이너 포트 (sendmock은 8081 포트를 사용한다고 가정)
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
