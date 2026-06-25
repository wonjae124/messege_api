plugins {
	kotlin("jvm") version "1.9.25"
	kotlin("plugin.spring") version "1.9.25"
	id("org.springframework.boot") version "3.5.14"
	id("io.spring.dependency-management") version "1.1.7"
	kotlin("plugin.jpa") version "1.9.25"
	jacoco
}

group = "com"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")
	implementation("org.springframework.boot:spring-boot-starter-data-redis")
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("com.github.f4b6a3:uuid-creator:5.3.3")
	implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.8.16")
	implementation("org.springframework.kafka:spring-kafka")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
	runtimeOnly("org.postgresql:postgresql")
	implementation("org.springframework.boot:spring-boot-starter-webflux")

	// Observability (Metrics, Tracing, Logs)
	implementation("io.micrometer:micrometer-registry-prometheus")
	implementation("io.micrometer:micrometer-tracing-bridge-otel")
	implementation("io.opentelemetry:opentelemetry-exporter-otlp")
	implementation("io.opentelemetry.instrumentation:opentelemetry-logback-appender-1.0:2.12.0-alpha")
	implementation("net.logstash.logback:logstash-logback-encoder:7.4")

	// Resilience4j (Circuit Breaker)
	implementation("io.github.resilience4j:resilience4j-spring-boot3:2.2.0")
	implementation("io.github.resilience4j:resilience4j-micrometer:2.2.0")
	implementation("io.github.resilience4j:resilience4j-reactor:2.2.0")

	// UUIDv7 Generator
	implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0")

	// ShedLock (Scheduler Distributed Lock)
	implementation("net.javacrumbs.shedlock:shedlock-spring:5.13.0")
	implementation("net.javacrumbs.shedlock:shedlock-provider-jdbc-template:5.13.0")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict")
	}
}

allOpen {
	annotation("jakarta.persistence.Entity")
	annotation("jakarta.persistence.MappedSuperclass")
	annotation("jakarta.persistence.Embeddable")
}

tasks.withType<Test> {
	useJUnitPlatform()
	maxHeapSize = "2048m"
	finalizedBy(tasks.jacocoTestReport) // report is always generated after tests run
}

tasks.jacocoTestReport {
	dependsOn(tasks.test) // tests are required to run before generating the report
	reports {
		xml.required.set(true)
		csv.required.set(false)
		html.required.set(true)
	}
}
