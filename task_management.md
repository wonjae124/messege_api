# Alarm API Project Task Management

This document tracks the tasks, progress, and roadmap for the Alarm API project.

---

## 🛠️ Technical Stack & Architecture
- **Language**: Kotlin
- **Framework**: Spring Boot 3.5.14
- **Build Tool**: Gradle (Kotlin DSL)
- **Architecture Pattern**: Hexagonal Architecture (Ports & Adapters)
- **API Endpoint Base Path**: `/alarms`
- **Key Endpoints**:
  - `POST /alarms` - Create a new alarm (triggers async message generation to Kafka)
  - `GET /alarms/{id}` - Retrieve details of an alarm
  - `POST /alarms/{id}/trigger` - Trigger an alarm execution immediately (synchronously)
- **Asynchronous Flow & Messaging**:
  - **Message Broker**: Apache Kafka
  - **Topic**: `alarm-jobs` (configured with multiple partitions)
  - **Partitioning**: Alarm ID used as the Partition Key to guarantee order and serial processing per alarm.
  - **Consumption & Idempotency**: `KafkaAlarmJobConsumer` consumes job triggers and applies `LockTemplate` distributed/database locking to prevent duplicate/concurrent deliveries (idempotency check).
- **Design Decisions**:
  - **Removal of Scheduling**: Reservation-related scheduling times (`scheduledAt`) have been completely removed from all domains, database models, DTOs, services, and tests. The API operates as a direct trigger-driven service.
- **Design Patterns**:
  - Factory Method (Alarm domain entity creation)
  - Template/Callback (LockTemplate & SimpleLockTemplate)
- **Dependencies**:
  - Spring Boot Starter Web & Actuator
  - Spring Boot Starter Data JPA (MySQL/H2)
  - Spring Boot Starter Data Redis
  - Spring Kafka (with Embedded Kafka for testing)
  - Springdoc OpenAPI Starter WebMVC UI (v2.8.16)
  - Mock API Integration for notifications (`MockApiNotificationAdapter`)

---

## 🗺️ Roadmap & Milestones

1. **Phase 1: Planning & Requirements Analysis**  (Completed)
   - Define API specifications, requirements, technical stack, and endpoints (e.g., base path `/alarms`).
2. **Phase 2: Architecture & System Design**  (Completed)
   - Design data models, database schema, hexagonal port/adapter architecture, Kafka messaging structure, and lock mechanisms for idempotency.
3. **Phase 3: Implementation & Design Patterns**  (Completed)
   - Set up project boilerplate, implement domain models, use cases, ports, adapters, design patterns, Kafka producers/consumers, and lock templates.
4. **Phase 4: Testing & Verification**  (Completed)
   - Write unit and integration tests, perform manual endpoint verification, create dedicated grader tests (including asynchronous Kafka flows), and ensure all tests pass.

---

## 📋 Task Checklist

### Phase 1: Planning & Requirements Analysis
- [x] Plan Alarm API structure and define endpoints (configured at `/alarms` root for RESTful principles)
- [x] Determine technical stack and key dependencies (Kotlin, Spring Boot 3.5.14, Gradle, etc.)
- [x] Refine requirements to completely remove reservation scheduling (`scheduledAt`) across all layers

### Phase 2: Architecture & System Design
- [x] Design DB Schema / data persistence model for alarms (`AlarmEntity`, `AlarmExecutionLogEntity`)
- [x] Plan Hexagonal Architecture (domain, application ports/services, adapters)
- [x] Design synchronization/lock templates for thread-safe alarm execution
- [x] Design external notification port (`NotificationPort`) and Mock API integration strategy
- [x] Design Kafka messaging layout (producers, consumers, topics, partition keys for ordering)
- [x] Design async processing idempotency architecture using locks

### Phase 3: Implementation & Refactoring
- [x] Initialize project codebase and configure environment
- [x] Implement database connection and ORM setup (Spring Data JPA integration)
- [x] Implement core Domain Model and business rules (Specialized design for one-time alerts, removing `scheduledAt` fields)
- [x] Implement application logic using Ports & Use Cases (`CreateAlarmUseCase`, `TriggerAlarmUseCase`, etc.)
- [x] Build web adapter (`AlarmController` mapped to `/alarms` and `/alarms/{id}/trigger`)
- [x] Build persistence adapters (`JpaAlarmRepositoryAdapter`, etc.)
- [x] Implement notification out-adapter (`MockApiNotificationAdapter` integrated with external Mock API server)
- [x] Implement Kafka messaging adapters:
  - **Producer**: `KafkaAlarmJobProducer` publishing to `MessageQueuePort` (using Alarm ID as Partition Key)
  - **Consumer**: `KafkaAlarmJobConsumer` handling message-driven triggers asynchronously
- [x] Apply design patterns:
  - **Factory Method** for creating Alarm domain instances
  - **Template/Callback** pattern for transaction/lock management (`LockTemplate`)

### Phase 4: Testing & Verification
- [x] Set up testing framework (JUnit 5 and `spring-kafka-test` configured)
- [x] Implement unit tests for core Alarm domain logic (`AlarmTest`)
- [x] Implement integration tests for API endpoints (`AlarmControllerTest` targeting `/alarms` and `/alarms/{id}/trigger`)
- [x] Create a dedicated test suite verifying the immediate trigger function for the grader (`AlarmImmediateTriggerTest.kt`)
- [x] Create an integration test suite validating asynchronous Kafka execution and idempotency (`AlarmKafkaIntegrationTest.kt` with `@EmbeddedKafka`)
- [x] Verify all test suites pass successfully
