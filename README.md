# ✈️ FlightStream — Distributed Telemetry Mesh

A high-concurrency, event-driven backend built to simulate and monitor real-time aircraft telemetry. This project demonstrates enterprise-grade data ingestion, stream processing, and time-series storage across a fully dockerized microservices architecture.

---

## 📋 Table of Contents

1. [Project Goal](#1-project-goal)
2. [System Architecture](#2-system-architecture)
3. [Tech Stack & Why](#3-tech-stack--why)
4. [Local Setup](#4-local-setup)
5. [Load Test Results](#5-load-test-results)
6. [Architectural Decision Records](#6-architectural-decision-records-adrs)
7. [What I Learned](#7-what-i-learned)
8. [Bugs & How I Fixed Them](#8-bugs--how-i-fixed-them)
9. [Future Improvements](#9-future-improvements)

---

## 1. Project Goal

Build a resilient distributed system capable of handling high-velocity telemetry data from 1,000+ simulated aircraft simultaneously. The focus is on:

- **System Reliability** — no data loss even after consumer restarts
- **Real-Time Intelligence** — detect dangerous flight events within 500ms of ingestion
- **Modern Java Concurrency** — leverage Java 21 Virtual Threads for scalability
- **Silent Flight Detection** — identify aircraft that lose connectivity
- **Live Data** — process real aircraft telemetry from the OpenSky Network API

### Success Criteria

| Criteria | Target | Result |
|---|---|---|
| Infrastructure Resilience | No data loss after consumer restart | ✅ Kafka offsets preserved |
| Real-Time Alerting | Safety events detected within 500ms | ✅ Alerts firing in real-time |
| Scalability | 1,000 concurrent flight streams | ✅ Stable at 1,000 streams |
| Silent Detection | Connection lost alert after 30s | ✅ Redis TTL working |
| Live Aircraft Data | Real flights from OpenSky API | ✅ 5,000+ global aircraft processed |

---

## 2. System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      FlightStream System                         │
│                                                                  │
│  ┌─────────────────┐     ┌──────────────┐     ┌─────────────┐   │
│  │  Java Producer  │────▶│              │────▶│             │   │
│  │  (1000 Virtual  │JSON │    Kafka     │     │  Spring     │   │
│  │   Threads)      │     │   Broker     │     │  Boot       │   │
│  └─────────────────┘     │   :9092      │     │  Consumer   │   │
│                           │              │     │             │   │
│  ┌─────────────────┐     │              │     └──────┬──────┘   │
│  │ OpenSky Producer│────▶│              │            │          │
│  │  (Live Aircraft │     └──────────────┘            │          │
│  │   via OAuth2)   │                      ┌──────────┤          │
│  └─────────────────┘                      │          │          │
│                               ┌───────────▼──┐  ┌────▼───────┐ │
│                               │  TimescaleDB │  │  Safety    │ │
│                               │  (pg16:5434) │  │  Alert     │ │
│                               └──────────────┘  │  Engine    │ │
│                                                  └─────┬──────┘ │
│                                                        │         │
│                                               ┌────────▼──────┐ │
│                                               │     Redis     │ │
│                                               │  Heartbeat    │ │
│                                               │ Monitor :6379 │ │
│                                               └───────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

**Java Producer** simulates 1,000 aircraft via Virtual Threads, each sending telemetry every second.

**OpenSky Producer** fetches real live aircraft from the OpenSky Network REST API every 20 seconds using OAuth2 Bearer token authentication. No changes to the consumer were required.

**Kafka** acts as the message highway. Decouples producers from consumers. Preserves all messages via offsets — if the consumer crashes, it resumes exactly where it left off.

**Spring Boot Consumer** listens to the `flight-telemetry` topic, deserializes JSON via Jackson, persists every record to TimescaleDB, and passes each message through the alert pipeline.

**Safety Alert Engine** maintains an in-memory HashMap tracking the last reading per `flight_id`. Fires a `RAPID DESCENT ALERT` if altitude drops 2,000+ feet in under 10 seconds.

**Redis Heartbeat Monitor** sets a 35-second TTL key per flight on every message. A `@Scheduled` job checks every 10 seconds for expired keys — triggering a `CONNECTION LOST` alert for silent aircraft.

---

## 3. Tech Stack & Why

| Technology | Role | Why Chosen |
|---|---|---|
| **Apache Kafka** | Message broker | Decouples producer/consumer, guarantees no data loss via offsets |
| **Spring Boot 3** | Consumer application | Production-grade Java framework, familiar ecosystem |
| **Java 21 Virtual Threads** | Concurrency model | 1,000 threads at near-zero memory cost vs ~1MB per platform thread |
| **TimescaleDB (pg16)** | Time-series storage | Auto-partitions by timestamp (Hypertable), optimized for append-heavy workloads |
| **Redis 8** | Heartbeat state | Sub-millisecond TTL operations — far faster than querying TimescaleDB |
| **OpenSky Network API** | Live flight data | Real aircraft telemetry via OAuth2 — 5,000+ global aircraft |
| **Docker Compose** | Infrastructure | Reproducible local environment for all services |
| **Jackson** | JSON parsing | Industry-standard Java JSON library |
| **Spring Data JPA** | DB persistence | Eliminates boilerplate SQL for entity management |

---

## 4. Local Setup

### Prerequisites

- Docker Desktop (running)
- Java 21+
- Maven

### Step 1 — Start Infrastructure

```bash
docker-compose up -d
```

This starts 4 services: Zookeeper, Kafka, TimescaleDB, Redis. Verify all are running:

```bash
docker-compose ps
```

### Step 2 — Run Consumer

```bash
cd consumer
./mvnw spring-boot:run
```

### Step 3a — Run Simulated Producer (1,000 Virtual Threads)

```bash
cd producer
./mvnw spring-boot:run
```

### Step 3b — Run OpenSky Producer (Live Aircraft)

Requires an OpenSky Network account and API client credentials. Add to `opensky-producer/src/main/resources/application.yml`:

```yaml
opensky:
  clientId: YOUR_CLIENT_ID
  clientSecret: YOUR_CLIENT_SECRET
```

Then run:

```bash
cd opensky-producer
./mvnw spring-boot:run
```

### Step 4 — Verify

```bash
docker exec -it timescaledb psql -U admin -d flightstream
SELECT flight_id, altitude, latitude, longitude, timestamp FROM telemetry ORDER BY timestamp DESC LIMIT 10;
```

---

## 5. Load Test Results

| Metric | Result |
|---|---|
| Concurrent flight streams | 1,000 |
| Concurrency model | Java 21 Virtual Threads |
| Records persisted to TimescaleDB | 29,233+ |
| Consumer errors | 0 |
| Live aircraft processed (OpenSky) | 5,000+ globally |
| Rapid descent alerts | Firing in real-time |
| Silent flight detection | TTL expiry confirmed at 35s |
| Infrastructure stability | Fully stable, no restarts required |

---

## 6. Architectural Decision Records (ADRs)

### ADR-001 — Kafka over REST Polling

**Status:** Accepted

**Context:** The system must monitor 1,000+ aircraft telemetry streams in real-time. Two options: REST-based polling (Pull) vs asynchronous event-driven (Push).

**Decision:** Asynchronous Push model using Apache Kafka as the message broker.

**Consequences:**

✅ Decouples producers from consumers — consumer can restart without data loss

✅ Handles bursty data without crashing the backend

✅ Kafka offsets guarantee no message loss on consumer restart

⚠️ Increases infrastructure complexity — requires managing Zookeeper + Kafka containers

---

### ADR-002 — Java 21 Virtual Threads over WebFlux

**Status:** Accepted

**Context:** Processing 1,000+ simultaneous connections requires a highly efficient threading model. Two options: Reactive programming (WebFlux) or Java 21 Virtual Threads (Project Loom).

**Decision:** Java 21 Virtual Threads for both the producer and consumer.

**Consequences:**

✅ 1,000 threads at near-zero memory cost vs ~1GB for platform threads

✅ Simpler, imperative code — no reactive chains or callback complexity

✅ No learning curve of reactive programming paradigm

⚠️ Requires JDK 21+ — not backwards compatible

---

### ADR-003 — TimescaleDB over plain PostgreSQL

**Status:** Accepted

**Context:** Telemetry data is append-only and deeply tied to timestamps. Standard PostgreSQL struggles with index bloat and slow analytical queries on large time-stamped datasets.

**Decision:** TimescaleDB (PostgreSQL extension) with Hypertable on the `timestamp` column.

**Consequences:**

✅ Full SQL compatibility — no new query language to learn

✅ Automatic time-based partitioning via Hypertable

✅ Optimized for write-heavy time-series workloads

⚠️ Adds a specific dependency beyond standard PostgreSQL

---

### ADR-004 — Redis TTL for Heartbeat Detection

**Status:** Accepted

**Context:** Aircraft may lose connectivity silently. The system needs to detect flights that stop sending data within 30 seconds. Two options: scheduled TimescaleDB query vs Redis TTL.

**Decision:** Redis with a 35-second TTL per `flight:{id}:last_seen` key, checked every 10 seconds by a `@Scheduled` job.

**Consequences:**

✅ Sub-millisecond key operations — no DB query overhead

✅ Automatic TTL expiry handled by Redis — no manual cleanup code

✅ Scales to 1,000 flights trivially

⚠️ Heartbeat state lost on Redis restart — acceptable for this use case

---

### ADR-005 — RestTemplate over OpenSky Java Library

**Status:** Accepted

**Context:** The official OpenSky Java library (`opensky-api 1.3.0`) targets Java 7 and uses an outdated HTTP client incompatible with JDK 21+. It also predates OpenSky's OAuth2 authentication requirement.

**Decision:** Bypass the library entirely. Use Spring's `RestTemplate` to call the OpenSky REST API directly with OAuth2 Bearer token authentication.

**Consequences:**

✅ Full control over authentication headers and token refresh

✅ Compatible with any JDK version

✅ Easier to maintain and debug

⚠️ Requires manual response parsing — OpenSky returns states as arrays, not objects

---

## 7. What I Learned

### Kafka
Kafka is not just a message queue — it is a distributed log. Offsets make data loss impossible as long as messages are retained. A consumer can crash, restart, and resume from exactly where it left off. This is fundamentally different from traditional queues where messages disappear once consumed.

Topics are named channels. Partitions are buckets within a topic. Messages with the same key always go to the same partition, guaranteeing order per key. Multiple consumer instances form a consumer group — Kafka distributes partitions across them, enabling horizontal scaling.

### Java 21 Virtual Threads
Platform threads are OS-managed and expensive (~1MB each). Virtual threads are JVM-managed and lightweight — thousands can run with negligible memory overhead. Virtual threads are cheap enough to afford one per task, even for tasks that spend most of their time waiting.

### TimescaleDB
Standard relational databases store data in a flat table. As time-series data grows, queries slow down because the index spans the entire table. TimescaleDB's Hypertable automatically partitions data into time-based chunks — a query for "last 10 minutes" only scans the relevant chunk.

### Redis
Redis is an in-memory key-value store with built-in TTL. Setting a TTL means Redis deletes a key automatically after a specified duration. The TTL expiry IS the detection mechanism — no background cleanup code required.

### OAuth2 Client Credentials Flow
OpenSky deprecated basic authentication in favour of OAuth2. Exchange `client_id` and `client_secret` for a Bearer token at the auth endpoint, then pass that token as `Authorization: Bearer {token}` on every API call. Tokens expire after 30 minutes.

### Docker Compose
`docker-compose.yml` orchestrates multiple services as a single unit. Each service is isolated but communicates via service names within the Docker network. Port mappings expose services to the local machine.

### Spring Boot
`@KafkaListener` abstracts all consumer loop logic. Jackson's `ObjectMapper` is the bridge between raw JSON strings and Java objects — used manually in Kafka consumers since there is no HTTP layer doing the conversion automatically. `CommandLineRunner` executes code after the Spring context is fully initialized.

---

## 8. Bugs & How I Fixed Them

| Bug | Cause | Fix |
|---|---|---|
| `pull access denied for confluentic` | Typo in image name — missing `n` | Fixed to `confluentinc/cp-zookeeper` |
| `Bind for 0.0.0.0:5432 failed` | Local PostgreSQL already using port 5432 | Mapped TimescaleDB to host port 5434 |
| `No resolvable bootstrap urls` | YAML indentation wrong — Kafka config not under `spring:` | Fixed indentation — YAML is whitespace-sensitive |
| `UnrecognizedPropertyException: timestamp` | Getter named `getTimeStamp()` did not match JSON key `timestamp` | Renamed to `getTimestamp()`, added `@JsonIgnoreProperties` |
| Safety alert never fired | `flightReadings.put()` placed after early return — first message never stored | Moved `put()` inside the null check block |
| `Producer closed while send in progress` | Spring shut down before virtual threads finished sending | Added `Thread.currentThread().join()` to block main thread |
| `opsForSet()` wrong Redis type | Confused Redis Set collection with simple key-value store | Changed to `opsForValue().set()` with TTL overload |
| OpenSky library charset error | Library targets Java 7, incompatible with JDK 21+ | Bypassed library, used RestTemplate directly |
| Port 8080 already in use | Consumer running on 8080 | Added `server.port: 8081` to OpenSky producer yml |

---

## 9. Future Improvements

- **Multiple Kafka Partitions** — Scale to 6 partitions with 3 parallel consumer instances to demonstrate partition-based horizontal scaling
- **CQRS** — Separate read (Redis cache) and write (TimescaleDB) models for high-throughput query performance
- **Event Sourcing** — Store every state change as an immutable event, enabling full flight history replay
- **WebSocket Dashboard** — Real-time frontend showing live flight positions and alert notifications
- **Testcontainers** — Integration tests that spin up real Kafka and TimescaleDB containers in CI
- **Prometheus Metrics** — Expose message throughput, alert latency, and consumer lag metrics
- **Kafka KRaft Mode** — Remove Zookeeper dependency using Kafka 4.0's built-in coordination
