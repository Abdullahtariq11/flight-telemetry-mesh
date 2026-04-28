# ✈️ FlightStream — Distributed Telemetry Mesh

A high-concurrency, event-driven backend built to simulate and monitor real-time aircraft telemetry. This project demonstrates enterprise-grade data ingestion, stream processing, and time-series storage across a fully dockerized microservices architecture.

---

## 📋 Table of Contents

1. [Project Goal](#1-project-goal)
2. [System Architecture](#2-system-architecture)
3. [Tech Stack & Why](#3-tech-stack--why)
4. [Local Setup](#4-local-setup)
5. [Load Test Results](#5-load-test-results)
6. [Architectural Decision Records (ADRs)](#6-architectural-decision-records-adrs)
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

### Success Criteria

| Criteria | Target | Result |
|---|---|---|
| Infrastructure Resilience | No data loss after consumer restart | ✅ Kafka offsets preserved |
| Real-Time Alerting | Safety events detected within 500ms | ✅ Alerts firing in real-time |
| Scalability | 1,000 concurrent flight streams | ✅ Stable at 1,000 streams |
| Silent Detection | Connection lost alert after 30s | ✅ Redis TTL working |

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    FlightStream System                       │
│                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────┐  │
│  │   Producer   │────▶│    Kafka     │────▶│  Consumer  │  │
│  │ (Java 21 +   │JSON │   Broker     │     │ (Spring    │  │
│  │  1000 VTs)   │     │   :9092      │     │   Boot)    │  │
│  └──────────────┘     └──────────────┘     └─────┬──────┘  │
│                                                   │         │
│                              ┌────────────────────┤         │
│                              │                    │         │
│                    ┌─────────▼──────┐   ┌─────────▼──────┐ │
│                    │  TimescaleDB   │   │ Safety Alert   │ │
│                    │  (pg16 :5434)  │   │    Engine      │ │
│                    └────────────────┘   └────────────────┘ │
│                                                   │         │
│                                         ┌─────────▼──────┐ │
│                                         │     Redis      │ │
│                                         │  Heartbeat     │ │
│                                         │ Monitor :6379  │ │
│                                         └────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Producer** — Simulates 1,000 aircraft via Java 21 Virtual Threads. Each flight sends telemetry (altitude, speed, lat/lon, timestamp) to Kafka every second.
2. **Kafka** — Acts as the message highway. Decouples producer from consumer. Preserves messages via offsets if the consumer goes down.
3. **Consumer** — Spring Boot app that listens to the `flight-telemetry` topic, deserializes JSON, persists to TimescaleDB, and runs the alert engine.
4. **TimescaleDB** — PostgreSQL with time-series optimization. Stores all telemetry as a Hypertable for fast time-based queries.
5. **Safety Alert Engine** — In-memory HashMap tracks last reading per flight. Fires a `RAPID DESCENT ALERT` if altitude drops 2,000+ feet in under 10 seconds.
6. **Redis Heartbeat Monitor** — Sets a 35-second TTL key per flight on every message. A scheduled job checks every 10 seconds for expired keys — triggering a `CONNECTION LOST` alert.

---

## 3. Tech Stack & Why

| Technology | Role | Why Chosen |
|---|---|---|
| **Apache Kafka** | Message broker | Decouples producer/consumer, guarantees no data loss via offsets |
| **Spring Boot 3** | Consumer application | Production-grade Java framework, familiar ecosystem |
| **Java 21 Virtual Threads** | Concurrency model | 1,000 threads at near-zero memory cost vs ~1MB per platform thread |
| **TimescaleDB (pg16)** | Time-series storage | Auto-partitions by timestamp (Hypertable), optimized for append-heavy workloads |
| **Redis 8** | Heartbeat state | Sub-millisecond TTL operations — far faster than querying TimescaleDB |
| **Docker Compose** | Infrastructure | Reproducible local environment for all 4 services |
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

This starts 4 services: Zookeeper, Kafka, TimescaleDB, Redis.

Verify all are running:

```bash
docker-compose ps
```

### Step 2 — Run Consumer

```bash
cd consumer
./mvnw spring-boot:run
```

### Step 3 — Run Producer

```bash
cd producer
./mvnw spring-boot:run
```

The producer will launch 1,000 virtual threads and begin streaming telemetry to Kafka immediately.

### Step 4 — Verify

Check TimescaleDB for persisted records:

```bash
docker exec -it timescaledb psql -U admin -d flightstream
SELECT COUNT(*) FROM telemetry;
```

---

## 5. Load Test Results

| Metric | Result |
|---|---|
| Concurrent flight streams | 1,000 |
| Concurrency model | Java 21 Virtual Threads |
| Records persisted to TimescaleDB | 29,233+ |
| Consumer errors | 0 |
| Rapid descent alerts | Firing in real-time |
| Silent flight detection | TTL expiry confirmed at 35s |
| Infrastructure | Fully stable — no restarts required |

---

## 6. Architectural Decision Records (ADRs)

### ADR-001 — Kafka over REST Polling

**Status:** Accepted

**Context:** The system must monitor 1,000+ aircraft telemetry streams in real-time. Two options considered: REST-based polling (Pull) vs asynchronous event-driven (Push).

**Decision:** Asynchronous Push model using Apache Kafka as the message broker.

**Consequences:**
- ✅ Decouples producers from consumers — consumer can restart without data loss
- ✅ Handles bursty data without crashing the backend
- ✅ Kafka offsets guarantee exactly-once delivery semantics
- ⚠️ Increases infrastructure complexity — requires managing Zookeeper + Kafka containers

---

### ADR-002 — Java 21 Virtual Threads over WebFlux

**Status:** Accepted

**Context:** Processing 1,000+ simultaneous connections requires a highly efficient threading model. Two options: Reactive programming (WebFlux) or Java 21 Virtual Threads (Project Loom).

**Decision:** Java 21 Virtual Threads for both the producer and consumer.

**Consequences:**
- ✅ 1,000 threads at near-zero memory cost (vs ~1GB for platform threads)
- ✅ Simpler, imperative code — no reactive chains
- ✅ No learning curve of reactive programming paradigm
- ⚠️ Requires JDK 21+ — not backwards compatible

---

### ADR-003 — TimescaleDB over plain PostgreSQL

**Status:** Accepted

**Context:** Telemetry data is append-only and deeply tied to timestamps. Standard PostgreSQL struggles with index bloat and slow analytical queries on large time-stamped datasets.

**Decision:** TimescaleDB (PostgreSQL extension) with Hypertable on the `timestamp` column.

**Consequences:**
- ✅ Full SQL compatibility — no new query language to learn
- ✅ Automatic time-based partitioning via Hypertable
- ✅ Optimized for write-heavy time-series workloads
- ⚠️ Adds a specific dependency beyond standard PostgreSQL

---

### ADR-004 — Redis TTL for Heartbeat Detection

**Status:** Accepted

**Context:** Aircraft may lose connectivity silently. The system needs to detect flights that stop sending data within 30 seconds. Two options: scheduled TimescaleDB query vs Redis TTL.

**Decision:** Redis with a 35-second TTL per `flight:{id}:last_seen` key, checked every 10 seconds by a `@Scheduled` job.

**Consequences:**
- ✅ Sub-millisecond key operations — no DB query overhead
- ✅ Automatic TTL expiry handled by Redis — no manual cleanup
- ✅ Scales to 1,000 flights trivially
- ⚠️ Heartbeat state lost on Redis restart (acceptable for this use case)

---

## 7. What I Learned

### Kafka
Kafka is not just a message queue — it's a distributed log. The key insight is that **offsets make data loss impossible** as long as messages are retained. A consumer can crash, restart, and resume from exactly where it left off. This is fundamentally different from traditional queues where messages disappear once consumed.

**Topics vs Partitions** — A topic is a named channel. Partitions are buckets within that topic. Messages with the same key always go to the same partition — guaranteeing order per key (per `flight_id` in this project).

**Consumer Groups** — Multiple consumer instances can form a group. Kafka distributes partitions across them, enabling horizontal scaling. Each partition is only read by one consumer in the group at a time.

### Java 21 Virtual Threads
Platform threads are OS-managed and expensive (~1MB each). Virtual threads are JVM-managed and lightweight — you can run thousands of them with negligible memory overhead. This project runs 1,000 virtual threads simultaneously with zero issues on a local machine.

The key mental model: virtual threads are cheap enough that you can afford one per task, even for tasks that spend most of their time waiting (like `Thread.sleep(1000)` in the producer).

### TimescaleDB
Standard relational databases store data in a flat table. As time-series data grows, queries slow down because the index spans the entire table. TimescaleDB's Hypertable automatically partitions data into "chunks" by time interval — so a query for "last 10 minutes" only scans the relevant chunk, not the entire table.

### Redis
Redis is an in-memory key-value store with built-in TTL (Time To Live). Setting a TTL on a key means Redis automatically deletes it after the specified duration. This makes it perfect for tracking "last seen" state — you don't need a background job to clean up stale data. The TTL expiry IS the detection mechanism.

### Docker Compose
`docker-compose.yml` orchestrates multiple services as a single unit. Key learning: each service is isolated but can communicate via service names (e.g., `zookeeper:2181` instead of `localhost:2181` within the Docker network). Port mappings (`host:container`) expose services to your local machine.

### Spring Boot
`@KafkaListener` abstracts all the Kafka consumer loop logic. Jackson's `ObjectMapper` is the bridge between raw JSON strings (what Kafka delivers) and Java objects. `CommandLineRunner` is the hook to execute code after Spring context is fully initialized.

---

## 8. Bugs & How I Fixed Them

| Bug | Cause | Fix |
|---|---|---|
| `pull access denied for confluentic/cp-zookeeper` | Typo in image name — missing `n` | Fixed to `confluentinc/cp-zookeeper` |
| `Bind for 0.0.0.0:5432 failed` | Local PostgreSQL already using port 5432 | Mapped TimescaleDB to host port 5434 |
| `No resolvable bootstrap urls` | YAML indentation wrong — Kafka config not under `spring:` | Fixed indentation — YAML is whitespace-sensitive |
| `UnrecognizedPropertyException: timestamp` | Java field named `timeStamp` (camelCase) didn't match JSON key `timestamp` | Renamed getter/setter to `getTimestamp()`, added `@JsonIgnoreProperties` |
| Safety alert never fired | `flightReadings.put()` was placed after early return — first message never stored in HashMap | Moved `put()` inside the null check block before returning |
| `Producer closed while send in progress` | Spring Boot shut down before virtual threads finished — `run()` returned immediately | Added `Thread.currentThread().join()` to block main thread |
| `opsForSet()` used instead of `opsForValue()` | Confused Redis Set type with simple key-value store | Changed to `opsForValue().set()` with TTL overload |

---

## 9. Future Improvements

- **OpenSky Network Integration** — Replace simulated producer with real live flight data from the OpenSky REST API (`opensky-network.org`). Zero consumer changes required.
- **WebSocket endpoint** — Stream alerts to a frontend dashboard in real-time
- **Multiple alert rules** — Low fuel simulation, airspace violation, speed anomaly
- **Kafka KRaft mode** — Remove Zookeeper dependency using Kafka 4.0's built-in coordination
- **Testcontainers** — Integration tests that spin up real Kafka + TimescaleDB containers
- **Metrics** — Expose Prometheus metrics for message throughput and alert latency