# 🚆 TrainTravel - Scalable Distributed Train Reservation & Ticketing Platform

> A production-grade, microservices-based railway ticketing and reservation system built with event-driven architecture, distributed transactions (Saga Pattern), high-concurrency seat locking, and full Docker containerization.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Core Engineering Highlights](#-core-engineering-highlights)
  - [1. Saga Distributed Transaction Pattern](#1-saga-distributed-transaction-pattern)
  - [2. High-Concurrency Redis Seat Locking](#2-high-concurrency-redis-seat-locking)
  - [3. Low-Latency Search via Elasticsearch](#3-low-latency-search-via-elasticsearch)
  - [4. Event-Driven Microservices with Kafka](#4-event-driven-microservices-with-kafka)
- [Port Reference](#-port-reference)
- [Tech Stack](#-tech-stack)
- [Services Ecosystem](#-services-ecosystem)
- [API Gateway Reference](#-api-gateway-reference)
- [Kafka Topics Reference](#-kafka-topics-reference)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Docker Infrastructure](#1-start-infrastructure)
  - [Database Migrations](#2-database-migrations)
  - [Microservices Startup](#3-run-microservices)
  - [Frontend Client](#4-run-frontend-client)
- [License](#-license)
- [Author](#-author)

---

## 🎯 Overview

TrainTravel is an end-to-end distributed railway reservation platform engineered for fault tolerance, data consistency, and high-throughput transaction processing:

- **Microservices Architecture**: Database-per-service pattern ensuring complete schema isolation and independent deployability across all service domains.
- **Distributed Transactions**: Orchestrated Saga pattern guaranteeing data consistency across booking, inventory, payment, and notification systems with compensating rollbacks.
- **Seat Concurrency Control**: Distributed Redis locks with exponential retry backoff, ensuring strict seat exclusivity during high-load flash booking windows.
- **Real-Time Event Streaming**: Apache Kafka messaging pipeline for decoupled asynchronous notifications, payment webhook handling, and search index synchronization.
- **Unified API Gateway**: Centralized Fastify gateway managing JWT validation, rate limiting, route proxying, and circuit breaking.

---

## 🏗️ Architecture

```
                            ┌───────────────────────┐
                            │  Frontend (React 18)  │
                            │  Port 3000            │
                            └───────────┬───────────┘
                                        │
                            ┌───────────▼───────────┐
                            │   API Gateway         │
                            │   Port 4000           │
                            │ (JWT, Rate Limiting,  │
                            │  Circuit Breaking)    │
                            └───┬───────┬───────┬───┘
              ┌─────────────────┼───────┼───────┼───────────────┐
              │                 │       │       │               │
   ┌──────────▼──────┐ ┌────────▼─┐ ┌───▼────┐ ┌▼──────────┐ ┌──▼──────────┐
   │ User Service    │ │ Search   │ │ Admin  │ │ Booking   │ │ Payment     │
   │ Port 4001       │ │ 4002     │ │ 4003   │ │ 4005      │ │ 4006        │
   └────────┬────────┘ └────┬─────┘ └───┬────┘ └─────┬─────┘ └─────┬───────┘
            │               │           │            │             │
            │          ┌────▼──────┐    │     ┌──────▼─────┐       │
            │          │ Inventory │    │     │ Notification│      │
            │          │ 4007      │    │     │ 4004 (Kafka)│      │
            │          └────┬──────┘    │     └──────┬──────┘      │
            │               │           │            │             │
            └───────────────┴───────────┴────────────┴─────────────┘
                                      │
                ┌─────────────────────┼─────────────────────┐
                │                     │                     │
        ┌───────▼──────┐    ┌─────────▼────────┐   ┌────────▼────────┐
        │  PostgreSQL  │    │  Redis Stack     │   │  Apache Kafka   │
        │  Port 5432   │    │  Port 6379/8001  │   │  Port 9092/9093 │
        └──────────────┘    └──────────────────┘   └─────────────────┘
                                                            │
                                                  ┌─────────▼─────────┐
                                                  │  Elasticsearch    │
                                                  │  Port 9200        │
                                                  └───────────────────┘
```

---

## ⚙️ Core Engineering Highlights

### 1. Saga Distributed Transaction Pattern
Booking a train ticket spans multiple distinct microservices (Booking, Inventory, Payment, Notification). In a microservices architecture, traditional 2-Phase Commit (2PC) creates blocking bottlenecks. TrainTravel implements an orchestrated **Saga Pattern**:
- **Initiation**: The user initiates a booking. `BookingService` creates a `PENDING` booking and dispatches an inventory reservation request.
- **Seat Lock**: `InventoryService` acquires a temporary seat reservation.
- **Payment Verification**: `PaymentService` captures the Razorpay transaction.
- **Compensation & Rollback**: If payment fails, the Saga automatically triggers compensating actions: releasing the inventory lock, transitioning the booking status to `FAILED`, and publishing an event to notify the user.

### 2. High-Concurrency Redis Seat Locking
During peak booking windows (e.g., Tatkal ticket sales), thousands of concurrent requests attempt to reserve the same seat:
- **Exclusivity**: Acquires atomic Redis distributed locks on `seat:{trainId}:{date}:{coach}:{seatNumber}` with an automatic 10-minute expiration TTL.
- **Exponential Backoff**: Concurrent threads retry lock acquisition using randomized jitter backoff, preventing thundering herd problems.
- **Deterministic Release**: Lock keys are released only by the holding process via cryptographic token validation.

### 3. Low-Latency Search via Elasticsearch
Direct relational queries joining train stations, routes, and schedules across millions of journeys degrade database performance.
- TrainTravel indexes pre-computed route hops, intermediate stations, and departure times in an **Elasticsearch 8 cluster**.
- Changes to train routes in `AdminService` publish Kafka events consumed by `SearchService` to keep search indexes synchronized in real time.

### 4. Event-Driven Microservices with Kafka
High-throughput asynchronous communication handled via Apache Kafka:
- Dedicated consumer groups for email notifications, payment webhooks, and booking audit logging.
- Dead-Letter Queues (DLQ) configured for poison-pill isolation and resilient message reprocessing.

---

## 🔌 Port Reference

| Service / Resource | Port | Protocol | Description |
|---|---|---|---|
| **API Gateway** | `4000` | HTTP / REST | Public entry point with rate limiting & reverse proxy |
| **User Service** | `4001` | HTTP / REST | User profiles, authentication & OTP validation |
| **Search Service** | `4002` | HTTP / REST | Elasticsearch queries & route lookups |
| **Admin Service** | `4003` | HTTP / REST | Stations, trains, coaches, schedules & fare rules |
| **Notification Service** | `4004` | Kafka Consumer | Asynchronous email delivery via SendGrid / Nodemailer |
| **Booking Service** | `4005` | HTTP / Kafka | Booking state machine & Saga orchestrator |
| **Payment Service** | `4006` | HTTP / REST | Razorpay payment intent & webhook ingestion |
| **Inventory Service** | `4007` | HTTP / REST | Real-time seat matrix & Redis distributed locking |
| **Frontend UI** | `3000` | HTTP / React | React 18 client application |
| **PostgreSQL** | `5432` | TCP | Relational database cluster for services |
| **Redis Stack** | `6379`, `8001` | TCP / HTTP | In-memory cache, distributed locks & RedisInsight |
| **Apache Kafka** | `9092`, `8080` | TCP / HTTP | Event streaming broker & Kafka UI management |
| **Elasticsearch** | `9200` | HTTP | Distributed search engine |

---

## 🛠️ Tech Stack

| Domain | Technology |
|---|---|
| **Runtime & Frameworks** | Node.js (v20+), Express.js, Fastify |
| **Frontend Architecture** | React 18, Vite, Tailwind CSS, Zustand, Lucide Icons |
| **Databases** | PostgreSQL 15, Redis 7 (Stack), Elasticsearch 8 |
| **ORM & Database Client** | Prisma ORM |
| **Message Streaming** | Apache Kafka (KafkaJS) |
| **Payment Processing** | Razorpay Node.js SDK & Webhooks |
| **Email Delivery** | SendGrid / Nodemailer |
| **Containerization** | Docker, Docker Compose |

---

## 📡 API Gateway Reference

All external client traffic routes through the API Gateway at `http://localhost:4000`:

| Method | Endpoint | Service Route | Auth Required | Description |
|---|---|---|---|---|
| `POST` | `/api/v1/auth/signup` | User Service | No | Register new user profile |
| `POST` | `/api/v1/auth/login` | User Service | No | Authenticate user & return JWT tokens |
| `POST` | `/api/v1/auth/verify-otp` | User Service | No | Validate email OTP |
| `GET` | `/api/v1/search/trains` | Search Service | No | Search trains between stations & dates |
| `GET` | `/api/v1/inventory/seats` | Inventory Service | Yes | Fetch real-time seat availability matrix |
| `POST` | `/api/v1/bookings` | Booking Service | Yes | Initiate new ticket booking (Saga start) |
| `GET` | `/api/v1/bookings/my` | Booking Service | Yes | List current user's booking history |
| `POST` | `/api/v1/payments/order` | Payment Service | Yes | Create Razorpay payment order |
| `POST` | `/api/v1/payments/webhook` | Payment Service | No | Verify Razorpay payment signature |
| `POST` | `/api/v1/admin/trains` | Admin Service | Admin | Create or update train schedules |

---

## 📬 Kafka Topics Reference

All topics are centrally maintained in `shared/constants/kafka-topics.js`:

| Topic Name | Producer | Consumer(s) | Payload / Purpose |
|---|---|---|---|
| `notification.email.otp` | User Service | Notification Service | Dispatch login & registration OTP emails |
| `notification.email.booking_confirmed` | Booking Service | Notification Service | Deliver booking confirmation & PDF ticket |
| `booking.created` | Booking Service | Inventory Service, Payment Service | Saga step 1: Lock seats & prepare payment |
| `booking.confirmed` | Booking Service | Notification Service, Search Service | Saga success: Update records & notify user |
| `booking.cancelled` | Booking Service | Inventory Service | Saga rollback: Release locked seat inventory |
| `payment.success` | Payment Service | Booking Service | Trigger ticket issuance on captured payment |
| `payment.failed` | Payment Service | Booking Service | Trigger Saga compensation on failed transaction |
| `search.index.sync` | Admin Service | Search Service | Re-index train routes in Elasticsearch |

---

## 📂 Project Structure

```
TrainTravel/
├── admin-service/             # Stations, routes, trains & schedule management
├── api-gateway/               # Fastify reverse proxy, JWT validation, rate limiting
├── booking-service/           # Booking state machine & Saga orchestrator
├── frontend/                  # React 18 client application with Tailwind CSS
├── inventory-service/         # Real-time seat allocation & Redis distributed locks
├── notification-service/      # Kafka consumer for email dispatch
├── payment-service/           # Razorpay integration & webhook verification
├── search-service/            # Elasticsearch-backed journey lookup engine
├── shared/                    # Common constants, Kafka topics, error utilities
├── user-service/              # User registration, profiles, and authentication
├── docker-compose.yml         # Multi-container infrastructure orchestration
├── TrainTravel_Master_Guide.html # Comprehensive system architecture & interview guide
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- **Node.js (v18+)**
- **Docker Desktop** (with Docker Compose)
- **npm** (v9+)

### 1. Clone the Repository
```bash
git clone https://github.com/surajsinghania11292/TrainTravel.git
cd TrainTravel
```

### 2. Start Infrastructure
```bash
docker compose up -d
```
This spins up PostgreSQL (`5432`), Redis Stack (`6379`, `8001`), Apache Kafka (`9092`), Kafka UI (`8080`), and Elasticsearch (`9200`).

### 3. Database Migrations
Initialize database schemas for all Prisma services:
```bash
cd user-service && npx prisma migrate deploy && cd ..
cd admin-service && npx prisma migrate deploy && cd ..
cd inventory-service && npx prisma migrate deploy && cd ..
cd booking-service && npx prisma migrate deploy && cd ..
cd payment-service && npx prisma migrate deploy && cd ..
```

### 4. Run Microservices
```bash
# In separate terminal windows (or using npm-run-all / PM2):
npm run dev --prefix api-gateway
npm run dev --prefix user-service
npm run dev --prefix search-service
npm run dev --prefix admin-service
npm run dev --prefix notification-service
npm run dev --prefix booking-service
npm run dev --prefix payment-service
npm run dev --prefix inventory-service
```

### 5. Run Frontend Client
```bash
cd frontend
npm install
npm run dev
```
Navigate to `http://localhost:3000` to launch the TrainTravel platform.

---

## 📄 License

Distributed under the **MIT License**.

---

## 👤 Author

**Suraj Kumar**
- GitHub: [@surajsinghania11292](https://github.com/surajsinghania11292)
- Email: [surajsinghania11292@gmail.com](mailto:surajsinghania11292@gmail.com)
