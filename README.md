# 🚆 TrainTravel - Scalable Distributed Train Reservation & Ticketing Platform

> A production-grade microservices-based train booking and reservation system backend, built with event-driven architecture, distributed transactions, and high concurrency seat locking.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Port Reference](#-port-reference)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the Application](#running-the-application)
- [Services](#-services)
- [Kafka Topics](#-kafka-topics)
- [API Documentation](#-api-documentation)
- [Environment Variables](#-environment-variables)
- [Project Structure](#-project-structure)

---

## 🎯 Overview

TrainTravel is an end-to-end distributed railway reservation platform engineered for fault tolerance, data consistency, and high-throughput transaction processing:

- **Microservices Design Patterns** — Database-per-service, API Gateway, Saga Choreography/Orchestration.
- **Inter-Service Communication** — REST for synchronous queries and Apache Kafka for asynchronous event streams.
- **Authentication & Authorization** — JWT access + refresh tokens, email OTP verification, and role-based access control.
- **Database Management** — PostgreSQL via Prisma ORM for dedicated relational services, Elasticsearch for low-latency search.
- **Concurrency & Distributed Locking** — Redis distributed locking with auto-expiry to prevent seat double-booking.
- **Containerization & Orchestration** — Docker Compose for containerizing all databases, Redis, Kafka, and search infrastructure.
- **Resilience & Fault Tolerance** — Rate limiting, circuit breaking, and Dead-Letter Queues (DLQ) configured across all consumers.

---

## 🏗️ Architecture

```
                            ┌───────────────────────┐
                            │  Frontend (React)     │
                            │  Port 3000            │
                            └───────────┬───────────┘
                                        │
                            ┌───────────▼───────────┐
                            │   API Gateway         │
                            │   Port 4000           │
                            │ (JWT, rate limit,     │
                            │  circuit breaker)     │
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
        │  PostgreSQL  │    │  Redis Stack     │   │   Kafka         │
        │  Port 5432   │    │  Port 6379/8001  │   │   Port 9092/9093│
        └──────────────┘    └──────────────────┘   └─────────────────┘
                                                            │
                                                  ┌─────────▼─────────┐
                                                  │  Elasticsearch    │
                                                  │  Port 9200        │
                                                  └───────────────────┘
```

**Key Architectural Principles:**
- **API Gateway** acts as the single point of entry for the frontend, performing request aggregation, rate limiting, and JWT context injection.
- **Database-per-Service** guarantees strict data isolation across domain boundaries.
- **Event-Driven Saga Pattern** coordinates complex booking, payment, and inventory rollbacks asynchronously.
- **Distributed Locking Engine** leverages Redis with exponential backoff retry algorithms to guarantee strict seat exclusivity during high concurrency windows.

---

## 🔌 Port Reference

| Service / Resource | Port | Details |
|---|---|---|
| API Gateway | `4000` | Public REST entrypoint |
| User Service | `4001` | Auth, OTP, Profile |
| Search Service | `4002` | Elasticsearch train search |
| Admin Service | `4003` | Stations, Trains, Routes |
| Notification Service | `4004` | Kafka consumer (no HTTP) |
| Booking Service | `4005` | Booking Saga orchestrator |
| Payment Service | `4006` | Razorpay integration & webhooks |
| Inventory Service | `4007` | Seat matrix & distributed locks |
| Frontend | `3000` | React / Vite UI |
| PostgreSQL | `5432` | `admin` / `traintravelpass` |
| Redis Stack | `6379`, `8001` | Redis & RedisInsight dashboard |
| Kafka / UI | `9092`, `8080` | Kafka broker & UI for Apache Kafka |
| Elasticsearch | `9200` | Search cluster |

---

## 🛠️ Tech Stack

- **Runtime & Frameworks**: Node.js, Express.js, Fastify
- **Frontend**: React 18, Vite, Tailwind CSS, Zustand, React Router
- **Databases**: PostgreSQL 15, Redis 7 (Stack), Elasticsearch 8
- **ORM & Data Layer**: Prisma ORM
- **Messaging & Streaming**: Apache Kafka (KafkaJS)
- **Payment Gateway**: Razorpay Node SDK
- **Email Delivery**: SendGrid / Nodemailer
- **Containerization**: Docker, Docker Compose

---

## 🚀 Getting Started

### Prerequisites
- Node.js (v18+)
- Docker & Docker Compose
- npm or yarn

### 1. Start Infrastructure
```bash
docker compose up -d
```

### 2. Database Migrations
Run Prisma migrations for all Postgres-backed services:
```bash
# Run for each service
cd user-service && npx prisma migrate deploy && cd ..
cd admin-service && npx prisma migrate deploy && cd ..
cd inventory-service && npx prisma migrate deploy && cd ..
cd booking-service && npx prisma migrate deploy && cd ..
cd payment-service && npx prisma migrate deploy && cd ..
```

### 3. Run Microservices
```bash
# In separate terminal windows or with a process manager:
cd api-gateway && npm run dev
cd user-service && npm run dev
cd search-service && npm run dev
cd admin-service && npm run dev
cd notification-service && npm run dev
cd booking-service && npm run dev
cd payment-service && npm run dev
cd inventory-service && npm run dev
```

### 4. Run Frontend Client
```bash
cd frontend
npm install
npm run dev
```
Open `http://localhost:3000` to access the TrainTravel booking application.

---

## 📬 Kafka Topics Reference

All topics are centrally maintained in `shared/constants/kafka-topics.js`:
- `notification.email.otp`
- `notification.email.welcome`
- `notification.email.booking_confirmed`
- `notification.email.booking_failed`
- `booking.created`
- `booking.confirmed`
- `booking.cancelled`
- `payment.initiated`
- `payment.success`
- `payment.failed`
- `inventory.locked`
- `inventory.released`
- `search.index.sync`

---

## 🛡️ License
ISC
