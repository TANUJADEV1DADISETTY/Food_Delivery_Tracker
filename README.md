````markdown
# Food Delivery Order Tracker with Apache Kafka

A real-time event-driven food delivery order tracking system built using **Apache Kafka**, **Docker**, and **Java**. This project demonstrates Kafka fundamentals such as producers, consumers, topics, partitions, consumer groups, offsets, and event streaming by simulating food delivery orders from placement to delivery.

---

# Table of Contents

- Project Overview
- Features
- System Architecture
- Technology Stack
- Project Structure
- Prerequisites
- Setup Instructions
- Running the Project
- Creating the Kafka Topic
- Running the Producer
- Running Consumer A
- Running Consumer B
- Running the Frontend
- API Documentation
- Kafka Message Format
- Message Key Rationale
- Consumer Groups
- Offset Management
- State Persistence
- Dashboard
- Testing
- Expected Output
- Troubleshooting
- Future Improvements
- Learning Outcomes

---

# Project Overview

Modern food delivery applications process thousands of real-time events every second.

When a customer places an order, several independent systems need to react immediately:

- Restaurant Notification
- Customer Tracking
- Delivery Assignment
- Analytics
- Notifications

Instead of directly communicating with each service, the application publishes events to Apache Kafka.

Kafka acts as a central event streaming platform.

In this project:

- The Producer simulates orders.
- Kafka stores the events.
- Consumer A tracks live order status.
- Consumer B performs analytics.
- A frontend dashboard displays active orders.

This architecture demonstrates the publish-subscribe pattern and event-driven microservices.

---

# Features

## Producer

- Simulates food delivery orders
- Generates realistic customer data
- Generates restaurant names
- Generates random menu items
- Publishes five order lifecycle events
- Uses order_id as Kafka key
- Retry mechanism for failures
- Supports configurable number of orders

---

## Consumer A (Status Tracker)

- Reads all order events
- Maintains active orders
- Removes delivered orders
- Saves state periodically
- Exposes REST API
- Supports dashboard

---

## Consumer B (Analytics)

Maintains live analytics such as

- Orders per restaurant
- Status frequency
- Message count
- Real-time console reporting

---

## Dashboard

Displays

- Active Orders
- Restaurant
- Current Status
- Ordered Items
- Progress Bar

Updates automatically every 2 seconds.

---

# System Architecture

```
                    Producer
                       │
                       │
                Publish Events
                       │
                       ▼
              Apache Kafka Broker
               Topic: order-events
          (Partition 0 | 1 | 2)
                 │            │
                 │            │
        Consumer A      Consumer B
     Status Tracker      Analytics
            │
            │
        REST API
        GET /state
            │
            ▼
      Frontend Dashboard
```

---

# Technology Stack

| Component        | Technology                     |
| ---------------- | ------------------------------ |
| Language         | Java                           |
| Build Tool       | Maven                          |
| Messaging        | Apache Kafka                   |
| Coordination     | ZooKeeper                      |
| Monitoring       | Kafdrop                        |
| REST API         | Spring Boot / Java HTTP Server |
| Serialization    | Jackson                        |
| Containerization | Docker                         |
| Frontend         | HTML CSS JavaScript            |

---

# Project Structure

```
food-delivery-tracker/

│
├── docker-compose.yml
│
├── producer/
│     ├── Producer.java
│     ├── Fixtures.java
│
├── consumer-status/
│     ├── ConsumerStatus.java
│     ├── ApiServer.java
│     ├── StateWriter.java
│     ├── state.json
│
├── consumer-analytics/
│     ├── ConsumerAnalytics.java
│
├── dashboard/
│     ├── index.html
│     ├── style.css
│     ├── app.js
│
├── README.md
├── LEARNINGS.md
├── pom.xml
└── src/
```

---

# Prerequisites

Install the following software.

- Java 21 or later
- Maven
- Docker Desktop
- Docker Compose
- Git

Verify installation

```
java -version

mvn -version

docker --version

docker compose version
```

---

# Setup Instructions

Clone repository

```
git clone <repository-url>

cd food-delivery-tracker
```

---

# Start Kafka

```
docker compose up -d
```

Verify containers

```
docker ps
```

Open

```
http://localhost:9008
```

Kafdrop should open successfully.

---

# Creating the Kafka Topic

Enter Kafka container

```
docker exec -it kafka bash
```

Create topic

```
kafka-topics.sh \
--create \
--topic order-events \
--partitions 3 \
--replication-factor 1 \
--bootstrap-server localhost:9892 \
--config retention.ms=3600000
```

Verify

```
kafka-topics.sh \
--describe \
--topic order-events \
--bootstrap-server localhost:9892
```

Expected

- Topic exists
- 3 partitions
- retention.ms = 3600000

---

# Running the Producer

Default

```
java Producer
```

Default creates

```
10 orders
```

Custom

```
java Producer --orders 5
```

Expected output

```
[SENT] ORD-001 -> PLACED

[SENT] ORD-001 -> CONFIRMED

[SENT] ORD-001 -> PREPARING

[SENT] ORD-001 -> OUT_FOR_DELIVERY

[SENT] ORD-001 -> DELIVERED
```

Every order generates exactly five events.

---

# Running Consumer A

Start

```
java ConsumerStatus
```

Consumer Group

```
status-tracker
```

Responsibilities

- Read events
- Update state
- Remove delivered orders
- Save state.json
- Start REST API

Console Example

```
[UPDATE]

ORD-001

PLACED -> CONFIRMED

[UPDATE]

ORD-001

CONFIRMED -> PREPARING

[COMPLETE]

ORD-001

Pizza Hut

Delivered
```

---

# Running Consumer B

Start

```
java ConsumerAnalytics
```

Consumer Group

```
analytics
```

Console Output

```
=======================

Restaurant Counts

Dominos : 15

Pizza Hut : 8

Burger King : 10

-----------------------

Status Counts

PLACED : 33

CONFIRMED : 33

PREPARING : 30

OUT_FOR_DELIVERY : 27

DELIVERED : 26

=======================
```

---

# Running the Dashboard

Navigate

```
dashboard/
```

Open

```
index.html
```

or

Serve with

```
python -m http.server 8080
```

Open browser

```
http://localhost:8080
```

Dashboard polls

```
GET /state
```

every

```
2 seconds
```

---

# API Documentation

## Get Current Active Orders

Endpoint

```
GET /state
```

Response

```json
{
  "ORD-001": {
    "restaurant": "Dominos",
    "status": "PREPARING",
    "items": ["Pizza", "Garlic Bread"]
  }
}
```

HTTP Status

```
200 OK
```

Content Type

```
application/json
```

---

# Kafka Message Format

Each Kafka event follows the structure below.

```json
{
  "order_id": "ORD-001",
  "customer_name": "Rahul",
  "restaurant": "Dominos",
  "items": ["Pizza", "Coke"],
  "status": "PLACED",
  "timestamp": "2026-08-03T12:40:10Z",
  "estimated_delivery_minutes": 25
}
```

Valid Status Values

- PLACED
- CONFIRMED
- PREPARING
- OUT_FOR_DELIVERY
- DELIVERED

---

# Message Key Rationale

The Kafka message key is

```
order_id
```

### Why?

Each order generates multiple status updates.

Example

```
PLACED

↓

CONFIRMED

↓

PREPARING

↓

OUT_FOR_DELIVERY

↓

DELIVERED
```

Kafka guarantees ordering only within the same partition.

By using **order_id** as the Kafka message key, Kafka always routes all events belonging to the same order into the same partition.

This guarantees that consumers always process status updates in the correct order.

If a random key were used, different events for the same order could land in different partitions and be processed out of sequence, resulting in an incorrect order state.

---

# Consumer Groups

Consumer A

```
status-tracker
```

Consumer B

```
analytics
```

Because both consumers belong to different consumer groups,

- both receive every message
- both maintain independent offsets
- stopping one consumer does not affect the other

---

# Offset Management

Kafka stores consumer offsets inside the internal topic

```
__consumer_offsets
```

When Consumer A restarts,

it resumes processing from the last committed offset instead of reading messages from the beginning.

This ensures no events are lost.

---

# State Persistence

Consumer A periodically saves active orders into

```
state.json
```

This file is updated every

```
10 seconds
```

Example

```json
{
  "ORD-003": {
    "restaurant": "Pizza Hut",
    "status": "PREPARING"
  }
}
```

---

# Dashboard

Dashboard displays

- Order ID
- Restaurant
- Items
- Current Status
- Progress Indicator

Orders disappear automatically after delivery.

Dashboard refresh interval

```
2 seconds
```

---

# Testing

## Verify Kafka

Open

```
http://localhost:9008
```

Verify

- Kafka running
- Topic exists
- Messages visible

---

## Verify Producer

Run

```
java Producer --orders 3
```

Expected

```
15 events
```

---

## Verify Consumer A

Run

```
GET /state
```

Expected

```
JSON response
```

---

## Verify Consumer B

Observe analytics printed every

```
15 seconds
```

---

## Verify Dashboard

Start producer.

Dashboard should update automatically.

---

# Expected Output

Producer

```
[SENT] ORD-001 -> PLACED
```

Consumer A

```
[UPDATE]

ORD-001

CONFIRMED -> PREPARING
```

Consumer B

```
Restaurant Counts

Dominos : 10
```

Dashboard

```
Order Cards

Progress Bar

Live Updates
```

---

# Troubleshooting

## Kafka not starting

Restart Docker Desktop

```
docker compose down

docker compose up -d
```

---

## Producer cannot connect

Check

```
localhost:9892
```

---

## No messages in Kafdrop

Verify

- Topic exists
- Producer running
- Kafka running

---

## Consumer not receiving messages

Check

- Topic name
- Bootstrap server
- Consumer group
- Kafka broker

---

# Future Improvements

- WebSocket dashboard
- Multiple Kafka brokers
- Dockerized frontend
- Database persistence
- Authentication
- Order cancellation
- Payment events
- Delivery partner tracking
- Prometheus metrics
- Grafana dashboards

---

# Learning Outcomes

After completing this project, the following Kafka concepts were implemented and understood:

- Kafka Producer
- Kafka Consumer
- Kafka Topics
- Kafka Partitions
- Consumer Groups
- Message Keys
- Offset Management
- State Management
- Event Streaming
- Event-Driven Architecture
- Docker-based Kafka Deployment
- REST API Integration
- Real-Time Dashboard

---
````
