# Kafka Event Streaming & Offset Learnings

This document records the experimental findings and conceptual answers for the Apache Kafka food delivery order tracker project.

---

# == Offset Experiment Findings

### Experiment Procedure & Observations:
1. **Initial Setup**: Started Kafka broker, Kafdrop, Consumer A (`status-tracker`), and Consumer B (`analytics`).
2. **Initial Event Stream**: Ran `python producer.py --orders 5`, producing 25 status transition events. Both Consumer A and Consumer B received and processed all 25 events.
3. **Stopping Consumer A**: Stopped Consumer A (Status Tracker) while keeping Consumer B (Analytics Engine) running.
4. **Generating Additional Events**: Ran `python producer.py --orders 5` to publish 25 new events to `order-events`.
   - *Observation*: Consumer B continued running smoothly and processed all 25 new events in real time.
5. **Restarting Consumer A**: Restarted Consumer A (`python consumer_status_tracker.py`).
   - *Observation*: Upon starting up, Consumer A immediately began processing all 25 events that were published while it was offline. It logged the status updates for those orders, updated its active order state dictionary, and caught up to the head of the log without losing any data.

### Key takeaway:
Kafka's log retention and offset tracking decouple message publishing from message consumption. A consumer can crash or go offline, and when it recovers, it resumes reading from its last committed offset without missing any events.

---

# == Guided Checkpoints

### 1. What happens when Consumer A restarts? Does it process the messages sent while it was offline? Why or why not?

**Answer**:
Yes, when Consumer A restarts, it processes all messages that were sent to Kafka while it was offline.

**Why this happens**:
Kafka stores messages durably on disk inside topic partitions regardless of whether consumers are currently active or online. When Consumer A connects using its consumer group ID (`status-tracker`), Kafka retrieves the last committed offset for that group for each partition in the `order-events` topic. Consumer A begins polling messages from `last_committed_offset + 1`. Since Kafka maintained all messages published during the offline period, Consumer A reads and processes them sequentially until it catches up to the latest log offset.

---

### 2. Research and Explain: What is a consumer offset? Where does Kafka store it?

**Answer**:

- **What is a Consumer Offset?**
  A consumer offset is an integer value representing the sequential position of a message within a specific Kafka topic partition. Offsets start at `0` for the first message produced to a partition and increment monotonically by 1 for each subsequent message (`0, 1, 2, 3...`). An offset acts as a bookmark, allowing a consumer to track exactly which messages it has already processed.

- **Where does Kafka store it?**
  In modern Kafka versions, consumer offsets are stored server-side in a special, internal, compacted system topic named **`__consumer_offsets`**.
  When a consumer commits its offset (either automatically via `enable.auto.commit=true` or explicitly via API), it sends a commit request to the Kafka broker acting as the group coordinator. The broker writes a record to `__consumer_offsets` where the record key is `(consumer_group_id, topic, partition)` and the record value is the committed `offset` integer.

---

### 3. Experiment: The `auto.offset.reset` policy (earliest vs. latest)

**Answer**:

The `auto.offset.reset` configuration property defines how a consumer behaves when reading from a topic under two specific conditions:
1. No committed offset exists for the consumer group (e.g. a brand-new consumer group connecting for the first time).
2. The current committed offset is invalid or out of range (e.g. older log segments were deleted due to retention policy expiration).

#### Difference between `earliest` and `latest`:

- **`auto.offset.reset = earliest`**:
  - The consumer automatically resets its starting offset to the **earliest available message** (offset 0 or lowest unexpired offset) in the partition.
  - *Behavior with a new group ID*: The new consumer will read all historical messages currently stored in the topic from the beginning of time (within retention limits).

- **`auto.offset.reset = latest`**:
  - The consumer automatically resets its starting offset to the **latest (newest) offset** at the end of the partition log.
  - *Behavior with a new group ID*: The new consumer will skip all existing historical messages and will only process new messages that are published *after* the consumer starts.

---

### 4. Theoretical: What would happen if both Consumer A and Consumer B shared the same consumer group ID?

**Answer**:

If Consumer A (Status Tracker) and Consumer B (Analytics Engine) shared the same group ID (e.g. `order-processors`):

1. **Partition Load Balancing / Worker Distribution**:
   Kafka consumer groups operate on a competing-consumer model designed for horizontal scaling. Kafka would distribute the 3 partitions of `order-events` across the available consumer instances in the group.
   - For instance, Consumer A might be assigned Partitions 0 and 1, while Consumer B is assigned Partition 2.

2. **Incomplete Data Stream per Application**:
   - Consumer A would **only** receive messages written to Partitions 0 and 1.
   - Consumer B would **only** receive messages written to Partition 2.

3. **System Breakdown**:
   - **Status Tracker (Consumer A)**: Would missing all orders assigned to Partition 2, resulting in an incomplete state API.
   - **Analytics Engine (Consumer B)**: Would missing all events from Partitions 0 and 1, producing incorrect and partial analytics counters.

#### Conclusion:
To implement the **Publish-Subscribe (Pub/Sub)** pattern where multiple independent applications each process 100% of all published events, each application MUST be configured with a **unique consumer group ID** (`status-tracker` vs `analytics`).
