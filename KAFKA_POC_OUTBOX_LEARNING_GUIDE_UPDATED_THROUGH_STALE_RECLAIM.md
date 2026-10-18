# Kafka POC — Transactional Outbox Learning Guide

> This document starts the **Outbox chapter** of the Kafka POC.
>
> It is intentionally organized like a book:
>
> - **Chapter** = one major architecture idea
> - **Module** = one related concept inside that chapter
> - **Submodule** = examples, failure cases, pseudocode, or interview notes
>
> The goal is revision-friendly learning, not code-by-code documentation.

---

# Chapter: Why We Are Moving from Inbox to Outbox

## Module: Where the POC Stands Before Outbox

So far, most of our reliability work was on the **consumer side**.

We solved problems such as:

```text
Kafka redelivery
duplicate consumer processing
concurrent consumers
consumer crash after claim
stale PROCESSING claims
retry timing
external email duplicates
provider-side idempotency
```

The consumer side evolved into:

```text
Kafka
  ↓
order-notification-service
  ↓
Inbox claim(eventId)
  ↓
PROCESSING / COMPLETED
  ↓
lease / stale reclaim
  ↓
exponential retry + jitter
  ↓
email provider
  ↓
Idempotency-Key = eventId
```

That made the consumer much safer.

But now we moved back to the **producer side** and asked:

> What if the order is saved in PostgreSQL, but the Kafka event is never published?

That is a different problem.

It is the problem Outbox is designed to solve.

---

## Module: Inbox vs Outbox — The Simple Difference

A good mental model:

```text
Inbox
= protect the CONSUMER

Outbox
= protect the PRODUCER
```

### Inbox asks:

```text
"Have I already processed this incoming event?"
```

### Outbox asks:

```text
"If my business transaction committed,
how do I guarantee the outgoing event is eventually published?"
```

Another way to remember:

```text
Inbox
→ incoming reliability

Outbox
→ outgoing reliability
```

---

# Chapter: The Producer Dual-Write Problem

## Module: Current Producer Flow

Our current `order-event-service` conceptually does:

```text
HTTP request
    ↓
save order in PostgreSQL
    ↓
create OrderPlacedEvent
    ↓
publish event to Kafka
    ↓
return response
```

Pseudo-code:

```text
createOrder():

    save order in DB

    build event

    kafka.send(event)

    return response
```

At first glance this looks fine.

But there are two separate systems:

```text
PostgreSQL
Kafka
```

and both need to be updated.

That is called a **dual write**.

---

## Module: Why `@Transactional` Does Not Solve It

We may have:

```java
@Transactional
public OrderResponse createOrder(...) {
    orderRepository.save(order);
    kafkaTemplate.send(...);
}
```

It is easy to assume:

```text
@Transactional
→ DB + Kafka are one transaction
```

but that is not true in our current design.

The Spring database transaction protects the PostgreSQL work.

It does not automatically mean:

```text
PostgreSQL commit
+
Kafka publish
```

become one atomic unit.

So this guarantee does NOT exist:

```text
either both succeed
or both fail
```

---

# Chapter: Failure Case — DB Succeeds, Kafka Fails

## Module: The Failure We Simulated

We introduced a POC-only product ID:

```text
productId = 3333
```

For that product we intentionally did:

```text
save order
    ↓
skip Kafka publish
    ↓
return normally
```

The purpose was not to imitate production code.

The purpose was to reproduce a real distributed-system failure deterministically.

Observed result:

```text
PostgreSQL:
order exists
status = PLACED ✅

Kafka:
no OrderPlacedEvent ❌

Notification service:
receives nothing ❌

Email:
never happens ❌
```

---

## Module: How This Happens in Real Production

In production nobody writes:

```text
if productId == 3333:
    skip Kafka
```

The same failure can happen naturally because PostgreSQL and Kafka are independent systems.

Examples:

```text
Kafka broker unavailable
network timeout
DNS problem
producer send failure
pod killed
JVM crash
deployment restart
node failure
broker leader transition
application dies between DB commit and Kafka publish
```

A realistic timeline:

```text
10:00:00.100
INSERT order

10:00:00.120
DB COMMIT ✅

10:00:00.125
pod dies ❌

10:00:00.130
Kafka publish never happens
```

Final state:

```text
DB knows about order
Kafka does not
```

This is an inconsistent system.

---

# Chapter: Why "Publish Kafka First" Does Not Fix It

## Module: The Question

We asked:

> Why not publish the Kafka event first, wait for success, and only then save to DB?

That sounds like it could solve:

```text
DB ✅
Kafka ❌
```

But it only reverses the failure.

---

## Module: Reverse Failure

New flow:

```text
publish Kafka
    ↓
Kafka acknowledges ✅
    ↓
save order in DB
    ↓
DB fails ❌
```

Now Kafka contains:

```text
OrderPlacedEvent
```

but PostgreSQL does not contain the order.

This creates a **phantom event**.

Example:

```text
Kafka:
OrderPlacedEvent(orderId=123) ✅

PostgreSQL:
order 123 does not exist ❌
```

A consumer could already react:

```text
send notification
reserve inventory
start payment
```

for an order that never committed.

---

## Module: Important Learning

Changing the order does not remove the dual-write problem.

It only changes the direction of inconsistency.

### DB first

```text
DB ✅
Kafka ❌
→ missing event
```

### Kafka first

```text
Kafka ✅
DB ❌
→ phantom event
```

The real question is not:

```text
"Which system should I write first?"
```

The real question is:

```text
"How do I avoid requiring two independent systems
to succeed inside one business transaction?"
```

That question leads directly to Transactional Outbox.

---

# Chapter: Transactional Outbox — Core Idea

## Module: The Architectural Shift

Instead of doing:

```text
DB write
+
Kafka write
```

inside one request path, we make the critical business transaction write only to PostgreSQL.

Inside ONE database transaction:

```text
BEGIN

INSERT order

INSERT outbox_event

COMMIT
```

Because both writes are in the same database transaction:

```text
order + outbox row
```

either:

```text
both commit ✅
```

or:

```text
both rollback ✅
```

There is no state where:

```text
order exists
but no record exists saying
"this event needs to be published"
```

---

## Module: The Outbox Mental Model

Think of the outbox table as:

```text
"Events that this service promises to publish"
```

Example:

```text
outbox_events

id
event_id
aggregate_id
event_type
payload
status
created_at
published_at
```

When order creation commits:

```text
orders row
+
outbox row
```

commit together.

Kafka publishing can happen later.

---

# Chapter: Before vs After Outbox

## Module: Before

```text
HTTP request
   ↓
save order
   ↓
DB COMMIT
   ↓
publish Kafka
   ↓
hope nothing fails
```

Problem:

```text
DB and Kafka are two independent writes
```

---

## Module: After

```text
HTTP request
    ↓
ONE PostgreSQL transaction
    |
    ├── save order
    |
    └── save outbox event
    ↓
COMMIT
```

Later:

```text
Outbox Publisher
    ↓
find PENDING event
    ↓
publish to Kafka
    |
    ├── success
    │     ↓
    │  mark PUBLISHED
    │
    └── failure
          ↓
       keep PENDING
          ↓
       retry later
```

This converts:

```text
"Kafka publish must succeed right now"
```

into:

```text
"Kafka publish must eventually succeed"
```

That is the big reliability improvement.

---

# Chapter: Why Outbox Is Safer

## Module: If PostgreSQL Fails

Suppose:

```text
INSERT order
    ↓
INSERT outbox
    ↓
DB error
```

Transaction rolls back:

```text
order ❌
outbox ❌
```

No phantom business state.

---

## Module: If Kafka Is Down

Suppose:

```text
order ✅
outbox ✅
```

commit.

Then publisher tries:

```text
Kafka publish ❌
```

We do NOT lose the event.

The outbox row remains:

```text
PENDING
```

Later:

```text
retry
    ↓
Kafka available
    ↓
publish succeeds
    ↓
mark PUBLISHED
```

This is the core guarantee:

```text
business transaction committed
→ event publication can be retried until success
```

---

# Chapter: Inbox vs Outbox in One Diagram

```text
                         PRODUCER SIDE
                    ─────────────────────

Client
  ↓
order-event-service
  ↓
same DB transaction
  ┌─────────────────────┐
  │ orders              │
  │ outbox_events       │
  └─────────────────────┘
           ↓
    Outbox Publisher
           ↓
         Kafka


                         CONSUMER SIDE
                    ─────────────────────

         Kafka
           ↓
order-notification-service
           ↓
     Inbox Claim
           ↓
 PROCESSING / COMPLETED
           ↓
   business side effect
```

Remember:

```text
OUTBOX
protects event publication

INBOX
protects event consumption
```

---

# Chapter: Inbox vs Outbox Comparison

| Topic | Inbox | Outbox |
|---|---|---|
| Side | Consumer | Producer |
| Main question | Have I processed this event? | Have I reliably published this business event? |
| Protects against | duplicate delivery / concurrent processing / crashes | DB commit + Kafka publish inconsistency |
| Key identity | `eventId` | usually same `eventId` |
| Typical states | PROCESSING, COMPLETED | PENDING, PUBLISHED |
| Main DB action | atomic claim incoming event | save outgoing event with business data |
| Retry target | business processing | Kafka publishing |
| Solves dual write? | No | Yes, for DB → event publication |
| Solves consumer duplicates? | Yes | No |

---

# Chapter: Why Inbox and Outbox Work Well Together

These patterns solve different halves of the same distributed workflow.

```text
Producer
  ↓
Outbox
  ↓
Kafka
  ↓
Inbox
  ↓
Consumer
```

Outbox says:

```text
"If business state committed,
I will eventually publish the event."
```

Inbox says:

```text
"If the event arrives more than once,
I will not blindly repeat the business effect."
```

Together:

```text
Producer reliability
+
Consumer reliability
```

---

# Chapter: End-to-End Mental Model

```text
Client
  ↓
Order Service
  ↓
┌──────────────────────────────┐
│ PostgreSQL Transaction       │
│                              │
│ INSERT order                 │
│ INSERT outbox_event          │
└──────────────────────────────┘
  ↓
commit
  ↓
Outbox Publisher
  ↓
Kafka
  ↓
Notification Service
  ↓
Inbox atomic claim
  ↓
Provider call
Idempotency-Key = eventId
  ↓
Email Provider
  ↓
SMTP
```

Every layer owns a specific reliability problem.

---

# Chapter: Interview Question — Why Do We Need Outbox?

## Question

> An API saves an order in PostgreSQL and then publishes `OrderPlacedEvent` to Kafka. What failure can happen?

Expected answer:

```text
dual-write problem
```

Candidate should explain:

```text
DB succeeds
Kafka fails
```

or:

```text
Kafka succeeds
DB fails
```

and why the two systems are not one atomic transaction.

---

## Strong Follow-Up Question

> Why not publish Kafka first and save DB only after Kafka acknowledges?

Expected answer:

```text
That only reverses the inconsistency.
```

Expected diagram:

```text
DB first:
DB ✅ → Kafka ❌
missing event

Kafka first:
Kafka ✅ → DB ❌
phantom event
```

Strong candidate should conclude:

```text
ordering alone cannot solve a dual write
```

---

# Chapter: Interview Question — Explain Transactional Outbox

Strong answer:

> Write the business row and an outbox row in the same local database transaction. Commit both together. A separate publisher reads pending outbox events and publishes them to Kafka. If Kafka is unavailable, the outbox row stays pending and can be retried. This avoids requiring PostgreSQL and Kafka to succeed atomically in the request transaction.

Expected diagram:

```text
BEGIN DB TX

orders
+
outbox_events

COMMIT

      ↓

Outbox Publisher

      ↓

Kafka
```

---

# Chapter: Interview Follow-Up — Is Outbox Exactly Once?

Important answer:

```text
Not automatically.
```

Why?

Suppose:

```text
publisher sends Kafka successfully ✅
        ↓
publisher crashes before marking
outbox row PUBLISHED ❌
```

When publisher restarts:

```text
same PENDING outbox row
        ↓
publish again
```

So Kafka may receive the same logical event more than once.

That is why our earlier work still matters:

```text
stable eventId
+
consumer Inbox/idempotency
```

Outbox gives us:

```text
reliable eventual publication
```

but consumers must still tolerate duplicate delivery.

This is one of the most important connections between Outbox and Inbox.

---

# Chapter: Outbox + Inbox Together

```text
Outbox publisher may publish duplicate
              ↓
            Kafka
              ↓
same eventId may arrive again
              ↓
Inbox detects / claims same eventId
              ↓
duplicate side effect prevented
```

So:

```text
Outbox
does NOT remove need for Inbox.

Inbox
does NOT remove need for Outbox.
```

They solve different failure windows.

---

# Chapter: Event Identity Through the Whole Pipeline

The same stable identity should travel:

```text
Order transaction
     ↓
Outbox eventId = ABC
     ↓
Kafka eventId = ABC
     ↓
Consumer Inbox eventId = ABC
     ↓
Provider Idempotency-Key = ABC
```

This creates one traceable logical operation across components.

Example:

```text
eventId = ABC

Outbox:
ABC

Kafka:
ABC

Inbox:
ABC

Email Provider:
ABC
```

Useful for:

```text
deduplication
debugging
distributed tracing
replay
DLT investigation
audit
```

---

# Chapter: Pseudocode — Current Design Without Outbox

```text
createOrder():

    order = saveOrder()

    event = buildEvent()

    kafka.send(event)

    return order
```

Problem:

```text
saveOrder and kafka.send
are not one atomic write
```

---

# Chapter: Pseudocode — Target Outbox Design

```text
@Transactional
createOrder():

    order = saveOrder()

    event = buildEvent()

    saveOutbox(
        eventId = event.eventId,
        aggregateId = order.id,
        eventType = ORDER_PLACED,
        payload = event,
        status = PENDING
    )

    return order
```

No Kafka send is required for correctness inside the order transaction.

Then separately:

```text
publishOutbox():

    events = findPendingEvents()

    for event in events:

        try:
            kafka.send(event.payload)

            markPublished(event)

        catch:
            keep PENDING
```

This is the design we are about to implement.

---

# Chapter: Architecture Decision Summary

## Problem

```text
order save
+
Kafka publish
```

are two independent writes.

## Failure

```text
DB ✅
Kafka ❌
```

or:

```text
Kafka ✅
DB ❌
```

## Wrong idea

```text
just change which one happens first
```

## Better design

```text
same DB transaction:

order
+
outbox row
```

## Publication

```text
separate reliable publisher
```

## If Kafka fails

```text
keep PENDING
retry later
```

## Important remaining fact

```text
publisher may still publish duplicate
```

Therefore:

```text
stable eventId
+
consumer Inbox
```

still matter.

---

# Chapter: What We Have Learned About Outbox So Far

Completed understanding:

```text
✅ Identified DB → Kafka dual-write problem

✅ Reproduced DB success + missing Kafka event

✅ Understood how this happens naturally in production

✅ Understood @Transactional does not automatically
   make PostgreSQL + Kafka one atomic transaction

✅ Considered Kafka-first approach

✅ Proved Kafka-first only creates reverse inconsistency

✅ Understood missing-event vs phantom-event failure

✅ Learned the core Outbox idea

✅ Learned order + outbox must commit in one DB transaction

✅ Learned Kafka publishing becomes eventual rather than immediate

✅ Learned Kafka failure leaves outbox PENDING

✅ Learned Outbox solves producer-side reliability

✅ Connected Outbox with Inbox

✅ Understood Outbox does not automatically mean exactly-once delivery

✅ Understood stable eventId must continue through:
   Outbox → Kafka → Inbox → provider
```

---

# Chapter: What We Have NOT Implemented Yet

Next implementation work:

```text
⏳ outbox_events table

⏳ OutboxEventEntity

⏳ OutboxEventRepository

⏳ save order + outbox row in same @Transactional method

⏳ remove direct Kafka publish from createOrder()

⏳ outbox publisher

⏳ PENDING / PUBLISHED lifecycle

⏳ Kafka failure retry

⏳ publisher crash after Kafka success

⏳ duplicate Kafka publication test

⏳ consumer Inbox verification against duplicate outbox publish
```

---

# Chapter: Interview Revision — 60-Second Answer

> The producer has a dual-write problem if it saves business data in PostgreSQL and separately publishes to Kafka. Doing DB first can leave committed data without an event if Kafka fails; doing Kafka first can create an event for data that later rolls back. Transactional Outbox solves this by saving the business row and an outbox event in the same local DB transaction. A separate publisher reads pending outbox records and publishes them to Kafka, retrying if Kafka is unavailable. Because the publisher can still crash after Kafka accepts the event but before marking the outbox record published, duplicate publication is still possible, so events need a stable `eventId` and consumers should remain idempotent, typically with an Inbox pattern.

---

# Chapter: Whiteboard Diagram to Remember

```text
                        PRODUCER

                ┌─────────────────────┐
Request ───────▶│ PostgreSQL TX       │
                │                     │
                │ orders              │
                │ outbox_events       │
                └─────────┬───────────┘
                          │
                        COMMIT
                          │
                          v
                  Outbox Publisher
                          │
                          v
                        Kafka


                        CONSUMER

                        Kafka
                          │
                          v
                     Inbox Claim
                          │
                  ┌───────┴────────┐
                  │                │
             COMPLETED         CLAIMED
                  │                │
                skip               v
                            business work
                                  │
                                  v
                              COMPLETED
```

Remember the shortest version:

```text
Outbox:
DB → Kafka reliability

Inbox:
Kafka → consumer reliability
```

That is the bridge from our Inbox work into the Outbox stage.

---

# Chapter: Implementing the Outbox in `order-event-service`

## Module: Outbox Table and Entity

We created a producer-owned Outbox table inside the same schema as the order service:

```text
order_event.orders
order_event.outbox_events
```

The Outbox row stores the exact event that must eventually be published.

Conceptual columns:

```text
id
event_id
aggregate_id
aggregate_type
event_type
topic
event_key
payload
status
created_at
published_at
```

Example:

```text
id             = 3
event_id       = e22aa42b-856e-431f-ba25-44cb142bce2a
aggregate_id   = 26
aggregate_type = ORDER
event_type     = ORDER_PLACED
topic          = order-events
event_key      = 26
status         = PUBLISHED
```

The Outbox status currently uses:

```text
PENDING
PUBLISHED
```

---

## Module: Why Store the Exact Event Payload?

We store the serialized event JSON in the Outbox:

```text
payload = {
  "eventId": "...",
  "orderId": 26,
  "customerId": 114,
  "productId": 2222,
  "quantity": 1,
  "placedAt": "..."
}
```

Question:

> Why not read the `orders` table later and rebuild the event?

Because the order may have changed between the original transaction and the later publishing time.

Example:

```text
10:00
Order created
status = PLACED

10:01
Order changes
status = PAID

10:02
Outbox publisher runs
```

If we rebuild `ORDER_PLACED` from the current order state at `10:02`, we may accidentally publish data that no longer represents the original event.

Better:

```text
business transaction
        ↓
create exact event now
        ↓
serialize exact event
        ↓
store payload in Outbox
```

So the Outbox contains the historical event that was created when the business transaction happened.

---

## Module: One Stable `eventId` Across the Whole Pipeline

The same logical event identity continues through every layer:

```text
OrderPlacedEvent.eventId
        =
Outbox.event_id
        =
Kafka event.eventId
        =
Consumer Inbox.event_id
        =
Provider Idempotency-Key
```

Example:

```text
eventId = ABC

Outbox:
ABC

Kafka:
ABC

Inbox:
ABC

Email Provider:
ABC
```

This gives us one identity for:

```text
deduplication
debugging
replay
tracing
DLT investigation
provider idempotency
```

---

# Chapter: Changing `createOrder()` to Use Outbox

## Module: Old Design

Before Outbox:

```text
@Transactional

save order
    ↓
build event
    ↓
kafkaTemplate.send(...)
    ↓
return
```

The problem was:

```text
PostgreSQL
+
Kafka
```

were still two independent resource writes.

---

## Module: New Design

We changed the request transaction to:

```text
@Transactional

save order
    ↓
create eventId
    ↓
build OrderPlacedEvent
    ↓
serialize event JSON
    ↓
save Outbox row as PENDING
    ↓
COMMIT
```

There is no direct Kafka send inside `createOrder()` anymore.

Pseudo-code:

```text
@Transactional
createOrder():

    savedOrder = save(order)

    eventId = UUID

    event = buildEvent(
        eventId,
        savedOrder
    )

    payload = serialize(event)

    saveOutbox(
        eventId,
        orderId,
        payload,
        PENDING
    )

    return response
```

---

## Module: Why This Is Atomic

Both writes use PostgreSQL inside the same local transaction:

```text
BEGIN

INSERT order

INSERT outbox_event

COMMIT
```

Therefore:

```text
order ✅
outbox ✅
```

or:

```text
order ❌
outbox ❌
```

We do not want:

```text
order ✅
outbox ❌
```

because that would recreate the missing-event problem.

---

## Module: Why Serialization Failure Must Roll Back

The exact event is serialized before the transaction finishes.

Conceptually:

```text
save order
    ↓
serialize event
    ↓
save outbox
```

If serialization fails, we throw an unchecked exception.

Then:

```text
serialization fails
    ↓
transaction rolls back
    ↓
order not committed
    ↓
outbox not committed
```

This preserves the rule:

```text
business state and publication intent
must commit together
```

---

# Chapter: The Outbox Publisher

## Module: Publisher Responsibility

Once `createOrder()` stores:

```text
status = PENDING
```

a separate background component is responsible for Kafka publication.

Flow:

```text
OutboxPublisher
    ↓
find PENDING rows
    ↓
deserialize payload
    ↓
publish Kafka event
    ↓
wait for Kafka acknowledgement
    ↓
mark PUBLISHED
```

The request thread no longer needs Kafka to be available for the order transaction to succeed.

---

## Module: First Publisher Strategy — Polling

For the first implementation we intentionally used a simple polling scheduler:

```text
@Scheduled(fixedDelay = 2000)
```

Conceptually:

```text
poll PENDING rows
    ↓
process them
    ↓
method finishes
    ↓
wait 2 seconds
    ↓
poll again
```

Important:

`fixedDelay = 2000` does NOT mean:

```text
start a new poll every exact 2 seconds
even if previous poll is still running
```

It means:

```text
previous invocation finishes
        ↓
wait 2 seconds
        ↓
start next invocation
```

This matters because Kafka publishing may itself take time.

---

## Module: Why Polling Was Chosen First

Polling has trade-offs:

```text
extra DB queries
small publication latency
empty polls when no events exist
```

But it is useful for learning because the reliability flow is very visible:

```text
PENDING row
    ↓
poller finds it
    ↓
Kafka publish
    ↓
PUBLISHED
```

Later we can compare production alternatives such as:

```text
CDC / Debezium
database notifications
batching
```

But first we want to understand the correctness model.

---

# Chapter: Kafka Acknowledgement and `.get()`

## Module: Important Question

We asked:

> Earlier we said Kafka does not return HTTP `200`. Then how do we know it succeeded before marking Outbox `PUBLISHED`?

Kafka does not return an HTTP response code.

But `KafkaTemplate.send()` returns a Future representing the asynchronous producer send.

Conceptually:

```text
send()
    ↓
Kafka producer sends record
    ↓
broker acknowledges according to `acks`
    ↓
Future completes successfully
```

When we call:

```java
kafkaTemplate
    .send(...)
    .get(10, TimeUnit.SECONDS);
```

`.get()` does not send a separate request to "fetch an ACK".

It simply waits for the already-started asynchronous send operation to complete.

So:

```text
Future completed successfully
→ Kafka acknowledged the send
```

according to the producer acknowledgement configuration.

---

## Module: What Kafka ACK Means

Kafka ACK means approximately:

```text
"Kafka accepted this record successfully
according to the configured durability/acks policy."
```

It does NOT mean:

```text
consumer received it
consumer processed it
email was sent
whole business workflow completed
```

So:

```text
Kafka ACK
!=
business workflow success
```

For Outbox we only need to know:

```text
Did Kafka accept this event?
```

If yes:

```text
mark Outbox PUBLISHED
```

---

# Chapter: Important Q&A — Why Not Just Use `.get()` in the Old Code?

## Module: The Question

We asked:

> If `.get()` tells us Kafka acknowledged the message, why didn't we simply use it in the original `createOrder()` method and avoid Outbox?

Example old design:

```text
@Transactional

save order
    ↓
Kafka send
    ↓
.get()
    ↓
wait for ACK
```

This improves error handling, but it does NOT make PostgreSQL + Kafka atomic.

---

## Module: Failure Still Exists

Inside a Spring DB transaction, the final database commit normally happens when the transactional method finishes.

Possible sequence:

```text
BEGIN DB transaction

INSERT order
    ↓
Kafka send
    ↓
Kafka ACK ✅
    ↓
method finishes
    ↓
DB COMMIT
```

Now imagine:

```text
Kafka ACK ✅
    ↓
DB COMMIT fails ❌
```

Final state:

```text
Kafka event exists ✅
PostgreSQL order does not exist ❌
```

That is the phantom-event problem.

So `.get()` answers:

```text
"Did this Kafka send succeed?"
```

It does NOT answer:

```text
"Can Kafka and PostgreSQL commit atomically together?"
```

---

## Module: The Difference Between `.get()` and Outbox

Remember:

```text
.get()

solves:
"Did this Kafka attempt succeed?"
```

Outbox solves:

```text
"If Kafka does not succeed,
how do I durably remember that
the event still needs publication?"
```

Outbox combines:

```text
durable publication intent
+
Kafka ACK
+
retry
```

That is why we still use `.get()` inside the Outbox publisher.

---

# Chapter: Outbox Recovery When Kafka Is Down

## Module: Real Failure Test

We stopped Kafka while leaving:

```text
PostgreSQL
order-event-service
notification-service
email-provider
Mailpit
```

running.

Then we created a normal order.

The order request was still able to commit:

```text
orders row ✅
outbox row = PENDING ✅
Kafka unavailable ❌
```

This proved the order transaction no longer depends on Kafka availability.

---

## Module: Producer Logs While Kafka Was Down

The Outbox publisher repeatedly attempted to connect to:

```text
localhost:9092
```

and Kafka client logged messages such as:

```text
Connection to node -1 could not be established
Bootstrap broker localhost:9092 disconnected
```

This was expected.

The important part was not the connection error.

The important part was:

```text
Outbox remained PENDING
```

instead of losing the event.

---

## Module: Why the Scheduler Was Not Actually Retrying Every 2 Seconds

The scheduler used:

```text
fixedDelay = 2 seconds
```

but the Kafka send used:

```text
.get(10 seconds)
```

So one attempt can conceptually look like:

```text
start poll
    ↓
Kafka send waits / reconnects
    ↓
eventually fails
    ↓
catch
    ↓
method finishes
    ↓
wait 2 seconds
    ↓
next poll
```

Therefore the actual interval between attempts can be longer than 2 seconds.

---

## Module: Recovery After Kafka Restart

We restarted Kafka.

The same Outbox row was still:

```text
PENDING
```

The publisher discovered it again:

```text
PENDING
    ↓
Kafka send
    ↓
Kafka ACK ✅
    ↓
mark PUBLISHED
```

The row changed to:

```text
status       = PUBLISHED
published_at = populated
```

The downstream notification service then received the event and the email flow completed.

This is the main Outbox guarantee we wanted to prove.

---

## Module: Before vs After Outbox

Before:

```text
DB save ✅
Kafka unavailable ❌
    ↓
event may be lost permanently
```

After:

```text
DB order ✅
Outbox PENDING ✅
Kafka unavailable ❌
    ↓
event remains durable
    ↓
Kafka returns
    ↓
event eventually published ✅
```

The Outbox converts:

```text
"Kafka must succeed now"
```

into:

```text
"Kafka must eventually succeed"
```

---

# Chapter: Operational Learning — Noisy Scheduler SQL Logs

## Module: Why Hibernate SELECT Kept Printing

The scheduler continuously runs:

```text
findTop50ByStatusOrderByCreatedAtAsc(PENDING)
```

So if Hibernate SQL logging is enabled, every poll prints a query.

We disabled SQL logging using the equivalent of:

```properties
spring.jpa.show-sql=false
logging.level.org.hibernate.SQL=OFF
logging.level.org.hibernate.orm.jdbc.bind=OFF
```

Important:

```text
SQL logging OFF
!=
scheduler polling OFF
```

The DB query still runs.

We only stopped printing it.

---

# Chapter: Outbox Does Not Guarantee One Kafka Record

## Module: The Next Failure Window

After proving Kafka-down recovery, we tested:

```text
Kafka send succeeds ✅
    ↓
Kafka ACK received ✅
    ↓
publisher crashes ❌
    ↓
before Outbox row becomes PUBLISHED
```

Now:

```text
Kafka already has event
```

but:

```text
Outbox still says PENDING
```

When the scheduler runs again:

```text
PENDING row
    ↓
publish again
```

So Outbox can publish the same logical event multiple times.

This is why Transactional Outbox normally gives:

```text
at-least-once publication
```

not guaranteed exactly-once delivery.

---

# Chapter: Simulating Crash After Kafka ACK

## Module: POC Product ID

We used:

```text
productId = 2222
```

to simulate:

```text
Kafka ACK received
    ↓
fail before PUBLISHED DB update
```

The simulation was placed AFTER:

```text
Kafka send
.get(...)
```

and BEFORE:

```text
status = PUBLISHED
published_at = NOW
```

That placement is essential.

---

## Module: Why We Used an In-Memory Set

We used an in-memory Set conceptually like:

```text
crashAfterAckOnce
```

The condition was:

```text
productId == 2222
AND
Set.add(eventId) == true
```

The Set is NOT part of the real Outbox architecture.

It is only a testing trick so the simulated crash happens exactly once for that event during the JVM run.

---

## Module: First Poll Dry Run

For event:

```text
productId = 2222
eventId   = ABC
```

First poll:

```text
Outbox = PENDING
    ↓
Kafka publish ABC
    ↓
Kafka ACK ✅
    ↓
productId == 2222
    ↓
Set.add(ABC)
    ↓
ABC was not present
    ↓
add() returns true
    ↓
throw simulated exception
```

Result:

```text
Kafka has ABC ✅

Outbox:
PENDING ❌
published_at = NULL
```

---

## Module: Second Poll Dry Run

Scheduler polls again.

Same row is still:

```text
PENDING
```

So:

```text
publish SAME event ABC again
    ↓
Kafka ACK ✅
    ↓
Set.add(ABC)
```

But ABC is already in the Set:

```text
add() returns false
```

Therefore the simulated crash is skipped.

Execution continues:

```text
mark PUBLISHED
set published_at
```

Final Outbox state:

```text
PUBLISHED ✅
```

But Kafka has received the logical event twice.

---

## Module: Why the Set Is Not a Production Solution

If the JVM really crashed:

```text
JVM dies
    ↓
in-memory Set disappears
```

So the Set does not provide reliability.

It merely lets us simulate:

```text
crash once
then recover
```

without actually killing the application.

The real reliability comes from:

```text
Outbox state
stable eventId
consumer idempotency
```

---

# Chapter: Actual Duplicate Publication Test

## Module: Actual Outbox Row

The test produced:

```text
orderId   = 26
productId = 2222
eventId   = e22aa42b-856e-431f-ba25-44cb142bce2a
```

Final Outbox DB state:

```text
status       = PUBLISHED
published_at = 2026-10-15 12:20:50.943
```

Producer logs showed:

```text
SIMULATED CRASH AFTER KAFKA ACK
BEFORE OUTBOX PUBLISHED

OUTBOX PUBLISH FAILED

OUTBOX PUBLISH ATTEMPT

OUTBOX PUBLISHED
```

This proves:

```text
Kafka publish #1 succeeded

DB publication marker did not

publisher retried

Kafka publish #2 succeeded

DB finally marked PUBLISHED
```

---

# Chapter: Kafka Physical Record vs Logical Event

## Module: Actual Consumer Result

The notification service received:

```text
offset = 22
eventId = e22aa42b-856e-431f-ba25-44cb142bce2a
```

and then:

```text
offset = 23
eventId = e22aa42b-856e-431f-ba25-44cb142bce2a
```

This is a very important distinction.

Kafka sees:

```text
offset 22
offset 23
```

as two separate physical records.

But our application sees:

```text
same eventId
```

as one logical integration event.

So:

```text
Kafka record identity
!=
business event identity
```

---

# Chapter: Inbox Protects Against Outbox Duplicate Publication

## Module: First Kafka Record

First physical record:

```text
offset = 22
eventId = ABC
```

Inbox flow:

```text
CLAIMED
    ↓
provider call
    ↓
SENT
    ↓
Inbox COMPLETED
```

Actual result:

```text
CLAIM RESULT = CLAIMED
EMAIL PROVIDER RESPONSE = SENT
EVENT COMPLETED
```

---

## Module: Second Kafka Record

Second physical record:

```text
offset = 23
eventId = SAME ABC
```

Inbox sees the existing logical event:

```text
eventId = ABC
status = COMPLETED
```

Result:

```text
CLAIM RESULT = COMPLETED
Duplicate completed event ignored
```

No second provider call was needed.

No second email was produced.

---

# Chapter: Outbox + Inbox — Proven End to End

The final behavior was:

```text
Outbox publisher attempts = 2

Kafka physical records    = 2

Logical eventId           = 1

Inbox completed rows      = 1

Actual email              = 1
```

This is the exact relationship we wanted to prove.

```text
OUTBOX

prevents event loss

but may publish duplicate records
```

and:

```text
INBOX

recognizes the same logical eventId

and prevents duplicate processing
```

Shortest interview statement:

```text
Outbox
= at-least-once publication

Inbox
= idempotent consumption
```

---

# Chapter: Full Producer-to-Consumer Reliability Flow Now

```text
Client
  ↓
order-event-service
  ↓
┌────────────────────────────────┐
│ PostgreSQL transaction         │
│                                │
│ INSERT order                   │
│ INSERT outbox(PENDING)         │
└────────────────────────────────┘
  ↓
COMMIT
  ↓
Outbox Publisher
  ↓
Kafka send + ACK
  |
  ├── failure
  │     ↓
  │  stay PENDING
  │     ↓
  │  retry later
  │
  └── success
        ↓
     mark PUBLISHED

          ↓

        Kafka

          ↓

order-notification-service
          ↓
atomic Inbox claim(eventId)
  |
  ├── first logical event
  │      ↓
  │   process
  │      ↓
  │   COMPLETED
  │
  └── duplicate logical event
         ↓
      COMPLETED found
         ↓
      ignore
```

---

# Chapter: Interview Question — Why Do We Still Need Inbox After Outbox?

## Question

> If Transactional Outbox reliably publishes the event, why do we still need consumer idempotency?

Expected answer:

```text
Because Outbox publication can itself be repeated.
```

Failure:

```text
Kafka ACK ✅
    ↓
publisher crashes
    ↓
before PUBLISHED DB update
    ↓
Outbox remains PENDING
    ↓
publish again
```

Therefore:

```text
same logical event
may appear in Kafka more than once
```

Consumer must use:

```text
stable eventId
+
Inbox/idempotency
```

---

# Chapter: Interview Question — Does Kafka ACK Mean End-to-End Success?

Expected answer:

```text
No.
```

Kafka ACK means:

```text
Kafka accepted the producer record
according to producer acknowledgement settings.
```

It does not mean:

```text
consumer processed it
email sent
payment completed
business workflow finished
```

---

# Chapter: Interview Question — Why Use `.get()` in the Outbox Publisher?

Expected answer:

```text
KafkaTemplate.send() is asynchronous.
```

Without waiting:

```text
send()
    ↓
mark PUBLISHED immediately
    ↓
Kafka send could fail later
```

That could create:

```text
Outbox = PUBLISHED
Kafka event = missing
```

For this learning implementation we wait:

```text
send
  ↓
.get()
  ↓
Kafka ACK
  ↓
mark PUBLISHED
```

Later a higher-throughput implementation could use async callbacks rather than blocking each send, but the correctness rule remains:

```text
do not mark successful publication
before Kafka confirms the send
```

---

# Chapter: Updated Outbox Design Decision Table

| Problem | Why It Happened | Improvement | What We Proved |
|---|---|---|---|
| DB succeeds, Kafka missing | dual write | Outbox row in same DB transaction | event intent survives Kafka outage |
| Kafka temporarily down | producer cannot publish | keep Outbox `PENDING` | automatic publication after Kafka restart |
| Need Kafka send result | `send()` is async | wait for Future / ACK | only mark `PUBLISHED` after broker acknowledgement |
| `.get()` in old code seems enough | ACK is not cross-system atomicity | keep DB transaction local using Outbox | avoids DB/Kafka dual-write dependency |
| Kafka ACK then publisher crash | Kafka and Outbox status update are separate | retry same stable eventId | duplicate Kafka publication reproduced |
| Duplicate Kafka records | Outbox is at-least-once | consumer Inbox | second logical event ignored |
| Same event, different offsets | Kafka append creates new physical record | stable application `eventId` | logical identity survives duplicate publication |

---

# Chapter: Updated POC Checkpoint

Completed Outbox implementation/testing:

```text
✅ Created Outbox status model

✅ Created outbox_events table/entity

✅ Created Outbox repository

✅ Stored exact serialized event payload

✅ Preserved stable eventId through Outbox

✅ Changed createOrder():
   order + Outbox PENDING in same transaction

✅ Removed direct Kafka publication
   from the business transaction

✅ Built polling OutboxPublisher

✅ Enabled scheduled publication

✅ Waited for Kafka send Future / ACK

✅ Marked PUBLISHED only after successful Kafka send

✅ Tested normal Outbox happy path

✅ Stopped Kafka intentionally

✅ Created order successfully while Kafka was unavailable

✅ Verified Outbox remained PENDING

✅ Restarted Kafka

✅ Verified same PENDING row published automatically

✅ Verified status changed to PUBLISHED

✅ Reproduced crash after Kafka ACK
   but before Outbox PUBLISHED

✅ Verified same logical event was published twice

✅ Verified duplicate Kafka records had different offsets

✅ Verified both records carried the same eventId

✅ Verified first event was CLAIMED by Inbox

✅ Verified second event returned COMPLETED

✅ Verified duplicate processing was ignored

✅ Verified only one actual email was sent
```

---

# Chapter: Current Architecture After Outbox + Inbox

```text
                         PRODUCER

Client
  ↓
Order Service
  ↓
┌──────────────────────────────┐
│ PostgreSQL TX                │
│                              │
│ orders                       │
│ outbox_events(PENDING)       │
└──────────────┬───────────────┘
               ↓
             COMMIT
               ↓
       Outbox Publisher
               ↓
         Kafka send/ACK
          │          │
          │ fail     │ success
          v          v
       PENDING    PUBLISHED
          │
          └── retry


                         CONSUMER

             Kafka
               ↓
       Notification Service
               ↓
       Inbox claim(eventId)
        │              │
        │ first        │ duplicate
        v              v
     CLAIMED       COMPLETED
        ↓              ↓
     process          ignore
        ↓
     provider
        ↓
     email
        ↓
    COMPLETED
```

---

# Chapter: What Problem Comes Next?

Our publisher currently uses:

```text
SELECT PENDING rows
```

and we have only one Outbox publisher instance.

Question:

> What if two `order-event-service` instances run at the same time?

Both may query:

```text
same PENDING row
```

before either marks it `PUBLISHED`.

Possible flow:

```text
Publisher A
finds event ABC

Publisher B
finds event ABC

A → Kafka publish

B → Kafka publish
```

The Inbox would still protect the consumer from duplicate business processing, but the producer side would generate unnecessary duplicate traffic.

The next Outbox design problem is therefore:

```text
multiple publisher instances
+
concurrent Outbox claiming / locking
```

That leads into:

```text
atomic Outbox claiming
database locking
SKIP LOCKED / claim state
batch ownership
multi-instance publisher safety
```

As before, we should reproduce that race before optimizing it.

---

# Chapter: Multi-Instance Outbox Publisher Race

## Module: Why One Publisher Instance Was Not Enough

Our first Outbox publisher worked correctly when only one `order-event-service` instance was running.

The publisher logic was:

```text
SELECT PENDING rows
        ↓
publish to Kafka
        ↓
mark PUBLISHED
```

The next question was:

> What happens when two application instances run the same scheduler against the same Outbox table?

We started:

```text
order-event-service A
port 8080

order-event-service B
port 8081
```

Both instances used the same PostgreSQL database, the same Outbox table, and the same Kafka broker.

That gave us two independent Outbox publishers.

---

## Module: Why the Race Exists

The original query only selected rows by status:

```text
WHERE status = PENDING
```

There was no ownership or lock.

So both instances could do:

```text
8080:
SELECT event ABC
status = PENDING

8081:
SELECT event ABC
status = PENDING
```

before either publisher updated the row.

That produces:

```text
                    PostgreSQL

                 ABC = PENDING
                  /        \
                 /          \
              8080          8081
               ↓              ↓
          SELECT ABC      SELECT ABC
               ↓              ↓
          publish ABC    publish ABC
```

The Outbox table still ends with one row, but Kafka may receive the same logical event more than once.

---

# Chapter: Reproducing the Multi-Publisher Race

## Module: Deterministic Delay with `productId = 1111`

To make the race easy to observe, we introduced:

```text
productId = 1111
```

The publisher pauses for five seconds before Kafka send:

```text
SELECT PENDING
    ↓
1111 detected
    ↓
sleep 5 seconds
    ↓
Kafka publish
```

That gives the second application instance enough time to poll the same still-`PENDING` row.

This delay is only a POC test tool.

It is not production logic.

---

## Module: Actual Event Used in the Test

The test created:

```text
orderId = 27

eventId =
ab26452a-5e8e-43cd-9da4-e570c00b981d
```

Instance `8080` logged:

```text
OUTBOX PUBLISH ATTEMPT
eventId=ab26452a...

SIMULATING SLOW OUTBOX PUBLISH
eventId=ab26452a...

OUTBOX PUBLISHED
```

Instance `8081` logged the same event:

```text
OUTBOX PUBLISH ATTEMPT
eventId=ab26452a...

SIMULATING SLOW OUTBOX PUBLISH
eventId=ab26452a...

OUTBOX PUBLISHED
```

This proved:

```text
same Outbox row
was selected concurrently
by both publishers
```

---

# Chapter: Why the Primary Key Did Not Throw an Error

## Module: Important Question

We asked:

> Both publishers update the same Outbox row. Why doesn't the primary key reject the second operation?

Because neither publisher is inserting a new Outbox row.

Both loaded the existing row:

```text
id = X
eventId = ABC
status = PENDING
```

Then both executed conceptually:

```sql
UPDATE order_event.outbox_events
SET status = 'PUBLISHED',
    published_at = ?
WHERE id = X;
```

So the database sees:

```text
UPDATE existing row
UPDATE same existing row
```

not:

```text
INSERT row X
INSERT row X
```

A primary-key violation would happen only for duplicate inserts.

---

## Module: Why Updating the Same Row Twice Is Still Dangerous

The database may end with:

```text
one row
status = PUBLISHED
```

which looks correct.

But before those updates happened:

```text
Publisher A → Kafka publish ✅
Publisher B → Kafka publish ✅
```

The duplicate side effect has already happened.

This repeats an earlier lesson from the Inbox work:

> Protecting final database state does not undo an external side effect that has already occurred.

Here the external side effect is:

```text
Kafka publication
```

---

# Chapter: Kafka Producer Idempotence Is Not Business Idempotence

## Module: What We Observed

Kafka producer configuration showed:

```text
enable.idempotence = true
```

The two application instances received different Kafka producer identities:

```text
8081:
ProducerId = 2001

8080:
ProducerId = 2002
```

This raised an important question:

> If Kafka producer idempotence is enabled, why did Kafka still accept both sends?

---

## Module: What `producerId` Actually Means

A Kafka `producerId` is not created for every business request.

It belongs to a Kafka producer session/instance.

Conceptually:

```text
Application instance A starts
        ↓
KafkaProducer A
        ↓
producerId = 2002
```

That same producer can send many records.

Example:

```text
producerId=2002
sequence=0 → event A

producerId=2002
sequence=1 → event B

producerId=2002
sequence=2 → event C
```

Kafka accepts all of them.

---

## Module: What Producer Idempotence Protects

Kafka producer idempotence mainly helps when the same producer retries the same send because acknowledgement was uncertain.

Conceptually:

```text
producerId = 2002
partition  = 0
sequence   = 10

send
    ↓
Kafka stores record
    ↓
ACK lost
    ↓
producer retries SAME sequence
```

Kafka can recognize:

```text
same producer
same partition
same sequence
```

and avoid appending the same producer attempt twice.

---

## Module: What Producer Idempotence Does NOT Protect

It does not compare our JSON business field:

```text
eventId = ABC
```

and globally deduplicate it.

It also does not stop two independent application producers from intentionally sending the same logical event.

Our race looked like:

```text
Producer 2001 → eventId ABC

Producer 2002 → eventId ABC
```

Kafka sees:

```text
two valid producer operations
```

so both are accepted.

Even one producer can intentionally send identical payloads twice:

```text
sequence=20 → ABC
sequence=21 → ABC
```

Kafka accepts both because they are two separate sends.

Remember:

```text
Kafka producer idempotence
=
protects producer retry of the same send

Application eventId / Inbox
=
protects logical business duplicates
```

---

# Chapter: Why the Fix Must Happen Before Kafka Publish

Adding only:

```text
UNIQUE constraints
```

or:

```text
optimistic locking after publish
```

would be too late.

The important failure already happened here:

```text
Publisher A → Kafka ABC
Publisher B → Kafka ABC
```

Therefore the producer-side fix must establish exclusive ownership BEFORE:

```text
kafkaTemplate.send(...)
```

Desired flow:

```text
Publisher A
→ claim ABC ✅

Publisher B
→ cannot claim ABC

only A reaches Kafka send
```

---

# Chapter: Fixing the Race with PostgreSQL `FOR UPDATE SKIP LOCKED`

## Module: Core Idea

We changed the publisher query to use:

```sql
FOR UPDATE SKIP LOCKED
```

Mental model:

```text
FOR UPDATE
=
lock the selected row for this transaction

SKIP LOCKED
=
if another transaction already locked a row,
do not wait for it;
skip it and continue
```

---

## Module: Locking Query

The publisher repository now uses a query conceptually like:

```sql
SELECT *
FROM order_event.outbox_events
WHERE status = 'PENDING'
ORDER BY created_at
LIMIT 10
FOR UPDATE SKIP LOCKED;
```

This is still a `SELECT`, but it is a locking `SELECT`.

---

# Chapter: Why the Locking Query Must Run Inside a Transaction

A row lock only lives for the database transaction.

Incorrect mental model:

```text
SELECT FOR UPDATE
transaction ends
        ↓
do Kafka work later
```

The lock would already be gone.

What we implemented:

```text
BEGIN TRANSACTION

SELECT PENDING
FOR UPDATE SKIP LOCKED
        ↓
row remains locked
        ↓
Kafka publish
        ↓
mark PUBLISHED

COMMIT
        ↓
lock released
```

So ownership remains exclusive while the batch is being processed.

---

# Chapter: Separating Scheduler from Transactional Processing

## Module: `OutboxPublisher`

The scheduler now only decides:

```text
WHEN should publishing run?
```

Conceptually:

```text
@Scheduled
publishPendingEvents():
    batchProcessor.publishBatch()
```

---

## Module: `OutboxBatchProcessor`

The transactional processor decides:

```text
HOW should rows be locked and published?
```

Conceptually:

```text
@Transactional
publishBatch():

    events = SELECT ... FOR UPDATE SKIP LOCKED

    for event:
        publish Kafka
        mark PUBLISHED
```

This separation also avoids Spring self-invocation problems around `@Transactional`.

---

# Chapter: Dry Run — Two Publishers After `SKIP LOCKED`

Suppose:

```text
ABC = PENDING
```

### Publisher 8080

Starts transaction:

```text
BEGIN
```

Runs:

```text
SELECT ABC
FOR UPDATE SKIP LOCKED
```

PostgreSQL locks ABC.

```text
ABC = locked by 8080
```

The POC `1111` delay now keeps the lock visible for five seconds.

---

### Publisher 8081

Runs the same query during those five seconds.

PostgreSQL sees:

```text
ABC is already locked
```

Because the query says:

```text
SKIP LOCKED
```

8081 does not wait.

It does:

```text
skip ABC
```

If another unlocked PENDING event exists:

```text
take that event
```

If none exists:

```text
return no rows
```

---

## Module: Result

Now:

```text
8080
→ owns ABC
→ publishes ABC

8081
→ skips ABC
→ does not publish ABC
```

This is the producer-side ownership guarantee we were missing.

---

# Chapter: Actual Result After the Fix

We reran the two-instance `1111` test with:

```text
8080 running
8081 running
```

and both using the same Outbox table.

After adding:

```text
FOR UPDATE SKIP LOCKED
```

only one publisher processed the event.

The second publisher did not publish the same `eventId`.

So the race was fixed.

Before:

```text
1 Outbox row
2 publishers
2 Kafka publications
```

After:

```text
1 Outbox row
2 publishers
1 active owner
1 Kafka publication
```

---

# Chapter: What Happens If the Lock Owner Crashes?

Suppose:

```text
8080
    ↓
locks ABC
    ↓
JVM / connection dies
```

The PostgreSQL transaction is rolled back.

The database releases the row lock automatically.

Because `PUBLISHED` was not committed:

```text
ABC remains PENDING
```

Later another publisher can select it.

Conceptually:

```text
8080 crashes
    ↓
DB transaction rollback
    ↓
lock released
    ↓
8081 future poll
    ↓
ABC available again
```

No manual unlock is required.

---

# Chapter: Important Limitation of Holding the DB Lock During Kafka I/O

Our current implementation keeps the transaction open while doing:

```text
Kafka network call
```

and possibly:

```text
.get(10 seconds)
```

That means while publishing:

```text
DB transaction remains open
DB connection remains occupied
row locks remain held
```

For a small learning POC this is excellent because the ownership model is easy to see.

But for high-throughput production workloads, holding database locks during slow network I/O can become expensive.

Potential effects:

```text
long transactions
connection-pool pressure
longer-held row locks
larger batches holding many locks
slower DB cleanup / contention
```

This leads naturally to another architecture option:

```text
short atomic claim transaction

PENDING
   ↓
PROCESSING / CLAIMED

COMMIT quickly

then publish Kafka outside the row lock
```

That approach needs its own:

```text
lease
stale claim recovery
failure semantics
```

which is very similar to what we already learned on the Inbox side.

---

# Chapter: `SKIP LOCKED` vs Atomic Claim — The Design Choice Ahead

## `FOR UPDATE SKIP LOCKED`

Strength:

```text
simple
database provides exclusive ownership
workers naturally split available rows
excellent for queue-like batch processing
```

Trade-off in our current implementation:

```text
transaction stays open during Kafka send
```

---

## Atomic Claim State

Possible future flow:

```text
PENDING
   ↓
atomic claim
   ↓
PROCESSING
   ↓
COMMIT claim quickly

then:

Kafka publish outside claim transaction
```

Strength:

```text
short DB transaction
no long row lock during network I/O
```

But introduces new questions:

```text
What if worker crashes after PROCESSING claim?

How does another worker reclaim it?

How long is claim lease?

What if Kafka succeeded but state update failed?
```

We already know many of these concepts from the Inbox implementation.

---

# Chapter: Updated Whiteboard — Multi-Instance Safe Outbox

```text
                     PostgreSQL

                  PENDING EVENTS
                        │
             ┌──────────┴──────────┐
             │                     │
             v                     v
       Publisher 8080        Publisher 8081
             │                     │
             │ SELECT ...          │ SELECT ...
             │ FOR UPDATE          │ FOR UPDATE
             │ SKIP LOCKED         │ SKIP LOCKED
             │                     │
             v                     v
        locks event A          skips event A
             │                     │
             │                     └── takes event B
             │
             v
          Kafka
             │
             v
       mark PUBLISHED
             │
             v
           COMMIT
```

For one event:

```text
one active DB owner
→ one publisher reaches Kafka
```

---

# Chapter: Interview Question — Multiple Outbox Publisher Instances

## Question

> Your Outbox publisher is deployed on five pods. All pods poll the same `PENDING` table. What prevents all five from publishing the same row?

Weak answer:

```text
The primary key will stop it.
```

Why wrong:

```text
they are not inserting duplicate rows;
they are reading the same row and performing an external publish.
```

Strong answer should discuss one of:

```text
SELECT ... FOR UPDATE SKIP LOCKED
```

or:

```text
atomic PENDING → PROCESSING claim
```

and explain that ownership must be established before the Kafka side effect.

---

# Chapter: Interview Question — Why `SKIP LOCKED`?

Without `SKIP LOCKED`:

```text
Publisher A locks row A

Publisher B tries row A
        ↓
waits for A
```

That can reduce worker parallelism.

With:

```text
FOR UPDATE SKIP LOCKED
```

Publisher B can:

```text
skip row A
take row B
```

So multiple workers can divide work efficiently.

---

# Chapter: Interview Question — Does `SKIP LOCKED` Remove Every Duplicate?

No.

It solves this specific race:

```text
multiple publishers simultaneously selecting
the same PENDING row
```

It does not eliminate every duplicate publication window.

We still have:

```text
Kafka ACK ✅
    ↓
publisher crashes before DB commit ❌
```

The DB transaction can roll back:

```text
PENDING remains
```

so the event may be published again later.

That is why:

```text
stable eventId
+
consumer Inbox
```

remain necessary.

---

# Chapter: Reliability Layers We Have Built So Far

```text
Producer business transaction
        ↓
Transactional Outbox
        ↓
multi-instance ownership
FOR UPDATE SKIP LOCKED
        ↓
Kafka
        ↓
stable eventId
        ↓
Consumer Inbox
        ↓
lease / stale recovery
        ↓
Provider Idempotency-Key
        ↓
email side effect
```

Each layer solves a different failure window.

---

# Chapter: Updated POC Checkpoint

Outbox stage completed so far:

```text
✅ Identified DB + Kafka dual-write problem

✅ Proved DB-first failure

✅ Proved Kafka-first only reverses the failure

✅ Created Outbox table

✅ Stored exact event payload

✅ Saved order + Outbox in same PostgreSQL transaction

✅ Built polling publisher

✅ Waited for Kafka ACK before PUBLISHED

✅ Proved Kafka-down recovery

✅ Proved PENDING survives Kafka outage

✅ Proved automatic publication after Kafka restart

✅ Reproduced Kafka ACK → crash before PUBLISHED

✅ Proved same logical event can reach Kafka twice

✅ Verified different Kafka offsets with same eventId

✅ Verified Inbox ignored duplicate logical event

✅ Started two producer-service instances

✅ Reproduced two publishers selecting same PENDING row

✅ Proved both publishers could send same logical event

✅ Learned why PK did not prevent the race

✅ Learned Kafka producerId is per producer session,
   not per business request

✅ Learned Kafka producer idempotence does not
   globally deduplicate application eventId

✅ Added FOR UPDATE SKIP LOCKED

✅ Kept locking query inside transaction

✅ Split scheduler from transactional batch processor

✅ Re-ran two-instance race

✅ Verified only one publisher owns/processes the row
```

---

# Chapter: Current Remaining Outbox Design Question

The current implementation is correct for the race we targeted, but the transaction remains open while Kafka I/O occurs.

The next architectural comparison is:

```text
Option A

FOR UPDATE SKIP LOCKED
+
hold transaction during Kafka publish
```

versus:

```text
Option B

short atomic claim:
PENDING → PROCESSING
+
publish outside the DB lock
+
lease / stale-claim recovery
```

The next learning goal is to compare those two approaches and decide which model we want for the production-style version of this POC.

---

# Chapter: Moving from Long DB Locks to Short Outbox Claims

## Module: Why We Changed the `SKIP LOCKED` Design

The previous design was correct for multi-instance ownership:

```text
BEGIN

SELECT ...
FOR UPDATE SKIP LOCKED
    ↓
Kafka publish
    ↓
wait for ACK
    ↓
mark PUBLISHED

COMMIT
```

It prevented two publishers from processing the same Outbox row.

But the database transaction remained open during Kafka network I/O, so the DB connection and row lock stayed held while waiting on Kafka.

For a more production-style design we wanted:

```text
claim quickly
commit quickly
release DB lock
then do Kafka I/O
```

---

# Chapter: New Outbox Lifecycle

## Module: Adding `PROCESSING`

We extended the Outbox status model from:

```text
PENDING
PUBLISHED
```

to:

```text
PENDING
PROCESSING
PUBLISHED
```

We also added:

```text
claimed_at
claimed_by
```

So a claimed row can look like:

```text
status       = PROCESSING
claimed_at   = 2026-10-18 23:13:18
claimed_by   = worker-ABC
published_at = NULL
```

This lets us distinguish work waiting to be claimed from work already owned by a publisher.

---

# Chapter: Short Claim Transaction

## Module: Target Flow

The new design is:

```text
SHORT DB TRANSACTION

PENDING
   ↓
SELECT ... FOR UPDATE SKIP LOCKED
   ↓
PROCESSING
claimed_at = NOW()
claimed_by = workerId
   ↓
COMMIT

DB lock released here

----------------------------

Kafka publish
   ↓
ACK
   ↓
separate short DB transaction
   ↓
PUBLISHED
```

So the Kafka network call is no longer inside the original row-lock transaction.

---

## Module: Worker Identity

Each running publisher instance gets a worker ID:

```text
workerId = UUID
```

Example:

```text
4bd17c0d-7fee-4d91-8579-a6bf1a3e934d
```

This is useful for debugging, observability, ownership tracking and stale-claim recovery.

---

# Chapter: Actual Short-Claim Test

We again used:

```text
productId = 1111
```

to simulate a slow Kafka publish.

With both `8080` and `8081` running, one instance logged:

```text
OUTBOX CLAIMED
| worker=4bd17c0d-7fee-4d91-8579-a6bf1a3e934d
| eventId=3aeb9846-f116-4faa-bdfb-016f88d47ebb

OUTBOX PUBLISH ATTEMPT

SIMULATING SLOW KAFKA PUBLISH

KAFKA ACK RECEIVED

OUTBOX PUBLISHED
```

The other instance printed nothing for that event.

That proved:

```text
PENDING
↓
one short exclusive claim
↓
PROCESSING
↓
other workers skip it
↓
one Kafka publication
```

without holding the original row lock during the slow Kafka call.

---

# Chapter: Schema Evolution Issue — Java Enum vs PostgreSQL CHECK Constraint

After adding:

```text
PROCESSING
```

to the Java enum, PostgreSQL rejected the update with:

```text
violates check constraint "outbox_events_status_check"
```

The database constraint still only allowed:

```text
PENDING
PUBLISHED
```

while the application now allowed:

```text
PENDING
PROCESSING
PUBLISHED
```

We updated the DB constraint to include `PROCESSING`.

Conceptually:

```sql
ALTER TABLE order_event.outbox_events
DROP CONSTRAINT outbox_events_status_check;

ALTER TABLE order_event.outbox_events
ADD CONSTRAINT outbox_events_status_check
CHECK (
    status IN (
        'PENDING',
        'PROCESSING',
        'PUBLISHED'
    )
);
```

Important production lesson:

> Application enum changes and database constraints must evolve together.

`ddl-auto=update` is convenient for a POC, but production systems should normally use explicit migrations such as Flyway or Liquibase.

---

# Chapter: New Failure Window — Worker Dies After Claim

The short-claim design commits:

```text
PROCESSING
```

before Kafka work starts.

So if a worker dies after claim:

```text
PENDING
↓
PROCESSING
↓
COMMIT ✅
↓
worker dies ❌
↓
Kafka was never called
```

the row stays `PROCESSING`.

Without recovery logic, it is stuck forever.

---

# Chapter: Reproducing Stuck `PROCESSING`

We used:

```text
productId = 6666
```

to simulate:

```text
claim Outbox
↓
commit PROCESSING
↓
crash BEFORE Kafka publish
```

The row became:

```text
status       = PROCESSING
claimed_at   = populated
claimed_by   = worker UUID
published_at = NULL
```

Neither publisher picked it again because the normal claim query only searched for:

```text
status = PENDING
```

This proved:

```text
short transactions ✅
no long DB locks ✅
multi-instance claim safety ✅

but

dead worker can leave PROCESSING forever ❌
```

---

# Chapter: Outbox Lease and Stale Claim Recovery

We reused the same core idea already learned on the Inbox side:

```text
PROCESSING + fresh
→ assume current worker is alive

PROCESSING + stale
→ ownership expired
→ allow another worker to reclaim
```

`claimed_at` becomes the lease timestamp.

For the POC we chose a lease of:

```text
30 seconds
```

This is intentionally longer than the normal Kafka wait time so legitimate work is not stolen too early.

---

# Chapter: Claimable Rows After Lease Support

A row is claimable when:

```text
status = PENDING
```

or:

```text
status = PROCESSING
AND claimed_at < staleBefore
```

Conceptually:

```sql
SELECT *
FROM order_event.outbox_events
WHERE status = 'PENDING'
   OR (
        status = 'PROCESSING'
        AND claimed_at < :staleBefore
   )
ORDER BY created_at
LIMIT 10
FOR UPDATE SKIP LOCKED;
```

This gives us both new work and abandoned work.

---

# Chapter: Atomic Stale Reclaim

Suppose:

```text
ABC = PROCESSING
claimed_at = stale
```

Both `8080` and `8081` poll.

PostgreSQL behavior:

```text
8080
↓
SELECT ABC
FOR UPDATE SKIP LOCKED
↓
locks ABC

8081
↓
same query
↓
ABC locked
↓
skip ABC
```

Then the winning worker refreshes:

```text
claimed_at = NOW()
claimed_by = current worker
status = PROCESSING
```

and commits.

So stale reclaim itself remains exclusive.

---

# Chapter: `CLAIMED` vs `RECLAIMED`

For:

```text
PENDING
```

we log:

```text
OUTBOX CLAIMED
```

For:

```text
PROCESSING + expired lease
```

we log:

```text
OUTBOX RECLAIMED
```

Both end in:

```text
status = PROCESSING
claimed_at = now
claimed_by = current worker
```

The difference is whether ownership is new or recovered.

---

# Chapter: Actual Stale-Recovery Test

A previously stuck event was:

```text
eventId =
0ad3943b-8aa8-456a-a214-4885e378fd08
```

The publisher later logged:

```text
OUTBOX RECLAIMED
| worker=832eb131-cbe6-4feb-87ec-f08f91d829b7
| eventId=0ad3943b-8aa8-456a-a214-4885e378fd08

OUTBOX PUBLISH ATTEMPT

KAFKA ACK RECEIVED

OUTBOX PUBLISHED
```

This proves:

```text
stale PROCESSING
↓
RECLAIMED
↓
Kafka publish
↓
PUBLISHED
```

worked correctly.

The same worker also handled a normal new event:

```text
OUTBOX CLAIMED
| eventId=cde94fd0-6a96-4957-ab23-070ca6ac1107

OUTBOX PUBLISH ATTEMPT

KAFKA ACK RECEIVED

OUTBOX PUBLISHED
```

So both normal and recovery paths are working.

---

# Chapter: Current Production-Style Outbox Lifecycle

```text
PENDING
   ↓
atomic short claim
   ↓
PROCESSING
claimed_at
claimed_by
   ↓
commit claim
   ↓
DB lock released
   ↓
Kafka publish
   |
   ├── success
   │      ↓
   │   PUBLISHED
   │
   └── worker dies / failure
          ↓
      PROCESSING remains
          ↓
       lease expires
          ↓
       RECLAIMED
          ↓
       Kafka retry
```

This is now much closer to a real worker-queue model.

---

# Chapter: Current Remaining Failure Window

Even with stale recovery, one important window remains:

```text
Kafka ACK ✅
↓
worker crashes BEFORE PUBLISHED update ❌
↓
status remains PROCESSING
↓
lease expires
↓
RECLAIMED
↓
same event published again
```

So Outbox remains:

```text
at-least-once publication
```

not guaranteed exactly-once publication.

That is why stable `eventId` and the consumer Inbox remain necessary.

---

# Chapter: Short Claim vs Long `SKIP LOCKED` Transaction

| Design | Ownership | DB Lock During Kafka I/O | Crash Recovery | Main Trade-off |
|---|---|---:|---|---|
| Long `FOR UPDATE SKIP LOCKED` transaction | DB row lock | Yes | DB rollback releases lock | Simple, but holds DB resources |
| Short `PENDING → PROCESSING` claim | persisted claim state | No | lease + stale reclaim | More state/logic, better DB behavior |

The production-style design we are now using is:

```text
short claim transaction
+
PROCESSING state
+
claimed_at lease
+
claimed_by
+
stale reclaim
```

---

# Chapter: Updated End-to-End Reliability Architecture

```text
Client
  ↓
Order Service
  ↓
┌──────────────────────────────┐
│ PostgreSQL TX                │
│                              │
│ orders                       │
│ outbox_events(PENDING)       │
└──────────────┬───────────────┘
               ↓
             COMMIT
               ↓
        Outbox Claim Worker
               ↓
 PENDING / stale PROCESSING
               ↓
      short row lock
               ↓
          PROCESSING
     claimed_at / claimed_by
               ↓
             COMMIT
               ↓
       DB lock released
               ↓
          Kafka publish
               ↓
             ACK
               ↓
          PUBLISHED

               ↓

             Kafka

               ↓

      Notification Consumer
               ↓
       Inbox claim(eventId)
               ↓
     idempotent processing
               ↓
        Email Provider
               ↓
   Idempotency-Key = eventId
               ↓
            Mailpit
```

---

# Chapter: Updated POC Checkpoint

Additional Outbox work completed:

```text
✅ Added PROCESSING Outbox state

✅ Added claimed_at

✅ Added claimed_by

✅ Fixed PostgreSQL CHECK constraint
   to allow PROCESSING

✅ Learned application enum and DB constraint
   must evolve together

✅ Split claim from Kafka publication

✅ Short claim transaction implemented

✅ DB lock released before Kafka I/O

✅ Tested two instances with short claim

✅ Only one worker claimed the same PENDING row

✅ Other worker ignored PROCESSING row

✅ Reproduced worker crash after claim
   but before Kafka publish

✅ Verified row became stuck PROCESSING

✅ Added claim lease

✅ Added stale PROCESSING detection

✅ Added atomic stale reclaim

✅ Added CLAIMED vs RECLAIMED logs

✅ Recovered previously stuck event

✅ Verified Kafka ACK after reclaim

✅ Verified final PUBLISHED state

✅ Normal new event claim still works
```

---

# Chapter: Where We Are Now

The Outbox design currently supports:

```text
business transaction atomicity
Kafka outage recovery
multiple publisher instances
short DB transactions
publisher ownership
stale claim recovery
at-least-once publication
```

The remaining Outbox concept to verify is:

```text
Kafka ACK
↓
crash before PUBLISHED
↓
lease expiry
↓
reclaim
↓
duplicate Kafka record
↓
Inbox absorbs duplicate
```

After that final confirmation, the Outbox chapter can be considered complete and the POC can move to:

```text
Kafka partitions
key routing
parallelism
multiple consumers
rebalancing
```
