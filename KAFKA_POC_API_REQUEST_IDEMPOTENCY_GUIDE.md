# API Request Idempotency — Kafka POC Extension

> This guide documents the API-level idempotency extension added to the existing Kafka Order POC.
>
> It is organized by chapter and scenario so each idea stays attached to the problem it solves.
>
> Scope:
> - API duplicate requests
> - payment-style retry/double-click scenario
> - separate idempotent order endpoint
> - DB unique key + atomic claim
> - sequential duplicate test
> - concurrent duplicate test
> - relationship between API `Idempotency-Key` and Kafka `eventId`
> - optional production enhancements

---

# Chapter: Why API Idempotency Was Needed

## Context

Our Kafka POC already had downstream idempotency:

```text
Outbox
→ stable eventId

Kafka
→ Inbox dedupe by eventId

Notification Service
→ provider call

Provider
→ Idempotency-Key = eventId
```

That protects duplicate event delivery and duplicate downstream side effects.

But it does not protect the HTTP boundary itself.

---

## Scenario: Payment-Style Double Submit

Suppose a user presses:

```text
Pay ₹1000
```

The backend successfully processes the payment, but the HTTP response is delayed or lost.

The user presses Pay again.

Without API idempotency:

```text
Request 1
→ business operation executes

Request 2
→ business operation executes again
```

For payments:

```text
₹1000 deducted
+
₹1000 deducted again
```

For our Order POC:

```text
Order 58
+
Order 59
```

even though the user intended one logical operation.

---

# Chapter: What an Idempotency Key Represents

The rule is:

```text
one logical operation
=
one stable idempotency key
```

Example:

```text
Idempotency-Key: order-test-123
```

If the same logical operation is retried because of:

```text
network timeout
double click
mobile reconnect
browser retry
unknown response state
```

the same key must be reused.

---

## When Should a New Key Be Generated?

Same operation:

```text
Pay ₹1000
↓
network timeout
↓
retry

→ SAME key
```

A genuinely new payment later:

```text
new logical payment
→ NEW key
```

So:

```text
Idempotency-Key
= identity of one logical business request
```

It is not:

```text
user identity
```

and not:

```text
identity of every HTTP attempt
```

---

# Chapter: Client-Generated Key vs Backend-Generated Business ID

## Client-Generated Key

The UI/mobile app may generate:

```text
UUID ABC
```

and send:

```text
Idempotency-Key: ABC
```

Every retry of that same operation reuses `ABC`.

This is what our POC demonstrates.

---

## Backend-Generated Payment/Order Intent

A stronger production design may first create:

```text
POST /payment-intents
↓
Backend creates PAY-5001
```

Then payment confirmation uses:

```text
PAY-5001
```

for all retries.

Good production systems can use both:

```text
client request identity
+
backend business resource identity
```

---

# Chapter: Why Kafka eventId Alone Was Not Enough

Our Kafka POC generates:

```text
eventId = UUID
```

and carries that same event ID through:

```text
Outbox
↓
Kafka
↓
Inbox
↓
Provider Idempotency-Key
```

That protects downstream event processing.

But the event ID is created after the order/business operation begins.

So if the user submits two HTTP requests:

```text
Request 1
→ Order 58
→ eventId ABC

Request 2
→ Order 59
→ eventId DEF
```

Kafka sees:

```text
ABC != DEF
```

and treats them as two valid events.

Therefore:

```text
API Idempotency-Key
= protects BEFORE business operation

eventId
= protects AFTER business event exists
```

---

# Chapter: Keeping the Existing Order Flow Untouched

We intentionally did not modify:

```text
POST /api/orders
```

That remains the original Order + Outbox flow.

Instead we added:

```text
POST /api/idempotent/orders
```

Architecture:

```text
/api/orders
→ normal flow

/api/idempotent/orders
→ API idempotency demo
```

This kept the learning isolated and avoided risking the already-working flow.

---

# Chapter: API Idempotency Data Model

We added a table/entity conceptually like:

```text
order_event.api_idempotency
```

with fields:

```text
id
idempotency_key
status
order_id
created_at
completed_at
```

Status values:

```text
PROCESSING
COMPLETED
```

Most important DB rule:

```text
idempotency_key UNIQUE
```

---

# Chapter: Why the Unique Constraint Matters

Two requests may arrive nearly simultaneously:

```text
Request A ─┐
           ├── key = order-race-456
Request B ─┘
```

If we only did:

```text
SELECT
then INSERT
```

both requests could theoretically see:

```text
key not found
```

and both try to create the business operation.

The DB unique constraint makes ownership atomic.

---

# Chapter: Atomic Claim with ON CONFLICT

The repository uses the idea:

```sql
INSERT INTO order_event.api_idempotency
    (idempotency_key, status, created_at)
VALUES
    (:key, 'PROCESSING', :createdAt)
ON CONFLICT (idempotency_key)
DO NOTHING;
```

Meaning:

```text
first request
→ insert succeeds
→ claimed = 1

duplicate/concurrent request
→ key already exists
→ DO NOTHING
→ claimed = 0
```

So PostgreSQL becomes the final race-condition guard.

---

# Chapter: Transaction Boundary

The winning request performs one logical transaction:

```text
BEGIN

claim Idempotency-Key
+
create Order
+
create Outbox event
+
store orderId
+
mark idempotency COMPLETED

COMMIT
```

This avoids bad partial states such as:

```text
idempotency COMPLETED
but Order missing
```

or:

```text
Order exists
but idempotency record missing
```

---

# Chapter: Duplicate Request Behavior

If the request does not win the claim:

```text
claimed = 0
```

the service loads the existing idempotency row.

If:

```text
status = COMPLETED
order_id exists
```

the backend does not create anything again.

Instead:

```text
load original Order
↓
return same OrderResponse
```

Example log:

```text
IDEMPOTENT DUPLICATE
| key=order-test-123
| returningOrderId=58
```

---

# Chapter: Sequential Duplicate Test

## First Request

We used:

```text
Idempotency-Key: order-test-123
```

The backend created:

```text
Order 58
```

and one Outbox event.

The Outbox later published normally:

```text
KAFKA ACK RECEIVED
↓
OUTBOX PUBLISHED
```

---

## Duplicate Requests

The same key was sent again.

Logs showed:

```text
IDEMPOTENT DUPLICATE
| key=order-test-123
| returningOrderId=58
```

No second order was created.

No second Outbox event was created.

---

## Result

```text
multiple HTTP attempts
+
same Idempotency-Key

→ one Order
→ one Outbox event
→ same response
```

---

# Chapter: Concurrent Double-Click Test

We then tested two requests arriving almost simultaneously with:

```text
Idempotency-Key: order-race-456
```

Actual logs:

```text
IDEMPOTENCY KEY CLAIMED | key=order-race-456

ORDER + OUTBOX SAVED
| orderId=59
| eventId=4168edb6-ca8e-4d8f-b63c-e28584e24a9b
| outboxStatus=PENDING

IDEMPOTENT REQUEST COMPLETED
| key=order-race-456
| orderId=59

IDEMPOTENT DUPLICATE
| key=order-race-456
| returningOrderId=59
```

Then the Outbox continued normally:

```text
OUTBOX CLAIMED
↓
OUTBOX PUBLISH ATTEMPT
↓
KAFKA ACK RECEIVED
↓
OUTBOX PUBLISHED
```

---

# Chapter: What the Concurrent Test Proved

Two nearly simultaneous requests:

```text
Request A ─┐
           ├── same Idempotency-Key
Request B ─┘
```

resulted in:

```text
1 idempotency record
1 Order
1 Outbox event
1 eventId
```

The duplicate request returned:

```text
Order 59
```

instead of creating a second order.

This directly models:

```text
payment double-click
network retry
browser/mobile retry
unknown response state
```

---

# Chapter: Full Idempotency Architecture

Our POC now has idempotency at multiple boundaries:

```text
CLIENT / HTTP
↓
API Idempotency-Key
↓
ONE Order
↓
ONE eventId
↓
Transactional Outbox
↓
Kafka
↓
Inbox dedupe using eventId
↓
Notification Service
↓
Provider request
↓
Idempotency-Key = same eventId
↓
ONE external side effect
```

---

# Chapter: Three Idempotency Layers

## API Request Idempotency

Protects:

```text
same logical HTTP/business request submitted multiple times
```

Identity:

```text
Idempotency-Key
```

Example:

```text
double-click Pay
```

---

## Kafka Consumer Idempotency

Protects:

```text
same Kafka logical event delivered multiple times
```

Identity:

```text
eventId
```

Mechanism:

```text
Inbox
```

---

## External Provider Idempotency

Protects:

```text
same downstream HTTP side effect called multiple times
```

Identity:

```text
Idempotency-Key = eventId
```

Example:

```text
email provider should not send twice
```

---

# Chapter: API Key vs eventId

They are related but protect different boundaries.

```text
API Idempotency-Key
= logical client/business request identity
```

```text
eventId
= logical integration-event identity
```

Example:

```text
HTTP Idempotency-Key = order-race-456
↓
Order 59
↓
eventId = 4168edb6-ca8e-4d8f-b63c-e28584e24a9b
```

One client operation creates one order, which creates one integration event.

---

# Chapter: Request Payload Mismatch — Optional Production Enhancement

Suppose:

```text
Idempotency-Key = ABC
```

first request:

```text
amount = ₹1000
```

later request:

```text
same key = ABC
amount = ₹5000
```

A production system should normally reject this.

A common approach is to store:

```text
request fingerprint / hash
```

with the idempotency record.

Then:

```text
same key + same payload
→ return original result

same key + different payload
→ reject
```

This was discussed but not required for the core POC.

---

# Chapter: Why UI Button Disable Is Not Enough

The UI may disable the Pay button after click:

```text
Pay
↓
click
↓
Processing...
```

That helps user experience but is not a correctness guarantee.

Duplicates can still happen through:

```text
network retry
browser retry
mobile reconnect
multiple tabs
two near-simultaneous requests
```

Therefore backend idempotency remains necessary.

---

# Chapter: Payment-Style Interview Scenario

## Question

A payment succeeds on the backend, but the network response is lost. The user presses Pay again. How do you prevent charging twice?

## Answer

Use an idempotency key representing the logical payment attempt.

```text
first request
key = ABC
↓
charge succeeds
↓
store result for ABC

response lost

retry
key = ABC
↓
backend finds ABC already completed
↓
do not charge again
↓
return original result
```

---

# Chapter: Questions and Answers

## What is API idempotency?

Repeated submissions of the same logical request produce one business effect.

## What identifies the logical request?

A stable:

```text
Idempotency-Key
```

## Should every retry generate a new key?

No.

Retries of the same logical operation must reuse the same key.

## When should a new key be generated?

When a genuinely new business operation begins.

## Does Kafka eventId solve HTTP double-click duplication?

No.

`eventId` protects downstream event processing after the business operation already exists.

## Why use a DB unique constraint?

To protect against simultaneous requests racing to claim the same key.

## Why use ON CONFLICT DO NOTHING?

To implement an atomic one-winner claim.

## What does a completed duplicate return?

The original business result.

In our POC:

```text
same original OrderResponse
```

## Why store order_id in the idempotency record?

So duplicate requests can return the original Order.

## Why keep the endpoint separate from /api/orders?

To preserve the already-working order flow and keep the learning experiment isolated.

---

# Chapter: Scenario-to-Concept Map

## User double-clicks Pay

Concept:

```text
API Request Idempotency
```

## Network response is lost after backend success

Concept:

```text
reuse same Idempotency-Key
return original result
```

## Two requests arrive almost simultaneously

Concept:

```text
DB UNIQUE constraint
atomic claim
ON CONFLICT DO NOTHING
```

## Kafka delivers the same logical event twice

Concept:

```text
Inbox
eventId dedupe
```

## Notification retries the provider call

Concept:

```text
Provider Idempotency-Key
using the same eventId
```

---

# Chapter: What We Proved Experimentally

```text
Created a separate API idempotency endpoint.

Created api_idempotency table/entity.

Added UNIQUE idempotency_key.

Added PROCESSING / COMPLETED states.

Added atomic DB claim.

Kept original /api/orders flow unchanged.

First request created one Order.

First request created one Outbox event.

Sequential duplicate returned the same Order.

Sequential duplicate created no new Outbox event.

Concurrent duplicate test used the same key.

Exactly one request claimed the key.

Exactly one Order was created.

Exactly one Outbox event was created.

Duplicate request returned Order 59.

The resulting Outbox event published normally to Kafka.

API idempotency now protects the HTTP boundary.

Kafka eventId still protects the event boundary.

Provider Idempotency-Key still protects the external side-effect boundary.
```

---

# Chapter: Final Revision Summary

Remember the boundaries:

```text
HTTP duplicate
→ API Idempotency-Key

Kafka duplicate
→ eventId + Inbox

Provider duplicate
→ eventId as Provider Idempotency-Key
```

End-to-end:

```text
Client
↓
API Idempotency-Key
↓
ONE Order
↓
ONE eventId
↓
Outbox
↓
Kafka
↓
Inbox
↓
Provider Idempotency-Key
↓
ONE external side effect
```

Main lesson:

> Idempotency is not one single feature. It must be applied at every boundary where the same logical operation can be repeated.

---

# Chapter: Topic Status

API Request Idempotency is complete for this POC.

Implemented:

```text
separate endpoint
unique key
atomic claim
same-result replay
sequential duplicate protection
concurrent duplicate protection
Order + Outbox integration
```

Optional future production enhancements:

```text
request fingerprint/hash
same key + different payload rejection
idempotency record retention/expiry
cleanup/archive policy
```
