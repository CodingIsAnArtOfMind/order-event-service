# Kafka POC — Architecture Evolution & Interview Design Guide

> This is a **design/revision guide**, not a code walkthrough.
>
> Goal: explain the architecture on a whiteboard without reopening the implementation.
>
> For every stage, remember:
>
> **problem → why it happened → design change → what it solved → what remained**

---

# 1. The 30-Second Story

We started with:

```text
Order API
   ↓
PostgreSQL
   ↓
Kafka
   ↓
Notification Consumer
   ↓
Email
```

Then the design evolved:

```text
Basic Kafka consumer
        ↓
stable eventId
        ↓
persistent duplicate tracking
        ↓
check-then-act race discovered
        ↓
atomic Inbox claim
        ↓
PROCESSING / COMPLETED
        ↓
lease + stale reclaim
        ↓
exponential backoff + jitter
        ↓
provider-side idempotency
        ↓
remaining SMTP ambiguity
```

The main principle:

> We did not add patterns because they sound good.  
> We first reproduced the failure, then introduced the pattern that solves it.

---

# 2. Current Architecture

```text
                           ┌─────────────────────────┐
                           │        Client           │
                           │  POST /api/orders       │
                           └────────────┬────────────┘
                                        │
                                        v
                           ┌─────────────────────────┐
                           │ order-event-service     │
                           │ save order              │
                           │ create eventId          │
                           │ publish Kafka event     │
                           └───────┬─────────┬───────┘
                                   │         │
                                   │         v
                                   │   PostgreSQL
                                   │   order_event.orders
                                   │
                                   v
                           ┌─────────────────────────┐
                           │ Kafka                   │
                           │ topic: order-events     │
                           │ key: orderId            │
                           │ value: event            │
                           └────────────┬────────────┘
                                        │
                                        v
                    ┌────────────────────────────────────┐
                    │ order-notification-service         │
                    │ Inbox claim by eventId             │
                    │ PROCESSING / COMPLETED              │
                    │ lease / stale reclaim               │
                    │ exponential retry + jitter          │
                    └────────────────┬───────────────────┘
                                     │ HTTP
                                     │ Idempotency-Key = eventId
                                     v
                    ┌────────────────────────────────────┐
                    │ mock-email-provider-service        │
                    │ provider idempotency               │
                    │ PROCESSING / SUCCESS                │
                    │ duplicate HTTP request protection   │
                    └────────────────┬───────────────────┘
                                     │
                                     v
                               JavaMailSender
                                     │
                                     v
                                  Mailpit
```

---

# 3. Identity Model

```text
orderId
= business / aggregate identity

eventId
= exact event/message identity

Kafka key
= routing / ordering identity

partition + offset
= Kafka storage position
```

Example:

```text
orderId   = 18
eventId   = ABC
partition = 0
offset    = 15
```

Why:

```text
Kafka key = orderId
→ events for same order stay partition-affine

eventId
→ identifies one exact logical event
```

---

# 4. Stage 1 — Basic Kafka Consumer

Initial flow:

```text
Kafka
  ↓
Consumer
  ↓
send email
```

Pseudo-code:

```text
onMessage(event):
    sendEmail(event)
```

Happy path works.

But Kafka may redeliver because of:

```text
retry
restart
offset reset
rebalance
failure before commit
manual replay
```

So we needed stable event identity.

---

# 5. Stage 2 — Stable `eventId`

Producer creates:

```text
eventId = UUID
```

once.

Pseudo-code:

```text
event = {
    eventId = UUID.randomUUID()
    orderId = savedOrder.id
}
```

Consumer must NOT regenerate it.

Why:

```text
same Kafka replay
→ same eventId
→ same logical event
```

---

# 6. Stage 3 — Persistent Duplicate Tracking

We introduced persistent tracking:

```text
processed_events

event_id
order_id
event_type
processed_at
```

Flow:

```text
event arrives
    ↓
existsByEventId?
    |
    ├── YES → duplicate → return
    |
    └── NO
          ↓
       send email
          ↓
       save eventId
```

This handled simple replay after successful processing.

But concurrency exposed a race.

---

# 7. Stage 4 — Check-Then-Act Race

Unsafe flow:

```text
CHECK
  ↓
ACT
  ↓
WRITE
```

Specifically:

```text
exists(eventId)?
    ↓ false
send email
    ↓
insert row
```

Two workers can do:

```text
Worker A → exists=false
Worker B → exists=false
```

before either inserts.

Actual result:

```text
2 emails
1 DB row
```

Why only 1 row?

```text
UNIQUE(event_id)
```

rejected the second insert.

Important lesson:

> A unique constraint protects database state, but it cannot undo an external side effect that already happened.

---

# 8. Stage 5 — Atomic Inbox Claim

We replaced:

```text
SELECT exists?
then INSERT
```

with one atomic operation:

```sql
INSERT ...
ON CONFLICT (event_id) DO NOTHING
```

Pseudo-code:

```text
rows = tryClaim(eventId)

if rows == 1:
    I own event

if rows == 0:
    someone else already owns/handled it
```

Why better:

```text
Worker A ─┐
          ├── PostgreSQL decides one winner
Worker B ─┘
```

No application race between check and claim.

---

# 9. Inbox Table

```text
order_notification.inbox_events

event_id
order_id
event_type
status
claimed_at
completed_at
```

Lifecycle:

```text
NO ROW
   ↓
CLAIMED
   ↓
PROCESSING
   ↓
work succeeds
   ↓
COMPLETED
```

Important change:

```text
"row exists?"
```

became:

```text
"what state is this event in?"
```

---

# 10. Stage 6 — Crash After Claim, Before Email

Failure:

```text
claim succeeds
   ↓
PROCESSING
   ↓
application crashes
   ↓
email never sent
```

DB:

```text
PROCESSING
claimed_at populated
completed_at NULL
```

If every existing row is treated as duplicate:

```text
retry
  ↓
row exists
  ↓
return
```

email may never happen.

Lesson:

> `PROCESSING` is ownership, not proof of completion.

---

# 11. Stage 7 — Lease / Stale Claim

We changed:

```text
PROCESSING = permanent ownership
```

to:

```text
PROCESSING = temporary lease
```

POC example:

```text
lease = 5 seconds
```

Flow:

```text
PROCESSING + age < 5s
→ BUSY
→ do not steal

PROCESSING + age > 5s
→ stale
→ try reclaim
```

Pseudo-code:

```text
if COMPLETED:
    return COMPLETED

if PROCESSING and fresh:
    return BUSY

if PROCESSING and stale:
    try atomic reclaim
```

---

# 12. Atomic Stale Reclaim

```sql
UPDATE inbox_events
SET claimed_at = NOW()
WHERE event_id = :eventId
  AND status = 'PROCESSING'
  AND claimed_at < :staleBefore;
```

Interpretation:

```text
affected rows = 1
→ reclaim won

affected rows = 0
→ reclaim lost
```

Two workers may see stale state, but only one conditional update succeeds.

---

# 13. Stage 8 — `BUSY` Must Retry

Bad:

```text
BUSY
→ return normally
```

Kafka may think the record succeeded.

Correct:

```text
BUSY
→ throw retryable exception
→ Kafka retries later
```

---

# 14. Stage 9 — Retry Timing Must Match Lease Timing

Question:

> What if retries finish before the lease expires?

Example:

```text
lease = 10s

attempt = 0s
retry 1 = 1s
retry 2 = 2s
stop
```

Every attempt sees fresh `PROCESSING`.

So:

```text
retry window
must extend beyond
lease timeout
```

These settings must be designed together.

---

# 15. Stage 10 — Exponential Backoff + Jitter

We moved from fixed retry to:

```text
exponential backoff
+
jitter
```

Example:

```text
base:
2s
4s
8s

actual:
1.7s
3.9s
7.4s
```

Why exponential:

```text
give dependency/system more recovery time
```

Why jitter:

```text
avoid synchronized retry spikes / thundering herd
```

---

# 16. Crash-Before-Email Recovery — Working

Observed flow:

```text
CLAIMED
   ↓
crash before email
   ↓
retry
   ↓
BUSY
   ↓
retry later
   ↓
RECLAIMED
   ↓
EMAIL SENT
   ↓
COMPLETED
```

Result:

```text
1 logical Kafka event
1 email
1 COMPLETED Inbox row
```

---

# 17. Stage 11 — Crash AFTER Email

Failure:

```text
CLAIMED
   ↓
EMAIL SENT ✅
   ↓
CRASH ❌
   ↓
COMPLETED never written
```

DB still says:

```text
PROCESSING
```

Then:

```text
lease expires
   ↓
RECLAIMED
   ↓
send email again
```

Actual result:

```text
2 emails
```

Important lesson:

> Local DB state cannot prove whether an external side effect already happened.

These look identical locally:

```text
crash before email
→ PROCESSING

email succeeded then crash
→ PROCESSING
```

---

# 18. Stage 12 — Provider-Side Idempotency

Decision:

> The component that performs the side effect should enforce the idempotency contract.

We introduced:

```text
mock-email-provider-service
```

Notification service sends:

```text
Idempotency-Key = eventId
```

Architecture:

```text
Kafka
  ↓
notification service
  ↓ HTTP
Idempotency-Key = ABC
  ↓
email provider
  ↓
SMTP
```

---

# 19. Provider Request State

Provider table:

```text
email_provider.email_requests

idempotency_key
order_id
customer_id
product_id
quantity
status
created_at
sent_at
```

Provider state:

```text
PROCESSING
SUCCESS
FAILED
```

Result model:

```text
CLAIMED
ALREADY_SUCCESS
IN_PROGRESS
```

---

# 20. Provider Atomic Claim

Again:

```sql
INSERT ...
ON CONFLICT (idempotency_key) DO NOTHING
```

Pseudo-code:

```text
if insert wins:
    CLAIMED

if key exists:
    inspect status
```

Then:

```text
SUCCESS
→ ALREADY_SUCCESS

PROCESSING
→ IN_PROGRESS
```

---

# 21. Why `ALREADY_SUCCESS` Is Success

Caller asks:

```text
"ensure operation ABC happens"
```

If provider already has:

```text
ABC = SUCCESS
```

then desired state already exists.

So:

```text
first call:
200 SENT

same key later:
200 ALREADY_SENT
```

Both are success.

---

# 22. End-to-End Idempotency Chain

```text
Kafka eventId = ABC
        ↓
notification service
        ↓
HTTP header:
Idempotency-Key = ABC
        ↓
email provider
        ↓
provider request:
idempotency_key = ABC
```

Kafka retry preserves the same eventId, so provider sees the same logical operation.

---

# 23. Crash-After-Email Test After Provider Idempotency

First attempt:

```text
notification CLAIMED
   ↓
provider call ABC
   ↓
provider sends email
   ↓
provider stores SUCCESS
   ↓
returns SENT
   ↓
notification crashes
```

Later:

```text
notification BUSY
   ↓
lease expires
   ↓
RECLAIMED
   ↓
provider call ABC again
   ↓
provider sees SUCCESS
   ↓
ALREADY_SENT
   ↓
NO second SMTP send
   ↓
notification COMPLETED
```

Result:

```text
multiple Kafka attempts
multiple HTTP calls
1 actual email
```

---

# 24. Important Boundary — The Hard Problem Moved

Provider internals:

```text
provider claim
   ↓
PROCESSING
   ↓
SMTP send
   ↓
SUCCESS
```

Possible failure:

```text
SMTP succeeds ✅
   ↓
provider crashes ❌
   ↓
SUCCESS not saved
```

Provider DB:

```text
PROCESSING
```

while email already exists externally.

So ambiguity moved from:

```text
order-notification-service
```

to:

```text
mock-email-provider-service
```

That is the correct ownership boundary.

---

# 25. Provider Crash-After-SMTP Test

POC product:

```text
4444
```

Simulated:

```text
provider sends SMTP
        ↓
SMTP succeeds
        ↓
provider crashes
        ↓
markSuccess never happens
```

Actual result:

```text
Mailpit:
1 email ✅

Provider DB:
PROCESSING ❌
```

Later same key:

```text
IN_PROGRESS
IN_PROGRESS
IN_PROGRESS
```

Good:

```text
no duplicate SMTP send
```

Bad:

```text
provider request stuck forever
```

---

# 26. Current Unresolved Edge — Ambiguous Outcome

If provider `PROCESSING` becomes stale:

```text
Should we resend?
```

If yes:

```text
SMTP may already have sent
→ duplicate email
```

If no:

```text
SMTP may never have sent
→ lost email
```

This is an ambiguous external outcome.

Conceptual state discussed:

```text
UNKNOWN
```

Meaning:

```text
we lost certainty about whether the final side effect happened
```

Possible handling:

```text
PROCESSING + fresh
→ IN_PROGRESS

PROCESSING + stale
→ UNKNOWN
→ do not blindly resend
→ reconcile / inspect / manual/provider-specific recovery
```

This has NOT been implemented yet.

---

# 27. Core Design Principle

The goal is usually not:

```text
"code executes exactly once"
```

The practical goal is:

```text
at-least-once attempts
+
idempotent business effect
```

Retries are acceptable if the business outcome does not duplicate.

---

# 28. Responsibility Boundaries

## `order-event-service`

```text
order persistence
event creation
eventId generation
Kafka publication
```

## Kafka

```text
durable log
partition ordering
consumer groups
offsets
```

## `order-notification-service`

```text
Kafka consumption
consumer Inbox
atomic claim
lease/reclaim
retry/backoff/jitter
DLT
calling email capability
```

## `mock-email-provider-service`

```text
email API
Idempotency-Key
provider request state
SMTP interaction
```

## Mailpit / SMTP

```text
final local email acceptance for POC
```

---

# 29. Interview Question — Idempotent Kafka Consumer

## Ask

> Kafka may redeliver a message and the consumer performs an external side effect. How would you prevent duplicate processing?

Weak answer:

```text
synchronized
ConcurrentHashMap
```

Why weak:

```text
one JVM only
lost after restart
does not protect multiple pods
```

Better answer:

```text
stable eventId
persistent dedup
unique constraint
```

Strong answer:

```text
atomic Inbox claim
PROCESSING / COMPLETED
crash recovery
```

---

# 30. Expected Interview Diagram — Consumer

```text
Kafka
  ↓
Consumer
  ↓
Atomic Inbox Claim(eventId)
   |
   ├── COMPLETED
   |      ↓
   |   ignore
   |
   ├── CLAIMED
   |      ↓
   |   process
   |
   └── PROCESSING
          ↓
      fresh / stale
```

Then:

```text
success
  ↓
COMPLETED
```

---

# 31. Interview Follow-Up — Why Not `exists()` Then Insert?

Ask:

> Why isn't `existsByEventId()` followed by `save()` enough?

Expected:

```text
check-then-act race
```

Diagram:

```text
Thread A: exists=false ─────┐
                            ├─ both act
Thread B: exists=false ─────┘
```

Strong candidate proposes:

```text
atomic INSERT
unique key
ON CONFLICT DO NOTHING
affected row count
```

---

# 32. Interview Follow-Up — Isn't UNIQUE Enough?

Ask:

> If `event_id` is UNIQUE, isn't duplicate processing solved?

Expected:

```text
No.
```

Because:

```text
Thread A → external side effect
Thread B → external side effect

A INSERT → success
B INSERT → unique-key failure
```

Database:

```text
1 row
```

Business effect:

```text
2 executions
```

---

# 33. Interview Follow-Up — Crash After Claim

Ask:

> Consumer claims an event and crashes before doing the work. What now?

Expected:

```text
PROCESSING
lease / timeout
stale claim
recovery
```

Diagram:

```text
CLAIMED
   ↓
PROCESSING
   ↓
CRASH
   ↓
time passes
   ↓
STALE
   ↓
RECLAIM
```

---

# 34. Interview Follow-Up — Two Pods Reclaim Together

Ask:

> Two pods see the same stale row. How do you prevent both from reclaiming?

Expected:

```text
conditional atomic UPDATE
```

Example:

```sql
UPDATE inbox
SET claimed_at = NOW()
WHERE event_id = ?
  AND status = 'PROCESSING'
  AND claimed_at < staleBefore;
```

Interpret:

```text
rows=1 → winner
rows=0 → loser
```

---

# 35. Interview Follow-Up — Retry vs Lease

Ask:

> Claim timeout is 30s but all Kafka retries finish in 5s. What happens?

Expected:

```text
all retries see fresh PROCESSING
retry may exhaust before reclaim
```

Strong answer:

```text
retry policy and lease duration must be designed together
```

Good additions:

```text
exponential backoff
jitter
max elapsed time
DLT
```

---

# 36. Interview Follow-Up — Why Jitter?

Expected:

```text
avoid synchronized retries / thundering herd
```

Diagram:

```text
Without jitter:
Pod1 → 2.0s
Pod2 → 2.0s
Pod3 → 2.0s

With jitter:
Pod1 → 1.8s
Pod2 → 2.1s
Pod3 → 2.3s
```

---

# 37. Interview Follow-Up — Crash After External Success

Ask:

> External email/payment API succeeds, then consumer crashes before marking COMPLETED. What happens?

Expected:

```text
retry may repeat external side effect
```

Strong answer:

```text
consumer dedup alone is not enough
```

Discuss:

```text
provider idempotency key
external operation ID
provider-side dedup
```

---

# 38. Expected Interview Diagram — Provider Idempotency

```text
Kafka
  ↓
Consumer
  ↓
eventId = ABC
  ↓
HTTP
Idempotency-Key = ABC
  ↓
Provider
   |
   ├── first ABC
   |      ↓
   |   perform side effect
   |      ↓
   |   remember SUCCESS
   |
   └── later ABC
          ↓
       return previous success
       do not repeat effect
```

---

# 39. Interview Follow-Up — Who Owns Idempotency?

Expected:

```text
Both sides own their own boundary.
```

Consumer:

```text
stable eventId
Inbox
retry-safe processing
```

Provider:

```text
stable Idempotency-Key
deduplication of operation it performs
```

Strong statement:

> Enforce idempotency as close as possible to the irreversible side effect.

---

# 40. Interview Follow-Up — Is Header Alone Enough?

Expected:

```text
No.
```

Provider must persist and enforce it.

Pseudo-code:

```text
atomic claim key

if SUCCESS:
    return previous success

if new:
    perform operation
    persist SUCCESS
```

Without server-side state, `Idempotency-Key` is only metadata.

---

# 41. Interview Follow-Up — Provider Crashes After Side Effect

Ask:

> Provider sends email, then crashes before saving SUCCESS. What now?

Expected:

```text
ambiguous outcome
```

Strong candidate should mention:

```text
downstream idempotency
status lookup
external receipt / operation ID
reconciliation
UNKNOWN state
manual recovery
business duplicate tolerance
```

Do not blindly say:

```text
just retry
```

---

# 42. Full Interview Whiteboard Answer

Question:

> Design a Kafka notification pipeline that handles duplicate delivery, crashes, concurrent consumers, retries, and duplicate external side effects.

Strong step-by-step answer:

```text
1. Producer creates stable eventId

2. Kafka key = business aggregate ID
   eventId = message identity

3. Consumer atomically claims eventId
   in persistent Inbox

4. Inbox tracks:
   PROCESSING / COMPLETED

5. COMPLETED duplicate:
   ignore

6. fresh PROCESSING:
   BUSY
   retry later

7. stale PROCESSING:
   atomic reclaim

8. retry with:
   exponential backoff + jitter

9. external request sends:
   Idempotency-Key = eventId

10. provider deduplicates repeated calls

11. consumer marks COMPLETED only after:
    SENT / ALREADY_SENT

12. ambiguous outcomes:
    reconciliation / DLT / operational handling
```

---

# 43. Full Interview Whiteboard Diagram

```text
                     ┌───────────────────┐
                     │     Producer      │
                     │ eventId = UUID    │
                     └─────────┬─────────┘
                               │
                               v
                     ┌───────────────────┐
                     │       Kafka       │
                     │ key = orderId     │
                     └─────────┬─────────┘
                               │
                               v
              ┌────────────────────────────────┐
              │ Notification Consumer          │
              │ Atomic Inbox Claim(eventId)    │
              └───────────────┬────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        v                     v                     v
   COMPLETED              PROCESSING             CLAIMED
      │                    fresh/stale              │
      v                       │                     v
   Ignore               BUSY / RECLAIM       Call Provider
                                                   │
                                    Idempotency-Key = eventId
                                                   │
                                                   v
                               ┌──────────────────────────────┐
                               │ Email Provider               │
                               │ Atomic key claim             │
                               │ PROCESSING / SUCCESS         │
                               └──────────────┬───────────────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         │                    │                    │
                         v                    v                    v
                      SENT             ALREADY_SENT          IN_PROGRESS
                         │                    │                    │
                         └─────────┬──────────┘                    │
                                   v                               v
                           Mark COMPLETED                      retry later
```

---

# 44. What Should a Senior Candidate Mention?

For an 8+ year backend candidate, strong signals:

```text
event identity
consumer groups / offsets
persistent idempotency
unique constraints
atomic ownership
check-then-act race
PROCESSING / COMPLETED
retryable vs non-retryable
backoff
jitter
DLT
provider idempotency
external side-effect ambiguity
observability
```

They do not need our exact class/table names.

They should understand the failure modes.

---

# 45. Interview Red Flags

Be cautious if candidate says:

```text
Kafka exactly-once means duplicate email cannot happen.
```

or:

```text
Use synchronized.
```

or:

```text
Use ConcurrentHashMap.
```

or:

```text
Just retry forever.
```

or:

```text
Unique constraint alone prevents duplicate email.
```

or:

```text
If external API returned 200, we can always know that after JVM crash.
```

These miss distributed-system boundaries.

---

# 46. Strong 60-Second Interview Answer

> I would give every event a stable `eventId` and use the business aggregate ID as the Kafka key for ordering. The consumer would use a persistent Inbox and atomically claim the `eventId` instead of doing `exists-then-insert`, because that races across pods. The Inbox would track `PROCESSING` and `COMPLETED`; fresh claims are treated as busy, while stale claims can be atomically reclaimed. Retry should use exponential backoff and jitter, with timing designed around the lease. For external side effects such as email or payments, I would propagate the same `eventId` as an idempotency key so the provider can deduplicate repeated calls. I would only mark the Inbox completed after the provider reports success or that the same operation already succeeded. Ambiguous provider outcomes need reconciliation rather than blind retry.

---

# 47. Interview Pseudocode

```text
consume(event):

    result = inbox.claim(event.eventId)

    if result == COMPLETED:
        return

    if result == BUSY:
        retryLater()

    if result in [CLAIMED, RECLAIMED]:

        providerResult =
            emailProvider.send(
                idempotencyKey = event.eventId,
                payload = event
            )

        if providerResult in [SENT, ALREADY_SENT]:
            inbox.complete(event.eventId)
            return

        if providerResult == IN_PROGRESS:
            retryLater()

        failOrDLT()
```

---

# 48. Design Decision Table

| Problem | Why It Happened | Improvement | Solved | New Edge |
|---|---|---|---|---|
| Kafka redelivery | same record can arrive again | stable `eventId` | exact message identity | need persistent dedup |
| Duplicate after restart | memory disappears | DB tracking | survives restart/pods | check-then-act race |
| Two workers see new event | SELECT + action + INSERT not atomic | atomic `INSERT ... ON CONFLICT` | one owner | owner can crash |
| Owner crashes before email | `PROCESSING` stuck | lease + stale reclaim | recovery | retry timing |
| Retries finish too early | retry window shorter than lease | exponential retry | reclaim possible | retry storms |
| Retry storm | consumers retry together | jitter | spreads load | external ambiguity |
| Crash after email | DB and side effect separate | provider idempotency | retry-safe HTTP call | provider ambiguity |
| Provider SMTP succeeds then crashes | SMTP and provider DB separate | unresolved; reconciliation/provider guarantee needed | — | current edge |

---

# 49. Design Principles to Remember

```text
1. Stable identity before retry.

2. Persistent state before distributed dedup.

3. Avoid check-then-act for ownership.

4. Use database atomicity for claims.

5. PROCESSING != COMPLETED.

6. Ownership needs recovery if workers can die.

7. Retry timing and lease timing must be designed together.

8. Use exponential backoff + jitter for transient distributed failures.

9. External side effects need idempotency at the side-effect boundary.

10. "Exactly once code execution" is usually the wrong mental model.

11. Prefer:
    at-least-once attempts
    +
    idempotent business outcome.

12. Ambiguous irreversible outcomes should not be blindly retried.
```

---

# 50. Current POC Status Before Transactional Outbox

Completed:

```text
✅ Kafka producer / consumer
✅ stable eventId
✅ consumer-group / offset understanding
✅ retries / DLT
✅ persistent deduplication
✅ concurrency race reproduction
✅ atomic Inbox claim
✅ PROCESSING / COMPLETED
✅ lease / stale reclaim
✅ exponential backoff
✅ jitter
✅ crash-before-email recovery
✅ crash-after-email duplicate reproduction
✅ mock email provider
✅ provider Idempotency-Key
✅ provider atomic claim
✅ provider SUCCESS dedup
✅ notification → provider HTTP integration
✅ duplicate prevention at notification boundary
✅ provider crash-after-SMTP ambiguity reproduced
```

Still unresolved:

```text
⚠ provider can remain PROCESSING
  after SMTP succeeds but before SUCCESS is stored

⚠ raw SMTP does not provide a strong standard
  idempotency / transaction guarantee
```

Next major Kafka architecture problem:

```text
order-event-service

save order to PostgreSQL ✅
        ↓
publish Kafka event ❌
```

This leads to:

```text
Transactional Outbox
```

---

# 51. Next Interview Topic — Transactional Outbox

Question:

> Your API saves an order in PostgreSQL and then publishes `OrderPlacedEvent` to Kafka. What happens if the DB commit succeeds but Kafka publishing fails?

Expected direction:

```text
dual-write problem
        ↓
Transactional Outbox
```

That is the next POC stage.
