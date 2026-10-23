# Kafka POC — Multi-Broker, Replication, Quorum & Failover

> This guide continues the Kafka POC after the single-broker, partitions, consumer-groups and rebalancing chapters.
>
> It is organized by **chapter and scenario**, not as a flat checklist.
>
> Each chapter keeps together:
>
> - context
> - scenario
> - what happened
> - why it happened
> - resolution / decision
> - interview-style questions
>
> The 3-broker cluster used here is temporary. After the experiments, the extra brokers, network and test volumes will be removed to save Mac RAM and disk. The Compose file and this guide are kept for revision.

---

# Chapter: From One Broker to a Kafka Cluster

## Context

Before this chapter, our Kafka POC already had:

```text
one Kafka broker
topic: order-events
three partitions
consumer groups
parallel consumers
rebalance
Inbox + Outbox reliability
```

The next goal was to understand what changes when Kafka has multiple brokers.

We created a temporary 3-node KRaft cluster:

```text
kafka-1
kafka-2
kafka-3
```

Each node was configured as:

```text
broker + controller
```

So every node in this local POC did two jobs.

---

## Architecture

From the Mac:

```text
localhost:29092 → kafka-1
localhost:39092 → kafka-2
localhost:49092 → kafka-3
```

Inside the Docker network:

```text
kafka-1:19092
kafka-2:19092
kafka-3:19092
```

Controller listeners:

```text
kafka-1:9093
kafka-2:9093
kafka-3:9093
```

---

## Question: Why do all brokers use port 19092 internally?

Because the containers have different hostnames.

These are different network addresses:

```text
kafka-1:19092
kafka-2:19092
kafka-3:19092
```

The port can be the same because the hostname is different.

From macOS all three are reached through `localhost`, so unique host ports are needed:

```text
localhost:29092
localhost:39092
localhost:49092
```

---

## Question: Did localhost:29092 start all three brokers?

No.

This command:

```text
docker-compose up -d
```

started all three brokers.

Later, this Spring property:

```properties
spring.kafka.bootstrap-servers=localhost:29092
```

only meant:

```text
"Use Broker 1 as my initial entry point into the Kafka cluster."
```

---

# Chapter: KRaft Controller Quorum

## Context

We checked the KRaft metadata quorum and observed:

```text
ClusterId = 4L6g3nShT-eMCtK--X86sw
CurrentVoters = 1,2,3
```

That proved all three nodes belonged to one Kafka cluster.

---

## What Is Quorum?

Quorum means:

> the minimum majority of controllers that must be available so Kafka can safely make cluster metadata decisions.

For 3 controllers:

```text
majority = 2
```

So:

```text
2 of 3 alive → quorum exists
1 of 3 alive → quorum lost
```

---

## What Does the Controller Manage?

The controller manages cluster metadata such as:

```text
which brokers exist
which topics exist
which partitions exist
which broker leads each partition
broker failures
leader elections
configuration changes
```

---

## Why Majority?

Imagine a network split:

```text
Controller 1        Controller 2 + Controller 3
```

If both sides independently changed cluster metadata, Kafka could get conflicting state.

With majority voting:

```text
2-controller side
→ majority
→ allowed to continue

1-controller side
→ no majority
→ not allowed to make cluster decisions
```

---

## Quorum vs ISR

These are different concepts.

```text
Controller quorum
= cluster control / metadata safety
```

```text
ISR
= partition replica/data safety
```

Short revision:

```text
Quorum = who can manage the cluster
ISR    = which replicas are safely caught up
```

---

# Chapter: Controller Leader vs Partition Leader

## Context

The metadata quorum showed:

```text
LeaderId: 1
```

At the same time topic metadata showed:

```text
P0 Leader = 3
P1 Leader = 1
P2 Leader = 2
```

These are different leadership concepts.

---

## Controller Leader

The controller leader handles:

```text
cluster metadata
broker registration
partition leader elections
configuration changes
```

---

## Partition Leader

A partition leader handles:

```text
reads
writes
replication coordination
```

for one partition.

---

## Mental Model

```text
Controller leader
= manages cluster metadata

Partition leader
= serves one partition
```

---

# Chapter: Creating Three Partitions Across Three Brokers

## Scenario

We created:

```text
topic = order-events
partitions = 3
replication factor = 1
```

Kafka assigned:

```text
P0 → Leader Broker 3
P1 → Leader Broker 1
P2 → Leader Broker 2
```

---

## Why This Mattered

This proved:

```text
topic != one broker
```

A topic's partitions can be spread across brokers.

Architecture:

```text
Broker 1
└── P1 leader

Broker 2
└── P2 leader

Broker 3
└── P0 leader
```

---

## Question: Did we manually create P0, P1 and P2?

No.

We specified:

```text
--partitions 3
```

Kafka automatically created:

```text
Partition 0
Partition 1
Partition 2
```

and automatically assigned leaders.

---

# Chapter: Understanding Leader, Replicas and ISR

## Context

With replication factor 1, we saw:

```text
Partition: 0
Leader: 3
Replicas: 3
ISR: 3
```

The important doubt was:

> Does `Replicas: 3` mean three replicas?

No.

---

## Correct Meaning

```text
Leader: 3
```

means:

```text
Broker 3 is current leader
```

```text
Replicas: 3
```

means:

```text
the only configured copy is on Broker 3
```

```text
ISR: 3
```

means:

```text
Broker 3 is currently in sync
```

The value is a broker ID, not a replica count.

---

# Chapter: Bootstrap Broker and Metadata Discovery

## Context

We wanted to answer:

> If Spring only starts with Broker 1, how can it send to a partition whose leader is Broker 2?

We started the producer with:

```properties
spring.kafka.bootstrap-servers=localhost:29092
```

Cluster metadata was:

```text
P0 leader = B3
P1 leader = B1
P2 leader = B2
```

Our test key:

```text
999
```

mapped to:

```text
Partition 2
```

---

## Actual Result

The producer successfully sent:

```text
key=999
partition=2
offset=0
```

even though only Broker 1 was configured as bootstrap.

---

## What Happened Internally

```text
Spring producer
↓
connects to Broker 1
↓
gets cluster metadata
↓
learns:

P0 → B3
P1 → B1
P2 → B2

↓
key=999 maps to P2
↓
producer sends directly to B2
```

---

## Important Rule

```text
bootstrap broker
≠
broker that handles every message
```

The bootstrap broker is the initial entry point for metadata discovery.

---

## Why Configure Multiple Bootstrap Servers in Production?

Example:

```properties
spring.kafka.bootstrap-servers=localhost:29092,localhost:39092,localhost:49092
```

This is mainly for bootstrap availability.

If Broker 1 is down at application startup, the client can still contact Broker 2 or Broker 3 and discover the cluster.

---

# Chapter: Moving to Replication Factor 3

## Scenario

We recreated `order-events` with:

```text
3 partitions
ReplicationFactor = 3
```

Kafka showed:

```text
P0
Leader: 3
Replicas: 3,1,2
ISR: 3,1,2

P1
Leader: 1
Replicas: 1,2,3
ISR: 1,2,3

P2
Leader: 2
Replicas: 2,3,1
ISR: 2,3,1
```

---

## Architecture

```text
Broker 1        Broker 2        Broker 3
--------        --------        --------
P0 follower     P0 follower     P0 LEADER
P1 LEADER       P1 follower     P1 follower
P2 follower     P2 LEADER       P2 follower
```

Each partition now had:

```text
1 leader
2 followers
```

---

## Meaning of Replication Factor

```text
ReplicationFactor = 3
```

means:

```text
three configured copies of every partition
```

---

## Meaning of ISR

ISR means:

```text
In-Sync Replicas
```

These are replicas considered sufficiently caught up with the partition leader.

So:

```text
Replicas = configured copies
ISR      = currently healthy/in-sync copies
```

---

# Chapter: acks=all and min.insync.replicas

## Context

We configured:

```text
ReplicationFactor = 3
min.insync.replicas = 2
```

Producer logs showed:

```text
acks = -1
```

which is:

```text
acks=all
```

---

## What Does acks=all Mean?

The producer asks Kafka for the strongest normal acknowledgement behavior, subject to the topic's in-sync replica rules.

---

## What Does min.insync.replicas=2 Mean?

Kafka requires at least:

```text
2 in-sync replicas
```

for an `acks=all` write to succeed.

---

## Mental Model

```text
ReplicationFactor
= configured number of copies

ISR
= currently healthy/caught-up copies

min.insync.replicas
= minimum ISR required for strong write acceptance

acks=all
= producer requests that strong durability rule
```

---

# Chapter: Broker Failure and Clean Partition Leader Failover

## Scenario

Before failure:

```text
P2 Leader = Broker 2
ISR = 2,3,1
```

We stopped Broker 2.

Topic metadata became:

```text
P2 Leader = Broker 3
ISR = 3,1
```

---

## What Happened?

```text
Broker 2 dies
↓
P2 loses its leader
↓
Kafka chooses another safe ISR member
↓
Broker 3 becomes leader
```

This was a clean leader election because Broker 3 was already in ISR.

---

## Producer Behavior

The producer logged:

```text
Node 2 disconnected
```

but still successfully produced:

```text
key=999
partition=2
offset=1
```

So:

```text
old leader dies
↓
Kafka elects safe new leader
↓
producer refreshes metadata
↓
producer routes to new leader
↓
write continues
```

---

# Chapter: Why Writes Still Worked with One Broker Down

After Broker 2 failed:

```text
ISR count = 2
```

Topic requirement:

```text
min.insync.replicas = 2
```

Therefore:

```text
2 available
>=
2 required
```

and `acks=all` writes still succeeded.

---

# Chapter: NOT_ENOUGH_REPLICAS

## Scenario

To prove durability enforcement, we temporarily changed:

```text
min.insync.replicas = 3
```

while only 2 ISR members were available.

So:

```text
available ISR = 2
required ISR  = 3

2 < 3
```

---

## Actual Result

Kafka repeatedly returned:

```text
NOT_ENOUGH_REPLICAS
```

The producer kept retrying.

Our endpoint eventually threw:

```text
TimeoutException
```

because it used:

```java
.get(10, TimeUnit.SECONDS)
```

---

## Important Learning: Two Different Timeouts

```text
Application wait timeout
= how long our controller waits
```

```text
Kafka producer retry/delivery behavior
= how long Kafka may keep retrying internally
```

Our HTTP thread timed out while Kafka's producer retries continued.

---

## Resolution

We restored:

```text
min.insync.replicas = 2
```

---

# Chapter: Replica Recovery and ISR Rejoin

## Scenario

We restarted Broker 2.

Eventually all partitions returned to:

```text
ISR = 1,2,3
```

---

## What Happened?

```text
Broker 2 returns
↓
fetches missing partition data
↓
catches up with current leaders
↓
rejoins ISR
```

Important:

```text
configured replica
≠
automatically in ISR
```

It must be sufficiently caught up first.

---

# Chapter: Preferred Replica vs Current Leader

## Context

For P2:

```text
Replicas: 2,3,1
```

The first replica is the preferred leader:

```text
Broker 2
```

During Broker 2 failure:

```text
Broker 3 became current leader
```

Later Broker 2 recovered and leadership returned to Broker 2.

---

## Important Distinction

```text
preferred leader
≠
current leader at every moment
```

Failures can temporarily move leadership elsewhere.

Kafka can later rebalance toward the preferred replica layout.

---

# Chapter: Clean Leader Election

## Clean Election Rule

A safe leader election promotes a replica that is sufficiently caught up and considered safe.

Our earlier failover:

```text
B2 leader dies
↓
B3 already in ISR
↓
B3 becomes leader
```

was a clean leader election.

---

## Safety Decision

If no safe leader is available, Kafka prefers:

```text
partition unavailable
```

over knowingly losing acknowledged data.

---

# Chapter: Unclean Leader Election — Disposable Data-Loss Scenario

## Why a Separate Topic?

We created:

```text
unclean-demo
```

so the experiment could safely destroy data without touching `order-events`.

Configuration:

```text
1 partition
ReplicationFactor = 2
Leader = Broker 2
Follower = Broker 3
```

---

# Chapter: Building a Stale Replica

## Stage 1 — Both Replicas Healthy

We produced:

```text
A
B
```

Both brokers had:

```text
A B
```

---

## Stage 2 — Pause Broker 3

We paused Broker 3.

It stopped replicating and fell out of ISR.

Then we produced:

```text
C
D
E
```

Because:

```text
min.insync.replicas = 1
```

Broker 2 alone could acknowledge them.

Now:

```text
Broker 2:
A B C D E

Broker 3:
A B
```

---

# Chapter: Safe Failure — Leader None

## Stage 3 — Kill Broker 2

We stopped Broker 2.

Kafka showed:

```text
Leader: none
ISR:
```

Kafka did NOT promote Broker 3 because Broker 3 was stale.

This was the safe behavior.

---

## Why Leader None Was Correct

Kafka effectively saw:

```text
Broker 2
= safe/up-to-date
= unavailable

Broker 3
= available
= stale
```

So it chose:

```text
temporary unavailability
```

instead of:

```text
possible acknowledged-data loss
```

---

# Chapter: ELR — Eligible Leader Replicas

During the safe failure state we also saw:

```text
Elr: 2
LastKnownElr: 2
```

Kafka 4.x can track Eligible Leader Replicas (ELR), which helps represent replicas that remain eligible under newer leader-selection rules.

In our scenario:

```text
Broker 2
= safe/eligible candidate
= down
```

Broker 3 was alive but stale, so normal rules did not promote it.

---

# Chapter: Forcing Unclean Leader Election

## Scenario

We enabled:

```text
unclean.leader.election.enable=true
```

only on:

```text
unclean-demo
```

Then forced an unclean leader election.

Kafka elected:

```text
Broker 3
```

even though Broker 3 was stale.

Topic metadata became:

```text
Leader: 3
ISR: 3
```

---

# Chapter: Actual Data Loss — C D E

Broker 3 only had:

```text
A B
```

Once it became leader:

```text
A B
```

became the active authoritative history.

We consumed from the beginning.

Actual output:

```text
A
B
```

The consumer reported only 2 messages.

So:

```text
C
D
E
```

were no longer part of the active partition history.

This was real data loss, not a theoretical explanation.

---

# Chapter: Why the Old Leader Did Not Restore C D E

Later Broker 2 returned with its old local log:

```text
A B C D E
```

But Broker 3 was now the current leader with:

```text
A B
```

Kafka does not say:

```text
"Broker 2 has more messages, restore them."
```

Followers must converge to the current leader's authoritative history.

So Broker 2 must discard/truncate the divergent tail:

```text
C D E
```

and follow the current leader.

---

## Final Proof

We produced:

```text
F
```

Then consumed from the beginning.

Actual final history:

```text
A
B
F
```

Not:

```text
A
B
C
D
E
F
```

This proved:

```text
unclean election
↓
stale replica becomes authoritative
↓
old newer history can be permanently discarded
```

---

# Chapter: Safety vs Availability

## Clean Election

```text
no safe leader
↓
partition unavailable

priority:
DATA SAFETY
```

## Unclean Election

```text
stale replica promoted
↓
partition available again

priority:
AVAILABILITY

risk:
DATA LOSS
```

Interview sentence:

> Clean election prefers temporary unavailability over acknowledged-data loss. Unclean election may restore availability by promoting a stale replica, but newer records can disappear permanently.

---

# Chapter: Controller Leader Failure

## Scenario

Before failure:

```text
Controller Leader = Node 1
```

Topic leaders included:

```text
P0 → B3
P1 → B1
P2 → B1
```

Then we stopped:

```text
kafka-1
```

Because this local node is combined:

```text
Broker 1 role      DOWN
Controller 1 role  DOWN
```

---

## Controller Result

Metadata quorum became:

```text
LeaderId = 2
```

So:

```text
Controller 1 down
↓
Controllers 2 + 3 remain
↓
2/3 quorum exists
↓
Controller 2 becomes controller leader
```

---

## Partition Result

Topic metadata became:

```text
P0 → Leader B3
P1 → Leader B2
P2 → Leader B2
```

Partition leadership moved where necessary.

---

## Two Failovers Happened

```text
Controller leader failover
= control plane
```

```text
Partition leader failover
= data plane
```

One node failure triggered both because our local nodes run both broker and controller roles.

---

# Chapter: Why the Cluster Survived Node 1 Failure

Controller side:

```text
2/3 controllers alive
→ quorum exists
```

Data side:

```text
ISR count = 2
min.insync.replicas = 2
```

Therefore:

```text
cluster metadata control continues
+
strong writes can continue
```

---

# Chapter: Healthy State After Recovery

After restarting Broker 1:

```text
Controller Leader = 2
CurrentVoters = 1,2,3
MaxFollowerLag = 0
```

And `order-events` returned to:

```text
ISR = 1,2,3
```

for every partition.

The cluster was fully healthy again.

---

# Chapter: End-to-End Multi-Broker Architecture

```text
                    Spring Producer

                         │
                  bootstrap broker(s)
                         │
                         v

                cluster metadata
                         │
               partition leader map
                         │

       ┌─────────────────┼─────────────────┐
       │                 │                 │
       v                 v                 v

   Broker 1          Broker 2          Broker 3
   +Controller       +Controller       +Controller

       │                 │                 │
       └──────────── KRaft quorum ─────────┘

       Topic: order-events
       RF = 3
       minISR = 2

       Each partition:
       one leader
       two followers
       changing ISR based on health
```

---

# Chapter: Failure Decision Tree

## Partition Leader Failure

```text
Leader fails
↓
Is there a safe ISR/eligible replica?
        │
        ├── YES
        │    ↓
        │  clean election
        │    ↓
        │  new leader
        │
        └── NO
             ↓
       partition unavailable
```

If unclean election is deliberately enabled:

```text
no safe leader
↓
stale replica may become leader
↓
availability restored
↓
possible permanent data loss
```

---

# Chapter: Interview Questions and Answers

## What is a broker?

A Kafka server/node that stores partition replicas and may lead partitions.

## What is a controller?

A control-plane role that manages Kafka cluster metadata.

## What is quorum?

The majority of controllers required to safely make metadata decisions.

## What is controller leader?

The controller currently leading metadata decisions.

## What is partition leader?

The broker currently serving reads/writes for one partition.

## Is controller leader the same as partition leader?

No.

## What is replication factor?

How many configured copies each partition has.

## What is a follower replica?

A non-leader copy that replicates data from the partition leader.

## What is ISR?

Replicas considered sufficiently caught up with the leader.

## What is min.insync.replicas?

The minimum number of in-sync replicas required for strong `acks=all` writes.

## What does acks=all mean?

The producer asks Kafka to enforce the strongest normal acknowledgement behavior according to the topic's ISR requirements.

## Why did writes work with one broker down?

Because:

```text
ISR count = 2
minISR = 2
```

## Why did NOT_ENOUGH_REPLICAS occur?

Because:

```text
ISR count = 2
minISR = 3
```

## What is a bootstrap broker?

An initial broker used to discover the Kafka cluster and partition-leader metadata.

## Does every message pass through the bootstrap broker?

No.

After metadata discovery, the client talks directly to the correct partition leader.

## What is clean leader election?

Promoting a safe, sufficiently caught-up replica.

## What is unclean leader election?

Promoting a stale replica when no safe leader is available.

## Why is unclean election dangerous?

Because acknowledged records present only on the old leader may be lost.

## If the old leader comes back with more records, does Kafka restore them?

No.

The old broker must follow the current leader's authoritative log, even if that requires truncating its own divergent tail.

## Why use 3 controllers?

Because 3 controllers can tolerate one controller failure while still keeping a majority:

```text
2 of 3
```

---

# Chapter: Scenario-to-Concept Map

## Scenario: Producer configured with only Broker 1, but event goes to Broker 2

Concept:

```text
bootstrap broker
metadata discovery
partition leader routing
```

## Scenario: Broker 2 dies and P2 continues on Broker 3

Concept:

```text
replication
ISR
clean partition leader election
metadata refresh
```

## Scenario: Only 2 ISR available but minISR changed to 3

Concept:

```text
acks=all
min.insync.replicas
NOT_ENOUGH_REPLICAS
```

## Scenario: Broker 2 returns and rejoins ISR

Concept:

```text
replica catch-up
ISR recovery
```

## Scenario: Broker 3 stale, Broker 2 dies, Leader becomes none

Concept:

```text
clean election
data safety over availability
```

## Scenario: Broker 3 forced leader and C D E disappear

Concept:

```text
unclean election
availability over safety
data loss
log truncation
```

## Scenario: Controller leader Node 1 dies and Node 2 becomes leader

Concept:

```text
KRaft quorum
controller leader failover
control-plane availability
```

---

# Chapter: What We Proved Experimentally

We did not only read these concepts. We reproduced them.

```text
Built a real 3-broker KRaft cluster.

Verified all nodes share one ClusterId.

Verified 3-controller quorum.

Observed controller leader.

Observed topic partitions distributed across brokers.

Proved bootstrap metadata discovery.

Proved producer can bootstrap from B1
and send directly to B2.

Moved order-events to RF=3.

Observed leaders and followers.

Observed ISR.

Configured min.insync.replicas=2.

Confirmed acks=all behavior.

Killed a partition leader.

Observed clean leader failover.

Observed producer metadata refresh.

Observed writes continue after one broker failure.

Raised minISR above available ISR.

Observed NOT_ENOUGH_REPLICAS.

Observed application timeout while Kafka retries continued.

Restarted failed broker.

Observed replica catch-up and ISR rejoin.

Observed preferred-leader behavior.

Created disposable unclean-demo topic.

Made follower stale.

Observed stale follower leave ISR.

Killed leader.

Observed Leader=none under clean election.

Enabled and forced unclean election.

Promoted stale replica.

Consumed only A,B.

Proved C,D,E were lost.

Restarted old broker.

Produced F.

Consumed A,B,F.

Proved old leader's extra data does not return.

Killed controller leader node.

Observed new controller leader election.

Observed simultaneous partition leader failover.

Restarted node.

Observed full ISR recovery.
```

---

# Chapter: Final Revision Summary

Normal healthy flow:

```text
bootstrap broker
↓
metadata discovery
↓
partition leader
↓
leader accepts write
↓
followers replicate
↓
ISR tracks healthy copies
↓
acks=all + minISR enforce durability
```

Safe failover:

```text
leader fails
↓
safe ISR member exists
↓
clean election
↓
new leader
↓
producer refreshes metadata
↓
traffic continues
```

Dangerous path:

```text
leader fails
↓
no safe replica
↓
clean election refuses
↓
partition unavailable

OR

unclean election enabled
↓
stale replica becomes leader
↓
availability restored
↓
possible permanent data loss
```

Control-plane path:

```text
controller leader fails
↓
remaining controller majority
↓
new controller leader
↓
cluster metadata management continues
```

---

# Chapter: Temporary Cluster Cleanup Plan

The 3-broker cluster is only for learning.

After this guide is saved and the experiments are complete:

```text
stop Spring services
↓
docker-compose down -v --remove-orphans
↓
remove temporary broker containers
↓
remove compose network
↓
remove test volumes
↓
keep docker-compose.yml
↓
keep this Markdown guide
↓
return to lightweight single kafka-poc broker
```

This preserves revision material while keeping future Mac RAM and disk usage low.

---

# Chapter: Where the Kafka POC Stands Now

Core Kafka learning now includes:

```text
Transactional Outbox
Inbox/idempotency
retries and stale claims
provider idempotency
partitions
keys
offsets
consumer groups
parallelism
rebalancing
multiple brokers
KRaft controllers
quorum
metadata discovery
replication
ISR
acks=all
minISR
broker failure
partition leader failover
replica recovery
preferred leader
clean election
unclean election
controller leader failover
actual data-loss demonstration
```

The main Kafka architecture chapter is effectively complete.

Remaining work is production polish / optional extension:

```text
final architecture review
metrics and observability
Outbox cleanup / archival
retry / DLT final review
optional OpenSearch read model
```
