# Start here — the story so far

**Read this first, every session.** It takes five minutes and rebuilds the
context that a week away erodes. The weekly sheets go deep on one topic each;
this page is the spine that connects them.

## How to use these notes

Each `week-N-concepts.md` has three layers. Use the one that matches why
you're opening it:

| Layer | When to read it | What it's for |
|---|---|---|
| **Part 1 — The story** | learning it, or coming back after a gap | narrative + worked traces with real numbers. Rebuilds understanding, not just recognition. |
| **Part 2 — Self-test** | mid-week, 10 minutes | find retrieval gaps *before* a drill does |
| **Part 3 — Recall card** | night before an interview | dense summary; only useful once Part 1 has clicked |

If a Part 3 bullet doesn't immediately unpack in your head into the *why*,
go back to Part 1 for that section. That gap is the signal.

## The through-line

The whole project is one argument, and each week is the next move in it.
Read this chain top to bottom — every week exists because the previous
week's solution created a new problem.

**Week 1 — you can't have a distributed system without a way to move facts
between services.**
Kafka is a durable, replayable log rather than a queue, which buys temporal
decoupling: `inventory-service` can be down for two hours and catch up from
its own committed offset. But the log guarantees ordering only *within a
partition*, so ordering became a key-choice problem. And delivery is
**at-least-once** — the consumer might see the same event twice.
➜ *left us needing:* a way to make repeat deliveries harmless.

**Week 2 — a service that owns state must write to its database AND publish
an event, and those two writes can't be atomic.**
The transactional outbox collapses them into one DB write plus a background
relay. That relay is at-least-once by construction, which turns an impossible
problem (atomicity across two systems) into a solvable one (duplicates). The
solution to duplicates is the idempotency ledger — `processed_events`, keyed
by `event_id`, enforced by the database rather than by a HashSet.
➜ *left us needing:* the same fix at hop 2 (inventory still dual-writes), and
a way to stop duplicating event classes across services.

**Week 3 — next up.** Every service redefines `OrderCreatedEvent` by hand. The
moment one team adds a field, the others break silently. Schema registry and
contracts.

## The one-sentence version of each week

Useful when someone says "tell me about this project" and you have 30 seconds.

- **Week 1:** *A durable log decouples services in time, but only guarantees
  order within a partition and only promises at-least-once delivery.*
- **Week 2:** *You can't write atomically to a database and a broker, so you
  write only to the database and relay — accepting duplicates, then making
  duplicates harmless with a database-enforced idempotency key.*

## The unifying idea (say this in interviews)

Every pattern in this project **converts an impossible guarantee into a
merely difficult one**:

- Outbox converts *atomicity across two systems* → *duplicate deliveries*
- Idempotency converts *"deliver exactly once"* → *"apply exactly once"*
- Sagas convert *distributed rollback* → *compensating transactions*

Name the impossible thing and the difficult thing it became, and you're
answering at the right altitude.
