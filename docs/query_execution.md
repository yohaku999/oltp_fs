# Exchange Execution Design

## Goal

The query engine follows the Volcano iterator model: each operator exposes `open()`, `next()`, and `close()` and is originally implemented as a single-threaded iterator.

The goal of the exchange layer is to introduce parallel execution without forcing ordinary operators to understand threads, queues, batching, or shuffle policies.

Ordinary operators should behave the same whether they are executed directly in a pipeline or placed behind an exchange boundary.

## Design Principle

Parallelism is treated as an execution concern, not as ordinary operator logic.

This means that the following concerns are intentionally localized inside the exchange subsystem:

- producer/consumer coordination
- buffering and batching
- partitioning and routing
- flow control and backpressure
- worker lifetime and shutdown order

In other words, the exchange layer acts as an adapter between the iterator model and a parallel data transport.

## Why `open()` Owns Runtime Initialization

In the Volcano model, the constructor stores static configuration, while `open()` creates execution-time state.

For an exchange operator, execution-time state includes things such as:

- runtime queues or channels
- current batch state
- worker thread startup
- producer/consumer activation

This is consistent with the iterator model. The constructor should keep immutable configuration such as factory functions, producer counts, and batching policy. The `open()` call should build the runtime state for one execution.

## Coordinator-Centered Architecture

The current direction is to center the design around an `ExchangeCoordinator`.

The coordinator is responsible for execution management, not for driving the entire query pipeline. The pipeline is still advanced by ordinary iterator calls such as `next()`. The coordinator only manages the exchange boundary.

The coordinator owns responsibilities such as:

- creating and wiring producer-side and consumer-side endpoints
- owning transport state such as queues or channels
- starting producers during `open()`
- propagating shutdown during `close()`
- joining worker threads
- determining when the exchange is fully drained and closed
- surfacing failures from worker threads

This answers the key ownership question: producer behavior should not be managed ad hoc by producers themselves. The exchange coordinator is the component that is responsible for producer lifetime and transport lifetime.

## Producer, Sink, Source, and Consumer

The conceptual split is:

- producer: generates rows from an upstream iterator
- sink: producer-facing endpoint used to emit rows into the exchange transport
- source: consumer-facing endpoint used to receive rows from the exchange transport
- consumer: downstream iterator that reads from a source and exposes `next()` to the rest of the pipeline

The important design choice is that producers should not directly know which raw queue to push into. They should write to a sink abstraction. Routing decisions should happen behind that interface.

This keeps the producer side independent from transport topology.

## Routing and Partitioning

The exchange layer should support pluggable routing policy.

Examples include:

- pass-through
- broadcast
- round-robin
- hash partitioning for shuffle

This routing logic should not be hard-coded into ordinary operators. It belongs either in the sink implementation or in a dedicated routing policy used by the coordinator.

## Single Queue plus Dispatcher vs Partition-Aware Queues

There are two closely related implementation strategies.

### 1. Single queue plus dispatcher

All producers publish into one queue. A dispatcher then forwards rows to downstream consumers.

Benefits:

- simpler to prototype
- fewer moving parts
- easy to reason about initially

Costs:

- one transport path becomes a bottleneck
- backpressure becomes more global
- consumer-specific slowdown can affect unrelated partitions

### 2. Partition-aware transport

Rows are routed into separate queues or channels for each partition or destination.

Benefits:

- better isolation between partitions
- more natural fit for hash shuffle
- independent backpressure and drain behavior per partition

Costs:

- more transport objects to manage
- more explicit lifecycle coordination

The real distinction is not simply the number of queues. The deeper issue is whether partitions are treated as independent execution units.

## Why Partition Independence Matters

In shuffle-heavy operators such as hash join, some partitions may receive much more data than others, or some downstream consumers may be slower than others.

This leads to situations like:

- partition 3 is backlogged
- partition 0 is still able to make progress

If the transport is globally serialized through one queue, a local hotspot can slow down unrelated work. Partition-aware transport allows independent progress when workloads are skewed.

## Factory vs Builder vs Coordinator

The exchange design may eventually use a builder-style object to assemble topology, but execution ownership should stay with the coordinator.

Recommended split:

- builder/factory: creates topology and endpoints
- coordinator: owns runtime state and lifecycle
- producer/consumer: only use the endpoints presented to them

This keeps construction logic separate from execution management.

## Near-Term Implementation Direction

The design should be flexible enough to start with a simpler implementation and grow into a richer one.

Suggested path:

1. keep ordinary operators unchanged
2. introduce an exchange boundary with coordinator-managed lifetime
3. expose sink/source style endpoints instead of direct queue access
4. allow routing policy to evolve from simple dispatch to partition-aware shuffle

This preserves the iterator model while giving the execution engine room to support vertical parallelism, horizontal parallelism, and future shuffle-based operators such as hash join.