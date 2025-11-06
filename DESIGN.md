# deployd Design

## Overview

deployd is a zero-downtime container deployment tool that uses a **sans-IO architecture** to separate business logic from IO operations.

## Core Design Principles

### 1. Sans-IO Architecture
Business logic is implemented as pure state machines that:
- Never perform IO operations directly
- Never call `Instant::now()` - time is passed as parameters
- Emit commands representing intentions
- Process events representing outcomes

### 2. Dependency Inversion
Policy code (what to do) is decoupled from implementation (how to do it):
- State machine decides actions → emits commands
- Event loop executes commands → produces events
- State machine processes events → updates state

### 3. Determinism
No async in business logic means:
- No `tokio::spawn` creating race conditions
- No channels with ordering dependencies
- No `Arc<Mutex<T>>` causing contention
- Pure functions with predictable outputs

## Architecture Components

### State Machine (`Runner`)
Pure state machine managing container lifecycle:
- Maintains desired state via `DesiredState`
- Plans reconciliation between desired and actual state
- Allocates IPs from 127.0.0.2-254 range
- Tracks container metadata for lifecycle management
- All methods are synchronous - no `async`

**Key APIs:**
- `poll_command()` - Returns next command to execute
- `handle_event(event, now)` - Processes operation results
- `poll_timeout()` - When state machine needs wake-up
- `handle_timeout(now)` - Processes time-based logic
- `plan_reconcile(now)` - Pure reconciliation logic
- `execute_reconcile(actions)` - Emits commands for actions

### Command/Event Abstraction
Separates intention from execution:

**Commands** (what to do):
- `ListContainers`
- `PullImage`, `CreateContainer`, `StartContainer`
- `StopContainer`, `RemoveContainer`

**Events** (what happened):
- `ContainersListed`, `ImagePulled`, `ContainerCreated`
- `ContainerStarted`, `ContainerStopped`, `ContainerRemoved`
- `Error` - captures all failure modes

### Event Loop (`main.rs`)
Single place where all IO happens:
1. Polls commands from Runner
2. Executes commands via Docker API
3. Converts results to events
4. Feeds events back to Runner with current time
5. Manages timers based on `poll_timeout()`
6. Handles TCP proxying and config reloading

### Time Abstraction
State machine never queries current time:
- `poll_timeout()` declares when to wake up
- `handle_timeout(now)` receives time updates
- Enables instant testing of time-based behavior

## Data Flow

```
Event Loop (IO boundary)
    ↓ commands
Runner (pure logic)
    ↓ events
Event Loop
    ↓ time
Runner
```

Commands flow out, events flow in, time is injected.

## State Management

### DesiredState
Maps service names to server instances:
- Maintains insertion order for blue-green deployments
- Tracks multiple versions during transitions
- Latest server in list is the active one

### Container Lifecycle
States tracked via `ContainerMetadata`:
- Desired → Creating → Starting → Running
- Running → Stopping → Removing → Removed

Metadata tracks timestamps for grace periods and cleanup.

## Reconciliation

Two-phase process:

**Planning (pure logic):**
- Compare desired state with running containers
- Identify containers to create, stop, and remove
- Returns `ReconcileActions` struct

**Execution (via commands):**
- Emits commands for each action
- Commands queued in order: Pull → Create → Start
- Event loop executes and reports results

## Testing Strategy

### Unit Tests (no IO)
Test pure logic:
- Reconciliation planning with simulated container lists
- Time-based behavior by advancing `Instant`
- Command emission by checking queue
- IP allocation and state management

Run in microseconds, no Docker needed.

### Integration Tests (`#[ignore]`)
Optional end-to-end validation with real Docker:
- Execute full command/event cycle
- Verify actual container creation
- Test blue-green deployments

Only run when Docker is available.

## Benefits

**Testability**: Core logic testable without IO
**Speed**: Unit tests in microseconds vs seconds
**Determinism**: No flaky tests from timing or async
**Simplicity**: No `Arc<Mutex<T>>`, just `&mut self`
**Clarity**: Clear boundary between logic and IO

## Key Files

- `src/server.rs` - Runner state machine
- `src/server/commands.rs` - Command/Event types
- `src/server/desired_state.rs` - State management
- `src/main.rs` - Event loop
- `DESIGN.md` - This file

## References

- [Sans-IO Blog Post](https://www.firezone.dev/blog/sans-io) - Firezone's explanation
- [quinn-proto](https://docs.rs/quinn-proto/) - Sans-IO QUIC implementation example
