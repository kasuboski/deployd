# deployd Sans-IO Architecture

## Overview

deployd follows a **sans-IO** design pattern where core business logic is separated from IO operations. This architecture provides:

- **Testability**: Core logic can be tested instantly without Docker daemon
- **Determinism**: No race conditions from async tasks or timers
- **Composability**: State machines are easy to compose and reason about
- **Flexibility**: Event loop can be optimized independently of protocol logic

## Core Principle

The fundamental idea is **dependency inversion**:
- Policy code (what to do) doesn't depend on implementation details (how to do it)
- Instead, both communicate via abstractions
- Business logic becomes pure state machines
- Event loop handles all side effects

## Architecture Components

### 1. State Machine: `Runner`

The `Runner` is a pure state machine that:
- Maintains desired state (`DesiredState`)
- Makes decisions about container lifecycle
- **Never** performs IO directly
- Returns commands to execute and accepts events

**Key Methods:**
```rust
impl Runner {
    // Pure decision-making
    fn plan_reconcile(&mut self, running: HashSet<String>) -> ReconcileActions;

    // Time abstraction
    fn handle_timeout(&mut self, now: Instant);
    fn poll_timeout(&self) -> Option<Instant>;

    // Command abstraction
    fn poll_command(&mut self) -> Option<DockerCommand>;
    fn handle_event(&mut self, event: DockerEvent, now: Instant);

    // Pure business logic
    fn add(&mut self, service: &Service) -> ServerResult<String>;
    fn remove(&mut self, name: String) -> ServerResult<bool>;
}
```

### 2. Abstractions: Commands and Events

#### DockerCommand
Commands represent **intentions** - what the Runner wants to happen:

```rust
pub enum DockerCommand {
    ListContainers,
    PullImage { image: String },
    CreateContainer { name: String, config: Config<String> },
    StartContainer { name: String },
    StopContainer { name: String, timeout: u64 },
    RemoveContainer { name: String },
}
```

#### DockerEvent
Events represent **outcomes** - what actually happened:

```rust
pub enum DockerEvent {
    ContainersListed { containers: Vec<ContainerSummary> },
    ImagePulled { image: String },
    ContainerCreated { name: String },
    ContainerStarted { name: String },
    ContainerStopped { name: String },
    ContainerRemoved { name: String },
    Error { context: String, error: ServerError },
}
```

This command/event pattern separates:
- **Planning** (Runner emits commands)
- **Execution** (Event loop executes commands)
- **Feedback** (Runner processes events)

### 3. Time Abstraction

Time is never queried directly. Instead:

**`poll_timeout() -> Option<Instant>`**
- Runner declares when it next needs to be woken up
- Returns the earliest timeout among all pending timers
- Event loop schedules wake-up at this time

**`handle_timeout(now: Instant)`**
- Event loop calls this when time advances
- Runner receives explicit `Instant` parameter
- Runner updates state based on elapsed time

**Benefits:**
- Tests can "fast-forward" time instantly
- No `tokio::time::sleep` in business logic
- Deterministic time-based behavior

### 4. Event Loop: `main.rs`

The event loop is the **only** place where IO happens:

```rust
async fn main() -> Result<()> {
    let mut runner = Runner::new()?;
    let socket = TcpListener::bind("0.0.0.0:8080").await?;
    let mut next_timeout = Instant::now();

    loop {
        // 1. Execute commands from state machine
        while let Some(cmd) = runner.poll_command() {
            let event = execute_docker_command(cmd).await;
            runner.handle_event(event, Instant::now());
        }

        // 2. Wait for next event
        tokio::select! {
            // Config file changed
            Ok(_) = config_watcher.changed() => {
                let svc = load_config().await?;
                runner.add(&svc)?;
            }

            // Timer expired
            _ = tokio::time::sleep_until(next_timeout) => {
                runner.handle_timeout(Instant::now());
            }

            // New TCP connection
            Ok((conn, _)) = socket.accept() => {
                handle_connection(conn, &runner).await;
            }
        }

        // 3. Update next timeout
        if let Some(timeout) = runner.poll_timeout() {
            next_timeout = timeout;
        }
    }
}
```

**Event Loop Responsibilities:**
- Poll commands from Runner
- Execute Docker API calls
- Feed events back to Runner
- Manage timers based on `poll_timeout()`
- Handle TCP connections
- Watch config file changes

## Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                      Event Loop                         │
│                      (main.rs)                          │
└─────────────────────────────────────────────────────────┘
         │                           ▲
         │ DockerEvent               │ DockerCommand
         │                           │
         ▼                           │
┌─────────────────────────────────────────────────────────┐
│                  State Machine                          │
│                    (Runner)                             │
│                                                         │
│  - Maintains desired state                              │
│  - Plans reconciliation                                 │
│  - Manages IP allocation                                │
│  - Tracks container metadata                            │
└─────────────────────────────────────────────────────────┘
         │                           ▲
         │ Instant                   │ Instant
         ▼                           │
┌─────────────────────────────────────────────────────────┐
│                    Time Abstraction                     │
│                                                         │
│  poll_timeout() → When to wake up                       │
│  handle_timeout(now) → Process elapsed time             │
└─────────────────────────────────────────────────────────┘
```

## State Management

### DesiredState
Tracks the desired configuration:
- Maps service names to server instances
- Maintains insertion order for blue-green deployments
- Pure data structure with no IO

### Runner State
Contains:
```rust
pub struct Runner {
    // Core state
    desired: DesiredState,
    ips: IpProvisioner,

    // Command queue
    pending_commands: VecDeque<DockerCommand>,

    // Container metadata for tracking
    container_metadata: HashMap<String, ContainerMetadata>,

    // Pending operations
    reconcile_pending: bool,
}
```

### Container Lifecycle States

Containers transition through states:
1. **Desired** - Added to desired state, not yet created
2. **Creating** - CreateContainer command issued
3. **Starting** - StartContainer command issued
4. **Running** - Container is active
5. **Stopping** - StopContainer command issued
6. **Removing** - RemoveContainer command issued
7. **Removed** - Cleaned up from state

The Runner tracks these transitions via `ContainerMetadata`.

## Reconciliation Process

Reconciliation is split into two phases:

### Phase 1: Planning (Pure Logic)
```rust
fn plan_reconcile(&mut self, running: HashSet<String>) -> ReconcileActions {
    let to_remove = self.old_servers(&running);
    let mut to_create = Vec::new();
    let mut to_stop = Vec::new();

    // Determine what needs to happen
    for name in self.desired.server_names() {
        if !running.contains(name) {
            to_create.push(name.clone());
        }
    }

    // Containers running but not desired
    for name in &running {
        if !self.desired.server_names().any(|n| n == name) {
            to_stop.push(name.clone());
        }
    }

    ReconcileActions { to_create, to_stop, to_remove }
}
```

### Phase 2: Execution (IO via Commands)
```rust
fn execute_reconcile_actions(&mut self, actions: ReconcileActions) {
    for name in actions.to_remove {
        self.desired.remove_server(name);
    }

    for name in actions.to_stop {
        self.pending_commands.push_back(
            DockerCommand::StopContainer { name, timeout: 30 }
        );
    }

    for name in actions.to_create {
        let server = self.desired.get_server(&name).unwrap();
        self.pending_commands.push_back(
            DockerCommand::PullImage { image: server.service.image.clone() }
        );
        // Create and start commands follow
    }
}
```

## Testing Strategy

### Unit Tests (Fast, No IO)

**Test business logic directly:**
```rust
#[test]
fn test_reconcile_identifies_missing_containers() {
    let mut runner = Runner::new().unwrap();
    runner.add(&test_service()).unwrap();

    let actions = runner.plan_reconcile(HashSet::new());

    assert_eq!(actions.to_create.len(), 1);
}

#[test]
fn test_old_containers_removed_after_grace_period() {
    let mut runner = Runner::new().unwrap();
    let now = Instant::now();

    runner.add(&service_v1()).unwrap();
    runner.mark_container_replaced("deployd-test-v1", now);

    // Advance time by 31 seconds (instant!)
    runner.handle_timeout(now + Duration::from_secs(31));

    let cmd = runner.poll_command();
    assert_matches!(cmd, Some(DockerCommand::StopContainer { .. }));
}
```

### Integration Tests (Optional, Require Docker)

Keep existing `#[ignore]` tests for end-to-end validation with real Docker daemon.

### Property-Based Tests

```rust
proptest! {
    #[test]
    fn reconcile_is_idempotent(services in vec(arbitrary_service(), 0..10)) {
        let mut runner = Runner::new().unwrap();
        for svc in services {
            runner.add(&svc).unwrap();
        }

        let running = simulated_containers(&runner);
        let actions1 = runner.plan_reconcile(running.clone());
        let actions2 = runner.plan_reconcile(running);

        prop_assert_eq!(actions1, actions2);
    }
}
```

## Benefits Realized

### 1. Fast Tests
- Reconcile logic: ~microseconds
- Time-based behavior: instant
- Error handling: no mocking needed

### 2. Deterministic Behavior
- No race conditions from tokio::spawn
- No channel ordering issues
- Pure functions with predictable outputs

### 3. Easy Error Simulation
```rust
#[test]
fn test_handles_docker_daemon_down() {
    let mut runner = Runner::new().unwrap();
    runner.add(&test_service()).unwrap();

    runner.handle_event(DockerEvent::Error {
        context: "list_containers".into(),
        error: ServerError::DockerError(...)
    }, Instant::now());

    // Verify graceful handling
}
```

### 4. Simplified Code
- No `Arc<Mutex<T>>` in protocol code
- No channels between tasks
- Just `&mut self` for state mutation
- Direct function calls instead of message passing

## Implementation Guidelines

### For State Machines
1. **Never call `Instant::now()`** - accept `Instant` as parameter
2. **Never perform IO** - emit commands instead
3. **Use `&mut self`** liberally - ownership is clear
4. **Return data structures** - not async futures

### For Event Loop
1. **Poll commands in a loop** until none remain
2. **Execute commands** and create events
3. **Feed events back** to state machine with current time
4. **Update timers** based on `poll_timeout()`

### For Tests
1. **Test logic, not IO** - use `plan_*` methods
2. **Advance time explicitly** - pass modified `Instant`
3. **Simulate events** - create `DockerEvent` directly
4. **Assert on commands** - check `poll_command()` output

## Migration Path

1. ✅ Create abstractions (DockerCommand, DockerEvent)
2. ✅ Add command queue to Runner
3. ✅ Add time methods (poll_timeout, handle_timeout)
4. ✅ Extract pure logic from reconcile → plan_reconcile
5. ✅ Refactor main.rs to be event loop
6. ✅ Update tests to use new APIs
7. ✅ Remove async from Runner (except integration tests)

## Future Enhancements

### Health Checks
```rust
enum DockerCommand {
    // ...
    HealthCheck { name: String, endpoint: String },
}

enum DockerEvent {
    // ...
    HealthCheckPassed { name: String },
    HealthCheckFailed { name: String, reason: String },
}
```

### Graceful Shutdown
```rust
impl Runner {
    fn handle_shutdown_signal(&mut self, now: Instant) {
        // Drain connections over 30s
        self.shutdown_started_at = Some(now);
        // Emit commands to stop accepting new connections
    }
}
```

### Multi-Service Support
```rust
impl Runner {
    fn add(&mut self, service: &Service) -> ServerResult<String>;
    fn remove_service(&mut self, name: &str) -> ServerResult<()>;
    fn route_connection(&self, service_name: &str) -> Option<SocketAddr>;
}
```

## References

- [Firezone Sans-IO Blog Post](https://www.firezone.dev/blog/sans-io)
- [Sans-IO Protocol Implementations](https://sans-io.readthedocs.io/)
- [quinn-proto](https://docs.rs/quinn-proto/) - Sans-IO QUIC implementation
- [str0m](https://github.com/algesten/str0m) - Sans-IO WebRTC
