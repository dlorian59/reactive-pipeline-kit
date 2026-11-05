# 0.3.0 — complete API reference

[EN](reference.en.md) · [RU](reference.ru.md) · [Practical guide](en.md) · [Versions](../index.md) · [README](../../README.md)

The table inventory covers every public library type and its operations. Examples
are ordinary independent consumers of local Maven artifacts, not pseudocode.
[Run the examples](examples/README.md#english); each program asserts its result.
Java 17 is the core baseline. [Runtime/module selection](java.en.md) explains
Java 21 executors and optional Java 25/27 modules separately.

## Typed composition and execution

[Pipeline source](../../core/src/main/java/io/github/alex59/pipeline/Pipeline.java) ·
[Step source](../../core/src/main/java/io/github/alex59/pipeline/Step.java)

| API | Contract | Executable example |
| --- | --- | --- |
| Pipeline.start<T>() | Identity definition; immutable, reusable, no subscription | [Example.java: typed](examples/Example.java) |
| then(name, Step), then(name, Step, StepPolicy) | Append a typed asynchronous stage, using defaults or an explicit policy; returns a new definition | [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) |
| then(name, Function<T,Mono<R>>[, policy]) | Ordinary reactive method/lambda, without ExecutionContext; same lazy step and retry boundary | [ProductService](examples/ProductService.java) |
| withProcessing(ProcessingOptions), process(Publisher) | Attach immutable defaults; process uses them; default four active records in input order | [ProductService](examples/ProductService.java) |
| map(name, Function) | Synchronous nonblocking transformation with the default policy; must return a value | [Example.java: typed](examples/Example.java) |
| thenIf(name, Predicate, Step, StepPolicy) | Same-output-type transformation; false passes input through; predicate runs once, outside retry | [Example.java: typed](examples/Example.java) |
| observe(StepObserver) | Returns a definition using the replacement observer for all stages; does not mutate previous definitions | [Example.java: diagnostics](examples/Example.java) |
| run(input) | Cold Mono; each subscription creates an execution ID and new attempt counters; input reference is reused | [Example.java: retry](examples/Example.java) |
| process(Publisher, concurrency, Ordering) | Cold Flux with a positive per-subscription limit on active record subscriptions | [Example.java: ordering](examples/Example.java) |
| Step.apply(input, ExecutionContext) | Called anew per attempt; returns a cold Mono with exactly one non-null value | [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) |

Names must be unique and nonblank within a definition. Null arguments are rejected.
An empty step result or a null Mono is a failure; Mono<Void> is not a core step
output. A null map result becomes an empty Mono and therefore a failure. No
scheduler is installed: blocking inside a callback can block its subscribing
thread and delay timeout/cancellation delivery. Caller objects are not copied or
made thread-safe. The library does not subscribe for you or persist executions.

```java
var lengths = Pipeline.<String>start().map("trim", String::trim)
    .map("length", String::length)
    .process(Flux.just(" java ", " ts "), 2, Ordering.INPUT)
    .collectList().block();
// [4, 2]
```

Imports: io.github.alex59.pipeline.Pipeline, io.github.alex59.pipeline.Ordering,
reactor.core.publisher.Flux. This is a synchronous demonstration method body;
return the publisher from an asynchronous application boundary instead of blocking.
For the full validation → HTTP → view scenario, run
[ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java).

## Attempt policy, errors and ordering

| API | Contract | Source and example |
| --- | --- | --- |
| StepPolicy(timeout, maxRetries, retryDelay, retryOn) and accessors | Positive per-attempt timeout, nonnegative additional retry count and fixed delay, non-null predicate | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java), [Example.java: retry/timeout](examples/Example.java) |
| StepPolicy.defaults(), once(Duration) | Defaults: 30 seconds, no retries; once chooses a timeout and disables retries | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java), [Example.java: timeout](examples/Example.java) |
| StepPolicy.readOnlyRemoteCall(), readOnlyRemoteCall(Predicate) | Three-second attempts, two retries, 100ms delay; selected connection/timeout errors or explicit classifier; selected safe reads only | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java) |
| ProcessingOptions(concurrency, ordering), concurrency(), ordering() | Immutable positive record budget and non-null output order | [Source](../../core/src/main/java/io/github/alex59/pipeline/ProcessingOptions.java) |
| ProcessingOptions.ordered(), ordered(n), sequential(), completionOrder(n) | Four input-ordered records by default; explicit n; one record including invocation order; or completion-ordered n | [ProductService](examples/ProductService.java) |
| StepFailure.step(), attempts(), getCause() | Stable stage name, attempts actually started, last original cause; constructed by the pipeline | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepFailure.java), [Example.java: failure](examples/Example.java) |
| Ordering.INPUT | Preserve emission order; completed later records can wait behind an earlier record | [Source](../../core/src/main/java/io/github/alex59/pipeline/Ordering.java), [Example.java: ordering](examples/Example.java) |
| Ordering.COMPLETION | Emit as records finish; result order depends on timing | [Source](../../core/src/main/java/io/github/alex59/pipeline/Ordering.java), [Example.java: ordering](examples/Example.java) |

Timeout excludes retry delay. Retry invokes only the failing step, reusing the
input and execution context. A failed condition produces StepFailure with
attempts=0; it is not retried. A failed transformation reports at least one attempt.
The first process error cancels the source and other active records; already
emitted values and external effects are not rolled back. Concurrency counts
records, not bytes, threads or global requests. Each active inner subscription
has prefetch 1; payloads may still be large. INPUT does not order external effects.

[Retry example](examples/Example.java) expects result=10, previous step count=1,
attempts=3. [Business example](examples/ProductEnrichmentExample.java) expects
validationRuns=2 and httpRequests=3. Classify HTTP failures inside the retried
stage. Use one retry owner per I/O operation: stacking a pipeline policy with
Spring @ReactivePolicy or another retry operator multiplies attempts. Cancellation
cannot reverse a remote write; retries require idempotency/replay safety.

## Execution context and observation

| API | Contract | Source and example |
| --- | --- | --- |
| ExecutionContext(UUID, ContextView), executionId(), subscriberContext() | Non-null metadata; one ID across a run's stages/retries; captures subscriber context at subscription | [Source](../../core/src/main/java/io/github/alex59/pipeline/ExecutionContext.java), [Example.java: diagnostics/blocking](examples/Example.java) |
| StepObserver.onEvent(StepEvent), NONE | Synchronous nonblocking, thread-safe observer or no-op | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepObserver.java), [Example.java: diagnostics](examples/Example.java) |
| StepEvent constructor/accessors | executionId, step, attempt, kind, elapsed, error | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepEvent.java), [Example.java: diagnostics](examples/Example.java) |
| StepEvent.Kind | STARTED, SUCCEEDED, FAILED, CANCELLED, SKIPPED | [Source](../../core/src/main/java/io/github/alex59/pipeline/StepEvent.java), [Example.java: diagnostics/typed](examples/Example.java) |

Attempt numbers start at 1; a skipped/failed condition uses 0. Elapsed is monotonic
attempt duration; STARTED/SKIPPED and predicate failure use zero. FAILED includes
the original Throwable; other events have no error. Example diagnostics expects
[STARTED, SUCCEEDED] and one execution ID. Different subscriptions get different
IDs. Reactor context values remain application-owned; no ThreadLocal propagation
is added. StepEvent is a data record, not a validator of manually created events.

Nonfatal observer failures are sent to Reactor onErrorDropped and never trigger
business retries. A nonfatal failure of that hook is discarded without recursive
reporting; Reactor-fatal exceptions are rethrown. Events omit business payloads,
but error objects and application-selected step names may expose sensitive data.

## HTTP and blocking boundaries

| API | Contract | Source and example |
| --- | --- | --- |
| HttpPort.exchange(request, context), asStep() | Application HTTP boundary; cold Mono of one response; direct adapter to Step | [Source](../../core/src/main/java/io/github/alex59/pipeline/HttpPort.java), [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) |
| HttpSteps.jdk(HttpClient, BodyHandler) | Async send; cancellation reaches the CompletableFuture; caller owns client and body policy | [Source](../../core/src/main/java/io/github/alex59/pipeline/HttpSteps.java), [Example.java: http](examples/Example.java) |
| BlockingSteps.call(ExecutorService, Operation), Operation.apply(input, context) | Fresh executor task per attempt; explicit context; checked exceptions become errors | [Source](../../core/src/main/java/io/github/alex59/pipeline/BlockingSteps.java), [Example.java: blocking](examples/Example.java) |

HTTP status is data: the HTTP example prints 503 with body "try later". Classify
retryable status inside the HTTP stage, as the business example does. The body
handler chooses buffering; ofString buffers the entire body. Cancellation cannot
guarantee the server did not process a request. There is no authentication,
endpoint policy, response-size limit or distributed transaction manager.

The blocking example prints blocking=team:java and closes its executor. Cancellation
requests interruption, including the executor-handoff race. Noncooperative work
can outlive cancellation and overlap retries; rejected tasks become errors. Do
not pass an inline executor. Java 21+ virtual-thread executors work through this
same core adapter; they do not need the Java 25 artifact and do not accelerate CPU
work. Application code owns executor shutdown and thread-safety.

## Optional Jolt transformations — Java 17+

[JoltStep source](../../jolt/src/main/java/io/github/alex59/pipeline/jolt/JoltStep.java) ·
[Executable JoltExample.java](examples/JoltExample.java)

JoltStep.fromSpec(List) snapshots/validates a specification. apply(input, context)
returns a cold Mono; each attempt copies the tree and compiles an isolated Chainr.
Supported operations: shift, default, remove, cardinality and sort. No arbitrary
transform classes or scripts are loaded. The sample shifts id into record.key,
prints jolt={record={key=7}} and asserts inputUnchanged=true.

Trees accept string-keyed Maps, Lists, null, String, Boolean, Byte, Short, Integer,
Long, exact BigInteger/BigDecimal classes and finite Float/Double values. Other
objects/numbers, non-string keys, cycles/depth above 128 are rejected. Entire trees
are materialized; this is not a streaming JSON parser. CPU work runs on the
subscribing thread unless the application selects a scheduler. Null root output
completes empty, which the core reports as a failed step. Context is not used by
Jolt. Spec compilation per attempt trades setup cost for isolated mutable state.

```sh
./gradlew publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=JoltExample
```

Artifact: io.github.59alex:reactive-pipeline-kit-jolt:0.3.0. This is local Maven
publication, not a claimed Central release.

## Optional scoped blocking context — Java 25+

[ScopedBlockingSteps source](../../java25/src/main/java/io/github/alex59/pipeline/java25/ScopedBlockingSteps.java) ·
[Executable ScopedExample.java](examples/ScopedExample.java)

ScopedBlockingSteps.call(ExecutorService, Operation) binds the current ExecutionContext
only while Operation.apply(input) runs synchronously. currentContext() returns
Optional.empty outside that scope. Task lifecycle/cancellation delegates to core
BlockingSteps. The binding ends before downstream notification, also on errors;
it does not propagate to arbitrary async tasks or Reactor scheduler changes.
The example prints scoped=team:java; outsideBound=false and uses virtual threads.

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=ScopedExample
```

Artifact: io.github.59alex:reactive-pipeline-kit-java25:0.3.0. Requires an installed
Java 25+ toolchain; the core remains Java 17. No preview features are required.

## Optional JFR observer — Java 27

[JfrStepObserver source](../../java27/src/main/java/io/github/alex59/pipeline/java27/JfrStepObserver.java) ·
[Executable JfrExample.java](examples/JfrExample.java) ·
[Profiling fixture](../../java27/src/examples/java/io/github/alex59/pipeline/java27/ProfileExample.java)

new JfrStepObserver() supplies a stateless StepObserver. EVENT_NAME is
io.github.alex59.pipeline.Step; enable that event in an application-owned Recording.
onEvent(StepEvent) creates an independent event only if it should commit. It writes
executionId, step, kind, attempt, elapsedNanos and errorType. It copies neither
payloads nor exception messages and does not open/close recordings or change VM
options. Static non-sensitive step names are still required: startup redaction
does not sanitize custom event fields. [JFR/redaction details](../JFR-JAVA27.md).

```sh
./gradlew -Pjava27 -PtestJavaVersion=27 publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=JfrExample
./gradlew -Pjava27 -PtestJavaVersion=27 :java27:profileJava27
```

Artifact: io.github.59alex:reactive-pipeline-kit-java27:0.3.0. Java 27 is a separate
optional baseline, not LTS. The sample checks two recorded events; adapter source and tests describe/check
the payload-free event fields. The profiling task writes a synthetic recording;
it is not a production performance benchmark. It needs an installed Java 27 JDK.
These documentation changes did not rerun the optional runtime matrix.

## Spring integration and choosing a boundary

The separate [Spring starter](https://github.com/dlorian59/reactive-pipeline-spring-boot-starter)
provides PipelineFactory, configuration and opt-in @ReactivePolicy for service
Mono/Flux methods. Read its versioned reference and AnnotatedExampleApplication
through its README. Clone the companion repository alongside this library and build its artifacts
locally; Maven Central publication is separate. It pulls only
core, not Jolt/Java 25/27. Unlike a core step, its AOP boundary allows empty results,
preserves original error types and does not create execution/observer metadata.

Use typed pipelines to compose distinct stages; use an injected service proxy for
one explicitly bounded service operation. Do not mark single-use streams or
nonrepeatable writes retry-safe without a replay/idempotency contract. Neither
integration provides a queue, durable workflow recovery or exactly-once effects.

[← Practical guide](en.md) · [Examples](examples/README.md) · [Versions](../index.md) · [README](../../README.md)
