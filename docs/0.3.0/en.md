# reactive-pipeline-kit 0.3.0 — usage guide

[Complete API reference](reference.en.md)

[← Back to documentation and versions / Назад к документации и версиям](../index.md)

[English](en.md) · [Русский](ru.md)

[Versions](../index.md) · [README](../../README.md) · [Java features](java.en.md)

Typed Reactor pipelines with local retry boundaries, bounded record concurrency, and optional JSON and JVM diagnostics adapters.

## Before the API: one business request

A product-import service needs to validate each record, read its category and
return an enriched view. A temporary read failure should retry that read, not the
whole import. A slow first product should not create an unbounded queue of later
requests. These two boundaries—one attempt policy per step and one record budget
per subscription—are what this library adds to Reactor.

Read this guide in application order: dependencies and modules, a complete service,
then composition, error classification, record limits, I/O and diagnostics. The
later optional modules are not prerequisites for the first example. No Spring
container or library-specific application framework is needed.

## 1. Install the modules you need

Core and Jolt require Java 17. Core is enough for the service below; Java 25 scoped
context and Java 27 diagnostics are separate optional dependencies. In your
existing application's pom.xml add:

```xml
<dependency>
  <groupId>io.github.59alex</groupId>
  <artifactId>reactive-pipeline-kit</artifactId>
  <version>0.3.0</version>
</dependency>
```

Gradle Kotlin DSL:

```kotlin
dependencies {
    implementation("io.github.59alex:reactive-pipeline-kit:0.3.0")
}
```

The coordinates must exist in your configured repository. A Central release is
not currently claimed. [Contributor instructions](../../CONTRIBUTING.md) explain
local artifact verification without making source builds an application prerequisite.

| Module artifact (group io.github.59alex, version 0.3.0) | Minimum Java | Use when |
| --- | --- | --- |
| reactive-pipeline-kit | 17 | Typed Mono steps, record processing, HTTP and blocking adapters |
| reactive-pipeline-kit-jolt | 17 | Transform bounded JSON trees, with per-attempt mutable state isolation |
| reactive-pipeline-kit-java25 | 25 | Nested blocking calls need ScopedValue execution metadata |
| reactive-pipeline-kit-java27 | 27 | Record pipeline step diagnostics in an application-owned JFR recording |

## Practical scenario: enrich product records

The full [ProductService.java](examples/ProductService.java) is a service with a
small executable check. Supply your existing method catalog::readCategory; it
returns a cold Mono<String>. The application decides which remote errors are
transient; its transport is not inferred from the method name.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.ProcessingOptions;
import io.github.alex59.pipeline.StepPolicy;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ProductService {
    public record Product(long id, String sku) { }
    public record ProductView(long id, String sku, String category) { }
    private final Pipeline<Product, ProductView> pipeline;

    public ProductService(Function<Long, Mono<String>> readCategory) {
        // The caller supplies an ordinary reactive method, e.g. catalog::readCategory.
        // Приложение передаёт обычный реактивный метод, например catalog::readCategory.
        pipeline = Pipeline.<Product>start().withProcessing(ProcessingOptions.ordered(4))
                .map("validate", product -> {
                    if (product.id() < 1 || product.sku() == null || product.sku().isBlank()) {
                        throw new IllegalArgumentException("Positive id and SKU required");
                    }
                    return product;
                })
                .then("catalog", product -> readCategory.apply(product.id())
                        .map(category -> new ProductView(product.id(), product.sku(), category)),
                        StepPolicy.readOnlyRemoteCall());
    }

    public Flux<ProductView> enrich(Flux<Product> products) {
        return pipeline.process(products);
    }

}
```

The method reference is adapted by the one-argument then overload. Validation
runs once per record, then the category is mapped into the output. process uses
options already attached to the definition, so each call site needs only its
input publisher. Return this Flux from a reactive boundary; do not block or call
subscribe inside a WebFlux handler.

Expected for categories books: ProductView(1, BK-1, books), then
ProductView(2, BK-2, books). The in-memory check proves service wiring; the separate
[ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) also starts
a loopback HTTP catalogue, fails its first read with 503, then verifies two
validation runs and three HTTP attempts. Its small string bodies are buffered.

For an existing **Spring Boot 4.1.2 + WebFlux + Reactor Netty** app, follow the
[full starter integration](https://github.com/dlorian59/reactive-pipeline-spring-boot-starter/blob/main/docs/0.1.0/en.md):
Maven/Gradle WebFlux and WebClient dependencies, application.yml, ProductService,
CatalogClient and ProductController with NDJSON input/output and errors. The
starter supplies settings, not a second execution engine. No MVC behavior is implied.

The following snippets focus on one capability at a time. Their complete imports
and executable checks are in [Example.java](examples/Example.java); block is used
only at that CLI boundary. Optional examples are separate Java 25/27 consumers.

## 2. Compose typed and conditional steps

Pipeline<I,O> records input and output types in Java. map changes the output type; thenIf runs a same-type step only when its predicate matches.

```java
Pipeline<String, Integer> sizes = Pipeline.<String>start()
    .map("trim", String::trim)
    .thenIf("remove-prefix", value -> value.startsWith("id:"),
        (value, context) -> Mono.just(value.substring(3)), StepPolicy.defaults())
    .map("length", String::length);
Integer result = sizes.run(" id:java ").block();
check(Objects.equals(4, result), "typed result");
System.out.println("typed=" + result);
```

Example.java: typed(). Expected output: typed=4.

> **Cold, immutable definition**
>
> Adding a step returns a new definition. Each subscription to run starts a fresh execution; results are not cached. Each step must emit one non-null value. False conditions pass the original value through, and predicates run once per record.

Ordinary reactive methods can use then("read", value -> client.read(value)) or
then("read", client::read, policy). Keep the two-argument Step overload when you
need ExecutionContext. Both overloads use the same lazy per-attempt engine;
method invocation does not happen during definition construction.

## 3. Retry only the failing step

The policy wraps one step, so an enrichment retry does not rerun prior validation or preparation. The retry count excludes the first attempt.

```java
AtomicInteger previousStep = new AtomicInteger();
AtomicInteger attempts = new AtomicInteger();
StepPolicy policy = new StepPolicy(Duration.ofSeconds(2), 2,
    Duration.ofMillis(10), IOException.class::isInstance);
var pipeline = Pipeline.<Integer>start()
    .map("prepare", value -> { previousStep.incrementAndGet(); return value + 1; })
    .then("enrich", (value, context) -> attempts.incrementAndGet() < 3
        ? Mono.error(new IOException("temporary")) : Mono.just(value * 2), policy);
Integer result = pipeline.run(4).block();
check(Objects.equals(10, result) && previousStep.get() == 1 && attempts.get() == 3,
    "retry boundary");
System.out.println("retry=" + result + "; previous=" + previousStep + "; attempts=" + attempts);
```

Example.java: retry(). Expected: retry=10; previous=1; attempts=3.

```java
var pipeline = Pipeline.<Integer>start().<Integer>then("slow",
    (value, context) -> Mono.never(), StepPolicy.once(Duration.ofMillis(50)));
try {
    pipeline.run(1).block();
    throw new AssertionError("timeout expected");
} catch (StepFailure failure) {
    check(failure.getCause() instanceof TimeoutException, "timeout cause");
    System.out.println("timeout=" + failure.step() + "; attempts=" + failure.attempts());
}
```

Example.java: timeout(). Expected: timeout=slow; attempts=1. The original cause is TimeoutException.

> **Reliability boundary**
>
> Timeout applies to each attempt; the delay between retries is extra. Defaults are 30 seconds and no retries. A retry reuses the input reference and may repeat a remote effect after an ambiguous failure. Keep input immutable and use idempotency when effects must not repeat. A timeout requests cancellation; it cannot forcibly stop blocking Java code.

For a selected read-only call, StepPolicy.readOnlyRemoteCall() gives a three-second
attempt timeout, two additional attempts and 100ms delay. Only direct ConnectException, SocketTimeoutException, HttpTimeoutException and
TimeoutException qualify. readOnlyRemoteCall(predicate) lets your adapter
classify selected status/transport errors; it is not a blanket retry switch. It
must not be applied automatically to writes. once(Duration) is the explicit
no-retry profile. A wrapped error will not qualify unless your predicate says so.

## 4. Limit concurrency and choose output order

process subscribes to at most the configured number of records at once. Choose input order or completion order explicitly.

```java
var delayed = Pipeline.<Integer>start().then("delay",
    (value, context) -> Mono.delay(Duration.ofMillis((4 - value) * 20L)).thenReturn(value));
List<Integer> ordered = delayed.process(Flux.just(1, 2, 3), 3, Ordering.INPUT)
    .collectList().block();
List<Integer> completed = delayed.process(Flux.just(1, 2, 3), 3, Ordering.COMPLETION)
    .collectList().block();
check(List.of(1, 2, 3).equals(ordered), "input order");
check(completed != null && Set.copyOf(completed).equals(Set.of(1, 2, 3)), "all results");
System.out.println("ordered=" + ordered + "; completionContainsAll=true");
```

Example.java: ordering(). INPUT emits [1, 2, 3]. COMPLETION contains the same values, but their order depends on timing.

- INPUT can hold later completed records behind a slow earlier record; it does not order external side effects.
- The first error fails the stream and cancels other active records. Results already emitted stay emitted.
- The limit counts records, not bytes or threads. Large payloads and noncooperative cancelled I/O can still consume resources.

Attach a workload profile once when constructing the service:

```java
var pipeline = Pipeline.<Integer>start()
    .withProcessing(ProcessingOptions.ordered(4))
    .then("read", value -> Mono.just(value + 1));
Flux<Integer> results = pipeline.process(Flux.just(1, 2, 3));
```

Imports add io.github.alex59.pipeline.ProcessingOptions. ordered() chooses four
active records; ordered(n) preserves input output order with an explicit budget.
sequential() permits one active record, including invocation order.
completionOrder(n) permits n active records and emits results as they finish.
All profiles use prefetch 1, fail fast, propagate cancellation and create a new
budget per subscription. They are not a global semaphore or a byte limit.
The explicit process(source, concurrency, ordering) overload still overrides
attached settings for that call. Composition and observe preserve attached options.

## 5. Connect HTTP and propagate cancellation

HttpSteps.jdk adapts an application-owned HttpClient. The example starts a loopback server, so no external service is required.

```java
HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
server.createContext("/value", exchange -> {
    byte[] body = "try later".getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(503, body.length);
    try (var output = exchange.getResponseBody()) { output.write(body); }
});
server.start();
try {
    var client = HttpSteps.jdk(HttpClient.newHttpClient(), HttpResponse.BodyHandlers.ofString());
    var fetch = Pipeline.<HttpRequest>start()
        .then("fetch", client.asStep(), StepPolicy.once(Duration.ofSeconds(2)));
    URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/value");
    var response = fetch.run(HttpRequest.newBuilder(endpoint).GET().build()).block();
    check(response != null && response.statusCode() == 503, "HTTP status remains data");
    System.out.println("http=" + response.statusCode() + "; body=" + response.body());
} finally {
    server.stop(0);
}
```

Example.java: http(). Expected: http=503; body=try later. A 503 response is data, not an automatic Reactor error.

> **Status, timeout and body size**
>
> To retry selected HTTP statuses, classify them as errors inside the HTTP step. A later mapping error does not repeat the request. Timeout or downstream cancellation reaches the HTTP future, but the server may already have processed the request. BodyHandlers.ofString buffers the entire body; this example does not promise byte-stream processing.

## 6. Isolate blocking I/O; use virtual threads when available

BlockingSteps.call submits a fresh task for each attempt and passes ExecutionContext explicitly. The application owns the executor.

```java
var executor = Executors.newFixedThreadPool(2);
try {
    var lookup = Pipeline.<String>start().then("blocking", BlockingSteps.call(executor,
        (value, context) -> {
            Thread.sleep(5);
            return context.subscriberContext().<String>get("tenant") + ":" + value;
        }));
    String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
    check("team:java".equals(result), "explicit context");
    System.out.println("blocking=" + result);
} finally {
    executor.shutdownNow();
    check(executor.awaitTermination(5, TimeUnit.SECONDS), "executor shutdown");
}
```

Example.java: blocking(), Java 17+. Expected: blocking=team:java. The executor is stopped after the subscribed work ends.

```java
var executor = Executors.newVirtualThreadPerTaskExecutor();
```

Java 21/25/27: replace only the executor creation in the preceding example. The core API is unchanged; no optional module is required.

> **Cooperative cancellation**
>
> Cancellation requests interruption and covers the race during executor handoff. Code that ignores interruption can outlive cancellation and overlap a retry. Virtual threads help blocking I/O but do not accelerate CPU work or replace the process concurrency limit. Do not pass an executor that runs blocking work inline on the subscriber thread.

## 7. Optional Java 25 scoped context

Use the java25 module when nested synchronous calls need execution metadata without passing a parameter through every method. It delegates task submission and cancellation to the core.

```bash
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
```

Run in the library checkout with JDK 25. Java 27 is also supported for this adapter.

```kotlin
implementation("io.github.59alex:reactive-pipeline-kit-java25:0.3.0")
```

Add inside the consumer dependencies block; omit this artifact on Java 17/21.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.java25.ScopedBlockingSteps;
import java.util.concurrent.Executors;
import reactor.util.context.Context;

public final class ScopedExample {
    public static void main(String[] args) {
        if (ScopedBlockingSteps.currentContext().isPresent()) throw new AssertionError("unexpected binding");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var lookup = Pipeline.<String>start().then("lookup", ScopedBlockingSteps.call(executor, value -> {
                if (!Thread.currentThread().isVirtual()) throw new AssertionError("virtual thread expected");
                var context = ScopedBlockingSteps.currentContext().orElseThrow();
                return context.subscriberContext().<String>get("tenant") + ":" + value;
            }));
            String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
            if (!"team:java".equals(result)) throw new AssertionError(result);
            if (ScopedBlockingSteps.currentContext().isPresent()) throw new AssertionError("binding escaped");
            System.out.println("scoped=" + result + "; outsideBound=false");
        }
    }
}

```

Complete ScopedExample.java, Java 25+. Expected: scoped=team:java; outsideBound=false.

> **Synchronous scope only**
>
> The binding ends when the operation returns or throws, before downstream notification. It does not automatically follow Reactor scheduler hops or unrelated executor tasks. Use Reactor context across asynchronous boundaries; the core itself has no ambient currentContext method.

## 8. Transform JSON trees with Jolt

The optional Jolt adapter copies the specification and input tree, then creates a separate transform instance for each attempt.

```kotlin
implementation("io.github.59alex:reactive-pipeline-kit-jolt:0.3.0")
```

Add inside dependencies. This module is included in the default local build and requires Java 17+.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.jolt.JoltStep;
import java.util.List;
import java.util.Map;

public final class JoltExample {
    public static void main(String[] args) {
        var reshape = JoltStep.fromSpec(List.of(
            Map.of("operation", "shift", "spec", Map.of("id", "record.key"))));
        var pipeline = Pipeline.<Object>start().then("reshape", reshape);
        Map<String, Object> input = Map.of("id", 7);
        Object result = pipeline.run(input).block();
        if (!Map.of("record", Map.of("key", 7)).equals(result)) throw new AssertionError(result);
        if (!Map.of("id", 7).equals(input)) throw new AssertionError("input changed");
        System.out.println("jolt=" + result + "; inputUnchanged=true");
    }
}

```

Complete JoltExample.java. Expected: jolt={record={key=7}}; inputUnchanged=true.

> **Tree transformation, not a streaming JSON parser**
>
> Supported operations are shift, default, remove, cardinality and sort. Trees are fully materialized; arbitrary Java classes and scripts are not loaded. Nonfinite numbers, custom Number implementations and depth above 128 are rejected. CPU work runs on the subscribing thread unless the application explicitly chooses a scheduler.

## 9. Observe executions and record JFR on Java 27

Observers receive lifecycle events with a stable execution ID across the steps and retries of one run. Reactor context carries application metadata.

```java
var events = new ConcurrentLinkedQueue<StepEvent>();
var lookup = Pipeline.<String>start()
    .then("tenant", (value, context) -> Mono.just(
        context.subscriberContext().<String>get("tenant") + ":" + value))
    .observe(events::add);
String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
check("team:java".equals(result) && events.size() == 2, "diagnostic events");
check(events.stream().map(StepEvent::executionId).distinct().count() == 1, "execution identity");
System.out.println("events=" + events.stream().map(StepEvent::kind).toList());
```

Example.java: diagnostics(), Java 17+. Expected: events=[STARTED, SUCCEEDED]. A new subscription creates a new execution ID.

Keep observers nonblocking and thread-safe. Observer failures go to the Reactor onErrorDropped hook and do not cause business-operation retries. If that hook also throws a nonfatal exception, it is discarded without invoking the hook again. Reactor-fatal errors are rethrown. Step events omit payloads, but their exception object may still contain application data.

```bash
./gradlew -Pjava27 -PtestJavaVersion=27 build publishToMavenLocal
./gradlew -Pjava27 -PtestJavaVersion=27 :java27:profileJava27
```

Java 27, library checkout. The profiling task writes java27/build/recordings/pipeline.jfr using synthetic data and a cleared child environment.

```kotlin
implementation("io.github.59alex:reactive-pipeline-kit-java27:0.3.0")
```

Optional consumer dependency for JfrExample.java; the core minimum remains Java 17.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.java27.JfrStepObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;

public final class JfrExample {
    public static void main(String[] args) throws Exception {
        Path destination = Path.of(args.length == 0 ? "pipeline-example.jfr" : args[0]).toAbsolutePath();
        Files.createDirectories(destination.getParent());
        try (Recording recording = new Recording()) {
            recording.enable(JfrStepObserver.EVENT_NAME).withoutStackTrace();
            recording.start();
            Integer result = Pipeline.<Integer>start().map("increment", value -> value + 1)
                .observe(new JfrStepObserver()).run(6).block();
            if (!Integer.valueOf(7).equals(result)) throw new AssertionError(result);
            recording.stop();
            recording.dump(destination);
        }
        long count = RecordingFile.readAllEvents(destination).stream()
            .filter(event -> event.getEventType().getName().equals(JfrStepObserver.EVENT_NAME)).count();
        if (count != 2) throw new AssertionError("expected 2 step events, got " + count);
        System.out.println("jfrEvents=" + count);
    }
}

```

Complete JfrExample.java, Java 27. Expected: jfrEvents=2. The application starts and stops the recording.

> **JFR redaction boundary**
>
> JfrStepObserver copies only executionId, step, kind, attempt, elapsedNanos and errorType, alongside standard JFR metadata. It omits payloads and exception messages. Java 27 startup redaction applies to selected built-in startup events, not these custom fields. Use static non-sensitive step names; the library does not mutate JVM options or promise a measured performance gain.

## 10. Inspect failures and fix configuration problems

StepFailure names the failed stage, reports the number of attempts and preserves the original cause. Empty output is a failure, not an unnoticed record drop.

```java
var invalid = Pipeline.<String>start().then("lookup", (value, context) -> Mono.empty());
try {
    invalid.run("java").block();
    throw new AssertionError("empty step must fail");
} catch (StepFailure failure) {
    check("lookup".equals(failure.step()), "failed step name");
    check(failure.getCause() instanceof NoSuchElementException, "empty completion cause");
    System.out.println("failure=" + failure.step() + "; cause=" + failure.getCause().getClass().getSimpleName());
}
```

Example.java: failure(). Expected: failure=lookup; cause=NoSuchElementException.

- Step completed without a value: return one value or model an explicit result type instead of Mono.empty.
- duplicate step name: give each stage a unique, nonblank name before running the pipeline.
- RejectedExecutionException: the application executor is closed or rejects work. Fix its lifecycle or capacity before retrying.
- No matching variant or UnsupportedClassVersionError: an optional Java 25/27 artifact was added to an older runtime. Remove it or use the required JDK; core and Jolt still need only Java 17.
- Artifact 0.3.0 not found: publish the corresponding local module first and enable mavenLocal in the consumer. Add -Pjava25 or -Pjava27 when that module is needed.
- No work happens: run and process return cold publishers. Subscribe at the application boundary; do not add internal subscribe calls inside individual steps.

> **What the library does not guarantee**
>
> There is no durable execution log, restart recovery, distributed transaction or exactly-once side-effect guarantee. Use an external durable source/checkpoint layer when records must survive process crashes, and make retried effects idempotent.

## Downloadable examples

- [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java): complete business flow.

[Run examples and return to this guide](examples/README.md#english).

- [Example.java](examples/Example.java)
- [JfrExample.java](examples/JfrExample.java)
- [JoltExample.java](examples/JoltExample.java)
- [ScopedExample.java](examples/ScopedExample.java)

[MIT](../../LICENSE)

[← Back to documentation and versions / Назад к документации и версиям](../index.md)

<a id="source-walkthrough"></a>
## Follow one execution through the source

Start at [ProductService](examples/ProductService.java): constructor defines the
flow, enrich supplies the input. [Pipeline](../../core/src/main/java/io/github/alex59/pipeline/Pipeline.java)
keeps immutable steps/options; run creates an execution on subscription; execute
wraps only one step in timeout/retry; process uses bounded Reactor flattening.
[StepPolicy](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java)
selects attempt behavior and [ProcessingOptions](../../core/src/main/java/io/github/alex59/pipeline/ProcessingOptions.java)
selects record behavior. The HTTP, blocking and Jolt adapters implement Step;
they do not introduce another scheduler or orchestration layer.

Keep application decisions at the edges: credentials and endpoints in the client,
validation in the service, admission control and partial-response handling in
the transport boundary. [Contributing](../../CONTRIBUTING.md) explains source and
packaged-artifact checks. [Versions](../index.md) · [README](../../README.md).
