# reactive-pipeline-kit

[![Build](https://github.com/dlorian59/reactive-pipeline-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/dlorian59/reactive-pipeline-kit/actions/workflows/ci.yml) [![Java 17+](https://img.shields.io/badge/Java-17%2B-437291)](docs/JAVA-SUPPORT.md) [![MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

**Version:** [0.3.0](docs/0.3.0/en.md) · [Versions](docs/index.md) · [API reference](docs/0.3.0/reference.en.md)

Enrich and transform records through named, typed Reactor steps. Each step gets
its own failure boundary: a temporary catalogue outage can retry its read without
repeating successful validation. One reusable definition handles a single record
or a stream with bounded record concurrency and predictable output ordering.

Useful for imports, API composition and transformation services with independent
retry/timeout requirements. For a short chain with no such boundaries, ordinary
Reactor operators are enough. The library does not persist jobs or schedule them.

## Add to an existing Java project

Java 17+ and a reactive caller. Maven:

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

Use your application's configured artifact repository. A Maven Central release is
not yet claimed; [contributor/local verification](CONTRIBUTING.md) is separate from
application integration. Core has no Spring dependency. For an existing **Spring
Boot 4.1.2 WebFlux + Reactor Netty** application, use the
[companion starter's complete controller/service/configuration example](https://github.com/dlorian59/reactive-pipeline-spring-boot-starter/blob/main/docs/0.1.0/en.md).
It includes both WebFlux and WebClient dependencies and retains this same engine.

## Put the flow in your service

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

Construct ProductService with your ordinary catalogue method, for example
new ProductService(catalog::readCategory). That method returns a cold Mono<String>;
it must classify remote failures before returning. No special port implementation,
context argument, reflection registry or framework callback is required.

Return service.enrich(records) from a reactive caller. It emits ProductView records
in input order, with at most four record subscriptions active. The definition is
immutable; each subscription owns its attempts and execution ID. The complete
[ProductService.java](docs/0.3.0/examples/ProductService.java) includes a small
asserted consumer, and the [HTTP business example](docs/0.3.0/examples/ProductEnrichmentExample.java)
checks an actual 503 then successful retry.

The readOnlyRemoteCall profile allows two additional attempts, 3 seconds each,
100ms retry delay, for direct ConnectException, SocketTimeoutException, HttpTimeoutException or TimeoutException only. It does not
retry arbitrary HTTP statuses, auth or validation failures. Wrapped transport
errors need explicit classification, or readOnlyRemoteCall(predicate). Ordinary
then and map have a 30-second timeout and no retry. Retry safety is your business decision.

## Capabilities with examples

| Capability | Short example | Details |
| --- | --- | --- |
| Typed ordinary service steps | then("read", catalog::read) | [Composition](docs/0.3.0/en.md#2-compose-typed-and-conditional-steps) |
| Conditional same-type work | thenIf("normalize", predicate, step, policy) | [Conditions](docs/0.3.0/en.md#2-compose-typed-and-conditional-steps) |
| Local retry boundary | then("read", operation, StepPolicy.readOnlyRemoteCall()) | [Retry and timeout](docs/0.3.0/en.md#3-retry-only-the-failing-step) |
| Bounded named processing profiles | withProcessing(ProcessingOptions.ordered(4)).process(records) | [Ordered, sequential, completion](docs/0.3.0/en.md#4-limit-concurrency-and-choose-output-order) |
| HTTP with cancellation | HttpSteps.jdk(client, bodyHandler) | [Application-owned HTTP](docs/0.3.0/en.md#5-connect-http-and-propagate-cancellation) |
| Blocking operations on an explicit executor | BlockingSteps.call(executor, (value, context) -> read(value)) | [Java 17 / virtual threads 21+](docs/0.3.0/en.md#6-isolate-blocking-io-use-virtual-threads-when-available) |
| Optional JSON transformation | JoltStep.fromSpec(spec) | [Jolt tree model](docs/0.3.0/en.md#8-transform-json-trees-with-jolt) |
| Context and diagnostics | observe(observer), run(input).contextWrite(context) | [ScopedValue 25+ / JFR 27](docs/0.3.0/en.md#9-observe-executions-and-record-jfr-on-java-27) |

## Learn in application order

[Guide](docs/0.3.0/en.md): choose modules → define a record flow → classify errors →
select workload limits → connect I/O → observe failures. Each chapter explains
what changes in the application, gives code and names the boundary that remains.
[Source walkthrough](docs/0.3.0/en.md#source-walkthrough) follows one subscription.

```text
Your application
  ProductService.java    reusable business pipeline
  CatalogClient.java     cold HTTP read and status classification
  ProductController.java reactive boundary, if using WebFlux
  application.yml        deployment settings, if using the Spring starter
```

## Compatibility and limits

Core/Jolt target Java 17 and support JVM 17/21/25/27. Application-owned virtual
executors are possible on Java 21+; java25 ScopedValue and java27 JFR are separate
optional artifacts. No preview flags. [Module/Java choices](docs/0.3.0/java.en.md).

Concurrency bounds active records, not bytes or total application traffic. INPUT
orders output, not external effects. Error cancels remaining work, but cannot
undo completed effects or already emitted output. Retry is not a transaction or
exactly-once guarantee; persistence, idempotency and restart recovery belong to
the application. Jolt materializes JSON trees. [Failure contracts](docs/ARCHITECTURE.md).

[MIT](LICENSE) · [Dependency notices](THIRD-PARTY-NOTICES.md)

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

**Версия:** [0.3.0](docs/0.3.0/ru.md) · [Все версии](docs/index.md) · [Справочник API](docs/0.3.0/reference.ru.md)

Обогащает и преобразует записи через именованные типизированные шаги Reactor.
У каждого шага своя граница отказа: временный сбой каталога позволяет повторить
его чтение, не выполняя успешную валидацию заново. Одно определение обслуживает
запись или поток с ограниченной параллельностью и выбранным порядком результатов.

Подходит для импорта, объединения API и сервисов преобразования с отдельными
политиками повторов/таймаутов. Для короткой цепочки без таких границ достаточно
операторов Reactor. Библиотека не хранит задания и не планирует их запуск.

## Подключение в существующий Java-проект

Java 17+ и реактивный вызывающий код. Maven:

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

Используйте репозиторий артефактов приложения. Выпуск в Maven Central пока не
заявлен; [локальная проверка исходников](CONTRIBUTING.md) описана отдельно от
интеграции. У core нет зависимости от Spring. Для существующего приложения
**Spring Boot 4.1.2 WebFlux + Reactor Netty** есть
[полный пример controller/service/configuration в стартере](https://github.com/dlorian59/reactive-pipeline-spring-boot-starter/blob/main/docs/0.1.0/ru.md).
В нём указаны зависимости WebFlux и WebClient; движок остаётся тем же.

## Разместите цепочку в сервисе

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

Создайте ProductService с обычным методом каталога: например,
new ProductService(catalog::readCategory). Метод возвращает холодный Mono<String>
и сам классифицирует внешние ошибки. Не нужны специальная реализация порта,
параметр контекста, реестр с рефлексией или callback фреймворка.

Верните service.enrich(records) из реактивного обработчика. Результат — ProductView
во входном порядке, максимум четыре активные подписки на записи. Определение
неизменяемо; у каждой подписки свои попытки и execution ID. Полный
[ProductService.java](docs/0.3.0/examples/ProductService.java) содержит исполняемую
проверку, а [HTTP-пример](docs/0.3.0/examples/ProductEnrichmentExample.java)
проверяет настоящий ответ 503 и последующий успешный повтор.

Профиль readOnlyRemoteCall даёт две дополнительные попытки по 3 секунды, задержку
100ms и допускает только прямые ConnectException, SocketTimeoutException, HttpTimeoutException или TimeoutException. Произвольные
HTTP-статусы, ошибки авторизации и валидации не повторяются. Обёрнутые транспортные
ошибки требуют классификации либо readOnlyRemoteCall(predicate). У обычных then
и map — 30 секунд без повторов. Безопасность повторного вызова определяет бизнес-код.

## Возможности с примерами

| Возможность | Короткий пример | Подробнее |
| --- | --- | --- |
| Типизированные методы сервиса | then("read", catalog::read) | [Композиция](docs/0.3.0/ru.md#2-соберите-типизированные-и-условные-шаги) |
| Условный шаг того же типа | thenIf("normalize", predicate, step, policy) | [Условия](docs/0.3.0/ru.md#2-соберите-типизированные-и-условные-шаги) |
| Локальная граница повторов | then("read", operation, StepPolicy.readOnlyRemoteCall()) | [Повторы и таймаут](docs/0.3.0/ru.md#3-повторяйте-только-упавший-шаг) |
| Именованные профили обработки | withProcessing(ProcessingOptions.ordered(4)).process(records) | [Порядок и лимиты](docs/0.3.0/ru.md#4-ограничьте-параллельность-и-выберите-порядок) |
| HTTP с отменой | HttpSteps.jdk(client, bodyHandler) | [Клиент приложения](docs/0.3.0/ru.md#5-подключите-http-и-передавайте-отмену) |
| Блокирующий вызов с явным executor | BlockingSteps.call(executor, (value, context) -> read(value)) | [Java 17 / виртуальные потоки 21+](docs/0.3.0/ru.md#6-изолируйте-блокирующий-ввод-вывод-и-используйте-виртуальные-потоки) |
| Необязательное преобразование JSON | JoltStep.fromSpec(spec) | [Дерево Jolt](docs/0.3.0/ru.md#8-преобразуйте-json-деревья-через-jolt) |
| Контекст и диагностика | observe(observer), run(input).contextWrite(context) | [ScopedValue 25+ / JFR 27](docs/0.3.0/ru.md#9-наблюдайте-выполнение-и-записывайте-jfr-на-java-27) |

## Изучение по порядку интеграции

[Руководство](docs/0.3.0/ru.md): выбор модулей → цепочка для записи → классификация
ошибок → лимиты нагрузки → подключение I/O → наблюдение отказов. Каждая глава
объясняет изменение в приложении, даёт код и обозначает оставшуюся границу.
[Путь по исходникам](docs/0.3.0/ru.md#source-walkthrough) следует одной подписке.

```text
Приложение
  ProductService.java    переиспользуемая бизнес-цепочка
  CatalogClient.java     холодное HTTP-чтение и классификация статусов
  ProductController.java реактивная граница при использовании WebFlux
  application.yml        настройки развёртывания при использовании стартера
```

## Совместимость и ограничения

Core/Jolt собраны для Java 17, поддерживают JVM 17/21/25/27. На Java 21+ доступны
виртуальные потоки через executor приложения. java25 ScopedValue и java27 JFR —
отдельные необязательные артефакты. Preview не нужен. [Выбор модулей/JVM](docs/0.3.0/java.ru.md).

Параллельность ограничивает записи, а не байты и не общую нагрузку приложения.
INPUT упорядочивает выдачу, но не внешние эффекты. Ошибка отменяет оставшуюся
работу, но не выполненные эффекты или выданные данные. Повтор не является
транзакцией или гарантией exactly-once: хранение, идемпотентность и восстановление
после перезапуска остаются в приложении. Jolt материализует дерево JSON.
[Контракты отказов](docs/ARCHITECTURE.md).

[MIT](LICENSE) · [Лицензии зависимостей](THIRD-PARTY-NOTICES.md)

</details>
