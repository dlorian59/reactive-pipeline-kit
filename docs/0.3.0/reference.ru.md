# 0.3.0 — полный справочник API

[EN](reference.en.md) · [RU](reference.ru.md) · [Практическое руководство](ru.md) · [Версии](../index.md) · [README](../../README.md)

Ниже перечислены все публичные типы библиотеки и их операции. Примеры — отдельные
приложения, использующие JAR из Maven Local, а не псевдокод.
[Команды запуска](examples/README.md#русский); каждая программа проверяет результат.
Базовая версия core — Java 17. [Выбор Java и модулей](java.ru.md) отдельно описывает
executor Java 21 и дополнительные модули Java 25/27.

## Типизированные шаги и выполнение

[Исходник Pipeline](../../core/src/main/java/io/github/alex59/pipeline/Pipeline.java) ·
[Исходник Step](../../core/src/main/java/io/github/alex59/pipeline/Step.java)

| API | Контракт | Запускаемый пример |
| --- | --- | --- |
| Pipeline.start<T>() | Неизменяемое определение с тождественным преобразованием; подписки нет | [Example.java: typed](examples/Example.java) |
| then(name, Step), then(name, Step, StepPolicy) | Новый асинхронный типизированный шаг с обычной или явной политикой; возвращает новое определение | [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) |
| then(name, Function<T,Mono<R>>[, policy]) | Обычный реактивный метод/lambda без ExecutionContext; прежняя ленивая граница шага/повторов | [ProductService](examples/ProductService.java) |
| withProcessing(ProcessingOptions), process(Publisher) | Неизменяемые настройки определения; process применяет их; по умолчанию четыре записи во входном порядке | [ProductService](examples/ProductService.java) |
| map(name, Function) | Синхронное неблокирующее преобразование со стандартной политикой; нужен результат | [Example.java: typed](examples/Example.java) |
| thenIf(name, Predicate, Step, StepPolicy) | Преобразование с сохранением типа; false пропускает вход; условие вычисляется один раз вне повторов | [Example.java: typed](examples/Example.java) |
| observe(StepObserver) | Новое определение с заменой наблюдателя для всех шагов; прежнее определение не меняется | [Example.java: diagnostics](examples/Example.java) |
| run(input) | Холодный Mono; подписка создаёт ID выполнения и счётчики попыток; ссылка на вход сохраняется | [Example.java: retry](examples/Example.java) |
| process(Publisher, concurrency, Ordering) | Холодный Flux с положительным лимитом активных записей на подписку | [Example.java: ordering](examples/Example.java) |
| Step.apply(input, ExecutionContext) | Новый вызов на попытку; холодный Mono с ровно одним ненулевым значением | [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) |

Имена шагов непустые, не состоят из пробелов и уникальны внутри определения.
Null-аргументы отклоняются. Пустой результат шага и null вместо Mono — ошибки;
Mono<Void> не является результатом шага core. Null из map превращается в пустой
Mono и тоже приводит к ошибке. Библиотека не устанавливает scheduler: блокировка
в callback блокирует вызывающий поток и может задержать таймаут/отмену. Объекты
приложения не копируются и не становятся потокобезопасными. Автоматических
подписок и сохранения состояния выполнения нет.

```java
var lengths = Pipeline.<String>start().map("trim", String::trim)
    .map("length", String::length)
    .process(Flux.just(" java ", " ts "), 2, Ordering.INPUT)
    .collectList().block();
// [4, 2]
```

Импорты: io.github.alex59.pipeline.Pipeline, io.github.alex59.pipeline.Ordering,
reactor.core.publisher.Flux. Это тело метода синхронного примера; на асинхронной
границе приложения возвращайте publisher вместо block. Полный сценарий
валидация → HTTP → модель приведён в
[ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java).

## Политика попыток, ошибки и порядок

| API | Контракт | Исходник и пример |
| --- | --- | --- |
| StepPolicy(timeout, maxRetries, retryDelay, retryOn) и accessors | Положительный таймаут попытки, неотрицательные число дополнительных попыток и фиксированная задержка, ненулевой predicate | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java), [Example.java: retry/timeout](examples/Example.java) |
| StepPolicy.defaults(), once(Duration) | Defaults: 30 секунд без повторов; once задаёт таймаут без повторов | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java), [Example.java: timeout](examples/Example.java) |
| StepPolicy.readOnlyRemoteCall(), readOnlyRemoteCall(Predicate) | Три секунды на попытку, два повтора, задержка 100ms; выбранные ошибки соединения/таймаута либо явный классификатор; только безопасное чтение | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepPolicy.java) |
| ProcessingOptions(concurrency, ordering), concurrency(), ordering() | Неизменяемый положительный бюджет записей и ненулевой порядок | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/ProcessingOptions.java) |
| ProcessingOptions.ordered(), ordered(n), sequential(), completionOrder(n) | По умолчанию четыре записи во входном порядке; явное n; одна запись с порядком вызовов; либо n записей по готовности | [ProductService](examples/ProductService.java) |
| StepFailure.step(), attempts(), getCause() | Имя шага, число начатых попыток и последняя исходная причина; объект создаёт пайплайн | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepFailure.java), [Example.java: failure](examples/Example.java) |
| Ordering.INPUT | Выдача в порядке входа; готовые поздние записи могут ждать раннюю | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/Ordering.java), [Example.java: ordering](examples/Example.java) |
| Ordering.COMPLETION | Выдача по завершению; порядок зависит от времени выполнения | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/Ordering.java), [Example.java: ordering](examples/Example.java) |

Таймаут не включает задержку между повторами. Retry вызывает только упавший шаг с
тем же входом и контекстом. Ошибка условия создаёт StepFailure с attempts=0 и не
повторяется; ошибка преобразования сообщает минимум одну попытку. Первая ошибка
process отменяет источник и остальные активные записи; уже выданные результаты
и внешние эффекты не откатываются. Лимит считает записи, не байты, потоки JVM или
глобальные запросы. Prefetch каждой внутренней подписки равен 1; размер записи
может оставаться большим. INPUT не упорядочивает внешние эффекты.

[Пример retry](examples/Example.java): result=10, предыдущий шаг выполнен один
раз, attempts=3. [Бизнес-пример](examples/ProductEnrichmentExample.java):
validationRuns=2, httpRequests=3. Классифицируйте HTTP-ошибки внутри повторяемого
шага. Выбирайте одного владельца retry для I/O: политика шага вместе с
Spring @ReactivePolicy или другим retry-оператором умножает число попыток. Отмена
не откатывает удалённую запись; повторы требуют идемпотентности/воспроизводимости.

## Контекст выполнения и наблюдение

| API | Контракт | Исходник и пример |
| --- | --- | --- |
| ExecutionContext(UUID, ContextView), executionId(), subscriberContext() | Ненулевые метаданные; единый ID шагов/повторов запуска; контекст захватывается при подписке | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/ExecutionContext.java), [Example.java: diagnostics/blocking](examples/Example.java) |
| StepObserver.onEvent(StepEvent), NONE | Синхронный неблокирующий потокобезопасный наблюдатель либо отсутствие действий | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepObserver.java), [Example.java: diagnostics](examples/Example.java) |
| Конструктор/accessors StepEvent | executionId, step, attempt, kind, elapsed, error | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepEvent.java), [Example.java: diagnostics](examples/Example.java) |
| StepEvent.Kind | STARTED, SUCCEEDED, FAILED, CANCELLED, SKIPPED | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/StepEvent.java), [Example.java: diagnostics/typed](examples/Example.java) |

Попытки нумеруются с 1; пропуск/ошибка условия использует 0. Elapsed — длительность
попытки по монотонным часам; STARTED/SKIPPED и ошибка predicate используют ноль.
FAILED содержит исходный Throwable, другие события — без ошибки. Diagnostics
проверяет [STARTED, SUCCEEDED] с одним ID; новая подписка получает новый ID.
Значения Reactor context принадлежат приложению; переноса ThreadLocal нет.
StepEvent — запись данных, не валидатор созданных вручную событий.

Нефатальные ошибки наблюдателя отправляются в Reactor onErrorDropped и не
повторяют бизнес-операции. Нефатальная ошибка самого hook отбрасывается без
рекурсивного вызова; Reactor-fatal ошибки пробрасываются. Полезная нагрузка
исключена, но объект исключения и выбранные приложением имена шагов могут
содержать чувствительные данные.

## HTTP и блокирующие операции

| API | Контракт | Исходник и пример |
| --- | --- | --- |
| HttpPort.exchange(request, context), asStep() | Прикладная HTTP-граница; холодный Mono ответа; прямое преобразование в Step | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/HttpPort.java), [ProductEnrichmentExample.java](examples/ProductEnrichmentExample.java) |
| HttpSteps.jdk(HttpClient, BodyHandler) | Асинхронный send, отмена достигает CompletableFuture; клиентом и body policy владеет приложение | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/HttpSteps.java), [Example.java: http](examples/Example.java) |
| BlockingSteps.call(ExecutorService, Operation), Operation.apply(input, context) | Новая задача executor на попытку, явный контекст; checked exceptions становятся ошибками | [Исходник](../../core/src/main/java/io/github/alex59/pipeline/BlockingSteps.java), [Example.java: blocking](examples/Example.java) |

HTTP-статус — данные: пример выводит 503 и тело "try later". Превращайте нужные
статусы в ошибки внутри HTTP-шага, как в бизнес-примере. BodyHandler выбирает
буферизацию; ofString хранит тело целиком. Отмена не доказывает, что сервер не
обработал запрос. Здесь нет аутентификации, правил адресов, лимита размера ответа
и менеджера распределённых транзакций.

Пример blocking выводит blocking=team:java и закрывает executor. Отмена запрашивает
прерывание, включая гонку передачи задачи executor. Код, игнорирующий прерывание,
может пережить отмену и выполняться одновременно с повтором; отказ executor
превращается в ошибку. Не передавайте executor, выполняющий задачу прямо в
вызывающем потоке. Virtual threads Java 21+ работают через тот же core-адаптер:
модуль Java 25 не нужен, CPU-вычисления быстрее не становятся. Закрытие executor
и потокобезопасность остаются обязанностью приложения.

## Дополнительные преобразования Jolt — Java 17+

[Исходник JoltStep](../../jolt/src/main/java/io/github/alex59/pipeline/jolt/JoltStep.java) ·
[Запускаемый JoltExample.java](examples/JoltExample.java)

JoltStep.fromSpec(List) копирует и проверяет спецификацию. apply(input, context)
возвращает холодный Mono; каждая попытка копирует дерево и создаёт свой Chainr.
Разрешены shift, default, remove, cardinality и sort; произвольные transform-классы
и скрипты не загружаются. Пример переносит id в record.key, выводит
jolt={record={key=7}} и проверяет inputUnchanged=true.

Разрешены Map со строковыми ключами, List, null, String, Boolean, Byte, Short,
Integer, Long, точные классы BigInteger/BigDecimal и конечные Float/Double.
Другие объекты/числа, нестроковые ключи, циклы/глубина свыше 128 отклоняются.
Дерево материализуется целиком: это не потоковый JSON-парсер. CPU-работа идёт в
потоке подписки, если приложение не выбрало scheduler. Null в корне результата
завершает Mono пустым, и core считает это ошибкой шага. Jolt не использует контекст.
Компиляция спецификации на каждую попытку обменивает затраты на изоляцию состояния.

```sh
./gradlew publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=JoltExample
```

Артефакт: io.github.59alex:reactive-pipeline-kit-jolt:0.3.0. Публикация локальная,
наличие релиза в Maven Central не заявляется.

## Дополнительный scoped-контекст — Java 25+

[Исходник ScopedBlockingSteps](../../java25/src/main/java/io/github/alex59/pipeline/java25/ScopedBlockingSteps.java) ·
[Запускаемый ScopedExample.java](examples/ScopedExample.java)

ScopedBlockingSteps.call(ExecutorService, Operation) связывает ExecutionContext
только на время синхронного Operation.apply(input). Вне области currentContext()
возвращает Optional.empty. Задачи и отмена используют core BlockingSteps.
Связывание завершается до уведомления downstream, в том числе при ошибке; оно не
переходит автоматически в произвольные async-задачи и Reactor scheduler.
Пример использует virtual threads и выводит scoped=team:java; outsideBound=false.

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=ScopedExample
```

Артефакт: io.github.59alex:reactive-pipeline-kit-java25:0.3.0. Нужен установленный
JDK 25+; core остаётся совместимым с Java 17. Preview-функций нет.

## Дополнительный наблюдатель JFR — Java 27

[Исходник JfrStepObserver](../../java27/src/main/java/io/github/alex59/pipeline/java27/JfrStepObserver.java) ·
[Запускаемый JfrExample.java](examples/JfrExample.java) ·
[Окружение профилирования](../../java27/src/examples/java/io/github/alex59/pipeline/java27/ProfileExample.java)

new JfrStepObserver() создаёт StepObserver без собственного изменяемого состояния.
EVENT_NAME равен io.github.alex59.pipeline.Step: включите это событие в Recording
приложения. onEvent(StepEvent) создаёт отдельное событие, если его следует записать.
Поля: executionId, step, kind, attempt, elapsedNanos и errorType. Payload и сообщения
исключений не копируются; наблюдатель не управляет Recording и параметрами JVM.
Используйте статические нечувствительные имена шагов: startup redaction не очищает
поля пользовательских событий. [JFR и redaction](../JFR-JAVA27.md).

```sh
./gradlew -Pjava27 -PtestJavaVersion=27 publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=JfrExample
./gradlew -Pjava27 -PtestJavaVersion=27 :java27:profileJava27
```

Артефакт: io.github.59alex:reactive-pipeline-kit-java27:0.3.0. Java 27 — отдельная
минимальная версия дополнительного модуля, не LTS. Пример проверяет два записанных
события; состав полей показан в исходнике и проверяется тестами адаптера. Задача
профилирования создаёт синтетическую запись, а не замер production-производительности.
Нужен установленный JDK 27. При этом обновлении документации матрица дополнительных
модулей повторно не запускалась.

## Spring и выбор границы

Отдельный [Spring starter](https://github.com/dlorian59/reactive-pipeline-spring-boot-starter)
предоставляет PipelineFactory, свойства и подключаемую явно @ReactivePolicy для
Mono/Flux-методов сервисов. Его README ведёт к справочнику версии и
AnnotatedExampleApplication. Клонируйте репозиторий стартера рядом с библиотекой и соберите артефакты
локально; публикация в Maven Central выполняется отдельно. Стартер включает
только core, без Jolt/Java 25/27. В отличие от шага core, AOP допускает пустые
результаты, сохраняет тип ошибки и не создаёт метаданные выполнения/наблюдателя.

Типизированный пайплайн объединяет отдельные этапы; внедрённый service proxy
ограничивает одну явно выбранную операцию. Не помечайте одноразовые потоки и
неповторяемые записи как retry-safe без контракта воспроизведения/идемпотентности.
Ни одна интеграция не предоставляет очередь, восстановление workflow после
перезапуска или exactly-once эффекты.

[← Руководство](ru.md) · [Примеры](examples/README.md) · [Версии](../index.md) · [README](../../README.md)
