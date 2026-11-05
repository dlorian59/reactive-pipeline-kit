# OSS-013 verification / Проверка — 2026-11-05

[README](../README.md) · [Contributing](../CONTRIBUTING.md) · [Versions / Версии](index.md)

## English

Java 17 build and local Maven publication passed: 33 core tests and 5 Jolt tests,
no failures/skips. Source/Javadoc artifacts built without new warnings. New focused
coverage exercises the ordinary-method overload, selected read-only retry (without
repeating validation), nonretryable security/TLS failures, and propagation of attached
sequential options through then/thenIf/map/observe. Existing cancellation, ordering,
concurrency, timeout and per-subscription tests remain in the same run.

Independent Maven consumers passed: ProductService returns two ordered views;
ProductEnrichmentExample performs two validations and three actual loopback HTTP
requests (product 1 succeeds on its second attempt). Consumers resolve JARs, not
sibling source/project substitutions. The companion starter also verified its new
real WebFlux HTTP consumer against these artifacts.

The optional java25 adapter also compiled successfully on the installed Java 25 JDK
after the additions (compile check, not a repeat of its runtime tests).

Only API additions/default attachment changed; Jolt, BlockingSteps, ScopedValue and
JFR implementations were unchanged. This update does not claim a repeated runtime
matrix for Java 21/25/27 or new performance results. The installed Java 27 symlink
currently resolves to an absent external directory; no toolchain was downloaded.
Prior module/runtime evidence remains historical in the corresponding Java docs.
Local artifact publication is not Maven Central publication.

## Русский

Сборка Java 17 и публикация в Maven Local успешны: 33 теста core, 5 Jolt, без
пропусков и ошибок. Артефакты исходников/Javadoc собраны без новых предупреждений.
Новые проверки охватывают обычный метод в then, выбранные ошибки профиля чтения
без повторной валидации, отсутствие повторов security/TLS и сохранение sequential
настроек через then/thenIf/map/observe. Прежние проверки отмены, порядка, лимита,
таймаута и изоляции подписок прошли в том же запуске.

Отдельные Maven-consumer прошли: ProductService вернул две упорядоченные модели,
ProductEnrichmentExample сделал две валидации и три настоящих локальных HTTP-запроса
(первый товар успешен со второй попытки). Подмены JAR исходниками соседнего проекта
нет. Стартер проверил новый настоящий WebFlux HTTP-consumer с этими артефактами.

Дополнительный адаптер java25 успешно скомпилирован на установленной Java 25
после изменений (проверка компиляции, не повтор его runtime-тестов).

Изменения добавляют API и закреплённые настройки; реализации Jolt, BlockingSteps,
ScopedValue и JFR не менялись. Повтор полной матрицы Java 21/25/27 и новые замеры
производительности не заявлены. Ссылка на установленную JDK 27 сейчас ведёт в
отсутствующий внешний каталог; toolchain не скачивался. Прежние сведения о JVM
сохранены как история в документации Java. Локальная публикация не является
публикацией в Maven Central.
