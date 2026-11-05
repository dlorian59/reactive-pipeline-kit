# Source and artifact verification / Проверка исходников и артефактов

[README](README.md) · [EN guide](docs/0.3.0/en.md) · [Руководство](docs/0.3.0/ru.md)

## English

These commands are for contributors, not dependency instructions for a consuming
application. Use JDK 17+ and the trusted Gradle wrapper. Local publication does not
publish to Maven Central. Do not change package coordinates while testing.

```sh
./gradlew build publishToMavenLocal --no-daemon --max-workers=2
./gradlew -p docs/0.3.0/examples run -Pexample=ProductService
./gradlew -p docs/0.3.0/examples run -Pexample=ProductEnrichmentExample
./gradlew -p docs/0.3.0/examples run -Pexample=Example
```

The independent example build resolves Maven artifacts, not project substitution.
ProductService checks the ordinary-method API; ProductEnrichmentExample starts a
loopback catalogue and asserts the local retry boundary. Example covers the
broader core APIs. Servers/executors created by examples are closed by their owners.

An installed JDK 25 permits `-Pjava25`; an installed JDK 27 permits `-Pjava27`.
Select test execution with `-PtestJavaVersion=17|21|25|27`; the chosen toolchain
must already exist. Do not add optional dependencies to Java 17 consumers.
[Recorded verification](docs/VERIFICATION.md) · [Detailed example runners](docs/0.3.0/examples/README.md) and
[Java compatibility](docs/JAVA-SUPPORT.md) list separate module requirements.

## Русский

Эти команды предназначены для разработчика библиотеки, а не для подключения
зависимости в приложение. Нужны JDK 17+ и доверенный wrapper Gradle. Локальная
публикация не отправляет артефакты в Maven Central. Координаты пакетов при проверке
не меняются. Команды приведены выше.

Отдельный consumer разрешает Maven-артефакты без подмены проекта исходниками.
ProductService проверяет API обычного метода, ProductEnrichmentExample запускает
локальный каталог и проверяет локальную границу повторов. Example охватывает
остальные основные API. Примеры закрывают созданные ими серверы и executor.

Установленная JDK 25 позволяет добавить `-Pjava25`, JDK 27 — `-Pjava27`.
`-PtestJavaVersion=17|21|25|27` выбирает установленную JVM тестов. Не добавляйте
необязательные зависимости в consumer Java 17.
[Запуск примеров](docs/0.3.0/examples/README.md) · [Совместимость Java](docs/JAVA-SUPPORT.md).
