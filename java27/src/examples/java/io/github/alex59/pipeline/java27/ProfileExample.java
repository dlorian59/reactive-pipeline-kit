package io.github.alex59.pipeline.java27;

import io.github.alex59.pipeline.Ordering;
import io.github.alex59.pipeline.Pipeline;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import jdk.jfr.Recording;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Runnable Java 27 profiling example using synthetic records.
 * Запускаемый пример профилирования Java 27 на синтетических записях.
 * The application controls the recording and JVM startup redaction options.
 * Приложение управляет записью и параметрами очистки стартовых данных JVM.
 * The observer does not change them; environment-variable events are never enabled here.
 * Наблюдатель их не меняет; события переменных окружения здесь не включаются.
 */
public final class ProfileExample {
    private ProfileExample() { }

    /**
     * Records a small pipeline and startup system properties to a local JFR file.
     * Записывает небольшой пайплайн и стартовые системные свойства в локальный JFR-файл.
     * @param args optional output path, default build/profile-java27.jfr
     *             необязательный путь результата, по умолчанию build/profile-java27.jfr
     * @throws Exception when local recording or file operations fail
     *                   при ошибке локальной записи или файловой операции
     */
    public static void main(String[] args) throws Exception {
        if (args.length > 1) throw new IllegalArgumentException("Expected at most one JFR output path");
        Path destination = Path.of(args.length == 0 ? "build/profile-java27.jfr" : args[0]).toAbsolutePath();
        Files.createDirectories(destination.getParent());
        // The application owns the recording and explicitly excludes environment-variable events.
        // Приложение владеет записью и явно исключает события переменных окружения.
        try (Recording recording = new Recording()) {
            recording.setName("reactive-pipeline-java27-example");
            recording.enable(JfrStepObserver.EVENT_NAME).withThreshold(Duration.ZERO).withoutStackTrace();
            recording.enable("jdk.InitialSystemProperty");
            recording.disable("jdk.InitialEnvironmentVariable");
            recording.start();

            // Use synthetic records; custom JFR fields are not sanitized by startup redaction.
            // Используем синтетические записи; startup redaction не очищает пользовательские поля JFR.
            var pipeline = Pipeline.<Integer>start()
                    .map("normalize", input -> input + 1)
                    .then("enrich", (input, context) -> Mono.delay(Duration.ofMillis(1)).thenReturn(input * 2))
                    .observe(new JfrStepObserver());
            var results = pipeline.process(Flux.range(0, 100), 8, Ordering.INPUT)
                    .collectList().block(Duration.ofSeconds(10));
            if (results == null || results.size() != 100 || results.get(99) != 200) {
                throw new IllegalStateException("Unexpected example result");
            }

            recording.stop();
            recording.dump(destination);
        }
        System.out.println("Recorded 100 synthetic pipeline executions to " + destination);
    }
}
