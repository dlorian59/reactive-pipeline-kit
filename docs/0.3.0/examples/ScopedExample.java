import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.java25.ScopedBlockingSteps;
import java.util.concurrent.Executors;
import reactor.util.context.Context;

public final class ScopedExample {
    public static void main(String[] args) {
        // The Java 25 scoped binding exists only inside the synchronous operation.
        // Scoped binding Java 25 существует только внутри синхронной операции.
        if (ScopedBlockingSteps.currentContext().isPresent()) throw new AssertionError("unexpected binding");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Virtual threads are application-owned; the adapter only submits each attempt.
            // Виртуальными потоками владеет приложение; адаптер лишь отправляет каждую попытку.
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
