import com.sun.net.httpserver.HttpServer;
import io.github.alex59.pipeline.HttpSteps;
import io.github.alex59.pipeline.ProcessingOptions;
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.StepPolicy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ProductEnrichmentExample {
    record Product(int id, String sku) { }
    record Enriched(Product product, String category) { }
    record ProductView(int id, String label) { }

    public static void main(String[] args) throws Exception {
        // A local catalogue fixture makes the business scenario reproducible without credentials.
        // Локальный каталог позволяет воспроизвести бизнес-сценарий без credentials.
        var calls = new ConcurrentHashMap<Integer, AtomicInteger>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/category/", exchange -> {
            int id = Integer.parseInt(exchange.getRequestURI().getPath().substring("/category/".length()));
            int attempt = calls.computeIfAbsent(id, unused -> new AtomicInteger()).incrementAndGet();
            boolean failOnce = id == 1 && attempt == 1;
            byte[] body = (failOnce ? "try later" : id == 1 ? "books" : "games")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(failOnce ? 503 : 200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        try {
            var endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            var http = HttpSteps.jdk(HttpClient.newHttpClient(), HttpResponse.BodyHandlers.ofString());
            var validations = new AtomicInteger();
            var policy = StepPolicy.readOnlyRemoteCall(IOException.class::isInstance);

            // Validation and output mapping are business code; only HTTP receives the retry policy.
            // Валидация и выходная модель — бизнес-код; политика повторов применяется только к HTTP.
            var pipeline = Pipeline.<Product>start()
                    .withProcessing(ProcessingOptions.ordered(2))
                    .map("validate", product -> {
                        validations.incrementAndGet();
                        if (product.id() < 1 || product.sku() == null || product.sku().isBlank()) {
                            throw new IllegalArgumentException("Product id and SKU are required");
                        }
                        return product;
                    })
                    .then("catalog", (product, context) -> {
                        var request = HttpRequest.newBuilder(endpoint.resolve("/category/" + product.id())).GET().build();
                        // Convert 503 to an error inside this step so its retry repeats the request.
                        // Преобразуем 503 в ошибку внутри шага, чтобы его повтор снова выполнил запрос.
                        return http.exchange(request, context).flatMap(response -> {
                            if (response.statusCode() == 503) return Mono.error(new IOException("Catalogue unavailable"));
                            if (response.statusCode() != 200) return Mono.error(new IllegalStateException("Unexpected catalogue status"));
                            return Mono.just(new Enriched(product, response.body()));
                        });
                    }, policy)
                    .map("view", value -> new ProductView(value.product().id(),
                            value.product().sku() + " / " + value.category()));

            // Blocking belongs only to this CLI boundary; reactive callers return the Flux.
            // Блокировка допустима только на этой границе CLI; реактивный обработчик возвращает Flux.
            var result = pipeline.process(Flux.just(new Product(1, "BK-1"), new Product(2, "GM-2"))).collectList().block(Duration.ofSeconds(10));
            var expected = List.of(new ProductView(1, "BK-1 / books"), new ProductView(2, "GM-2 / games"));
            if (!expected.equals(result) || validations.get() != 2
                    || calls.get(1).get() != 2 || calls.get(2).get() != 1) {
                throw new AssertionError("Product enrichment or retry boundary changed");
            }
            System.out.println("products=" + result);
            System.out.println("validationRuns=2; httpRequests=3; product1Attempts=2");
        } finally {
            // Close the demo-owned server even if an assertion or HTTP call fails.
            // Закрываем сервер демо даже при ошибке проверки или HTTP-вызова.
            server.stop(0);
        }
    }
}
