import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.ProcessingOptions;
import io.github.alex59.pipeline.StepPolicy;
import java.time.Duration;
import java.util.List;
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

    public static void main(String[] args) {
        // Only this executable check blocks; a reactive controller returns enrich directly.
        // Блокируется только исполняемая проверка; реактивный контроллер возвращает enrich напрямую.
        var service = new ProductService(id -> Mono.just("books"));
        var actual = service.enrich(Flux.just(new Product(1, "BK-1"), new Product(2, "BK-2")))
                .collectList().block(Duration.ofSeconds(2));
        var expected = List.of(new ProductView(1, "BK-1", "books"), new ProductView(2, "BK-2", "books"));
        if (!expected.equals(actual)) throw new AssertionError(actual);
        System.out.println(actual);
    }
}
