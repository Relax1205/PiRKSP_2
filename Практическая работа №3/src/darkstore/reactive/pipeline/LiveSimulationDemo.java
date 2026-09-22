package darkstore.reactive.pipeline;

import darkstore.reactive.data.SampleDataSource;
import darkstore.reactive.model.ProcessedItem;
import darkstore.reactive.model.SupplyRecord;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Демонстрация имитации поступления новых данных во времени: элементы "приходят" через
 * PublishSubject с задержкой в отдельном потоке, а построенная реактивная цепочка
 * (filter -> map -> buffer-агрегация) автоматически их обрабатывает по мере поступления.
 */
public final class LiveSimulationDemo {

    private LiveSimulationDemo() {
    }

    public static void run() throws InterruptedException {
        System.out.println("\n=== Observable: имитация поступления данных во времени ===");
        PublishSubject<SupplyRecord> incoming = PublishSubject.create();

        Observable<ProcessedItem> processed = incoming
                .observeOn(Schedulers.computation())
                .filter(r -> r.getQuantity() > 0)
                .flatMap(r -> validate(r).onErrorResumeNext(error -> {
                    log("ошибка", "запись отброшена: " + error.getMessage());
                    return Observable.empty();
                }))
                .doOnNext(item -> log("live-обработка", item.toString()));

        // Каждые 2 секунды агрегируем всё, что накопилось, по поставщикам (buffer по времени).
        Disposable aggregation = processed
                .buffer(2, TimeUnit.SECONDS)
                .filter(batch -> !batch.isEmpty())
                .subscribe(batch -> {
                    Map<String, Double> totalsBySupplier = batch.stream()
                            .collect(Collectors.groupingBy(ProcessedItem::getSupplierName,
                                    Collectors.summingDouble(ProcessedItem::getTotalCost)));
                    log("live-агрегация", "за последние 2с: " + totalsBySupplier);
                });

        List<SupplyRecord> live = SampleDataSource.liveBatch();
        Thread producer = new Thread(() -> {
            for (SupplyRecord record : live) {
                try {
                    Thread.sleep(600);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                log("источник(new)", "поступила новая запись: " + record);
                incoming.onNext(record);
            }
            incoming.onComplete();
        }, "producer-thread");
        producer.start();
        producer.join();

        Thread.sleep(2500); // дождаться последнего окна агрегации
        aggregation.dispose();
    }

    private static Observable<ProcessedItem> validate(SupplyRecord record) {
        return Observable.fromCallable(() -> {
            if (record.getProduct() == null || record.getProduct().isBlank()) {
                throw new IllegalArgumentException("пустое название товара у поставщика " + record.getSupplierId());
            }
            if (record.getPrice() < 0) {
                throw new IllegalArgumentException("отрицательная цена товара '" + record.getProduct() + "'");
            }
            return new ProcessedItem(record.getSupplierId(), record.getSupplierName(), record.getProduct(),
                    record.getPrice() * record.getQuantity());
        });
    }

    private static void log(String stage, String message) {
        System.out.printf("[%s][%s] %s%n", Thread.currentThread().getName(), stage, message);
    }
}
