package darkstore.reactive.pipeline;

import darkstore.reactive.data.SampleDataSource;
import darkstore.reactive.model.ProcessedItem;
import darkstore.reactive.model.SupplierSummary;
import darkstore.reactive.model.SupplyRecord;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Демонстрация базовой реактивной цепочки на Observable:
 * источник -> filter -> map/flatMap (с обработкой ошибок) -> groupBy + reduce -> результат.
 * Часть операций выполняется в Schedulers.io()/Schedulers.computation(), что видно по
 * именам потоков в логе.
 */
public final class ObservableDemo {

    private ObservableDemo() {
    }

    public static void run() throws InterruptedException {
        System.out.println("\n=== Observable: обработка исходного набора поставок ===");
        List<SupplyRecord> source = SampleDataSource.initialBatch();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.fromIterable(source)
                .subscribeOn(Schedulers.io())
                .doOnNext(r -> log("источник", r.toString()))
                // Исключаем товары, которых нет в наличии
                .filter(r -> r.getQuantity() > 0)
                // flatMap с точечной обработкой ошибок: некорректная запись не обрывает поток,
                // а просто пропускается (Observable.empty()) после логирования.
                .flatMap(record -> validateAndTransform(record)
                        .onErrorResumeNext(error -> {
                            log("ошибка", "запись отброшена: " + error.getMessage());
                            return Observable.empty();
                        }))
                .observeOn(Schedulers.computation())
                .doOnNext(item -> log("обработано", item.toString()))
                .groupBy(ProcessedItem::getSupplierId)
                .flatMapSingle(group -> group.reduce(
                        new SupplierSummary(group.getKey(), "", 0.0, 0),
                        (acc, item) -> new SupplierSummary(
                                item.getSupplierId(),
                                item.getSupplierName(),
                                acc.getTotalValue() + item.getTotalCost(),
                                acc.getItemCount() + 1)))
                .toList()
                .subscribe(
                        summaries -> {
                            System.out.println("--- Итоги по поставщикам ---");
                            summaries.forEach(s -> log("итог", s.toString()));
                            latch.countDown();
                        },
                        error -> {
                            System.out.println("Необработанная ошибка цепочки: " + error);
                            latch.countDown();
                        });

        latch.await(5, TimeUnit.SECONDS);
    }

    /** Преобразование и валидация одной записи; при некорректных данных бросает исключение. */
    private static Observable<ProcessedItem> validateAndTransform(SupplyRecord record) {
        return Observable.fromCallable(() -> {
            if (record.getProduct() == null || record.getProduct().isBlank()) {
                throw new IllegalArgumentException("пустое название товара у поставщика " + record.getSupplierId());
            }
            if (record.getPrice() < 0) {
                throw new IllegalArgumentException("отрицательная цена товара '" + record.getProduct() + "'");
            }
            double totalCost = record.getPrice() * record.getQuantity();
            return new ProcessedItem(record.getSupplierId(), record.getSupplierName(), record.getProduct(), totalCost);
        });
    }

    private static void log(String stage, String message) {
        System.out.printf("[%s][%s] %s%n", Thread.currentThread().getName(), stage, message);
    }
}
