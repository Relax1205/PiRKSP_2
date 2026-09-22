package darkstore.reactive.pipeline;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Демонстрация ситуации, когда источник Flowable формирует элементы быстрее,
 * чем их успевает обрабатывать подписчик, и способов управления скоростью передачи данных.
 */
public final class FlowableBackpressureDemo {

    private FlowableBackpressureDemo() {
    }

    public static void run() throws InterruptedException {
        System.out.println("\n=== Flowable: источник быстрее обработчика (backpressure) ===");
        withoutBackpressureHandling();
        withOnBackpressureDrop();
    }

    /** Без явного управления скоростью — при переполнении внутреннего буфера возникает
     *  MissingBackpressureException, которую мы перехватываем и не даём приложению упасть. */
    private static void withoutBackpressureHandling() throws InterruptedException {
        System.out.println("--- Без управления backpressure (ожидаем MissingBackpressureException) ---");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();

        Flowable.<Integer>create(emitter -> {
            for (int i = 1; i <= 5000 && !emitter.isCancelled(); i++) {
                emitter.onNext(i); // источник эмитит мгновенно, без учёта запросов подписчика
            }
            emitter.onComplete();
        }, BackpressureStrategy.ERROR)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .doOnNext(i -> slowConsume())
                .subscribe(
                        i -> processed.incrementAndGet(),
                        error -> {
                            log("ошибка", "поток остановлен корректно: " + error);
                            System.out.println("Обработано элементов до сбоя: " + processed.get());
                            latch.countDown();
                        },
                        () -> {
                            System.out.println("Обработка завершена без ошибок, элементов: " + processed.get());
                            latch.countDown();
                        });

        latch.await(10, TimeUnit.SECONDS);
    }

    /** С управлением скоростью: onBackpressureDrop() отбрасывает элементы, которые
     *  подписчик не успевает запросить, вместо падения с исключением. */
    private static void withOnBackpressureDrop() throws InterruptedException {
        System.out.println("--- С onBackpressureDrop() (лишние элементы отбрасываются) ---");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        AtomicInteger dropped = new AtomicInteger();

        Flowable.<Integer>create(emitter -> {
            for (int i = 1; i <= 5000 && !emitter.isCancelled(); i++) {
                emitter.onNext(i);
            }
            emitter.onComplete();
        }, BackpressureStrategy.MISSING)
                .onBackpressureDrop(i -> dropped.incrementAndGet())
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .doOnNext(i -> slowConsume())
                .subscribe(
                        i -> processed.incrementAndGet(),
                        error -> {
                            log("ошибка", error.toString());
                            latch.countDown();
                        },
                        () -> {
                            System.out.println("Обработано: " + processed.get() + ", отброшено: " + dropped.get());
                            latch.countDown();
                        });

        latch.await(10, TimeUnit.SECONDS);
    }

    private static void slowConsume() {
        try {
            Thread.sleep(1); // подписчик заведомо медленнее источника
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void log(String stage, String message) {
        System.out.printf("[%s][%s] %s%n", Thread.currentThread().getName(), stage, message);
    }
}
