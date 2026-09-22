package darkstore.reactive;

import darkstore.reactive.pipeline.FlowableBackpressureDemo;
import darkstore.reactive.pipeline.LiveSimulationDemo;
import darkstore.reactive.pipeline.ObservableDemo;

/**
 * Точка входа: реактивная обработка данных АС Поставщиков (darkstore) на RxJava 3.
 * Последовательно демонстрирует:
 *  1) Observable: filter/map/flatMap + обработка ошибок + groupBy/reduce агрегация + Schedulers;
 *  2) Observable: имитация поступления новых данных во времени (PublishSubject + buffer);
 *  3) Flowable: работа при превышении скорости источника над скоростью обработчика (backpressure).
 */
public final class Main {

    public static void main(String[] args) throws InterruptedException {
        ObservableDemo.run();
        LiveSimulationDemo.run();
        FlowableBackpressureDemo.run();
        System.out.println("\nГотово.");
    }
}
