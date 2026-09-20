package task1;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

/**
 * Задание 1. Сравнение последовательной, многопоточной и ForkJoin обработки
 * массива из 10 000 элементов по времени выполнения и расходу памяти.
 *
 * Запуск: java task1.Main [SUM|MAX|MIN] [размер массива]
 */
public class Main {

    public static void main(String[] args) throws Exception {
        final Operation op = args.length > 0 ? Operation.valueOf(args[0].toUpperCase()) : Operation.SUM;
        int size = args.length > 1 ? Integer.parseInt(args[1]) : 10_000;
        final int threads = Runtime.getRuntime().availableProcessors();

        final int[] array = new int[size];
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            array[i] = random.nextInt(1000);
        }

        System.out.println("Операция: " + op.getTitle());
        System.out.println("Размер массива: " + size + ", задержка на элемент: " + ArrayProcessor.DELAY_MS + " мс");
        System.out.println("Доступно ядер: " + threads);
        System.out.println();

        final ForkJoinPool pool = new ForkJoinPool(threads);

        Result sequential = measure("Последовательно", new Callable<Long>() {
            @Override
            public Long call() {
                return ArrayProcessor.sequential(array, op);
            }
        });
        Result multithreaded = measure("Многопоточно (" + threads + " Thread)", new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return ArrayProcessor.multithreaded(array, op, threads);
            }
        });
        Result forkJoin = measure("ForkJoinPool (" + threads + ")", new Callable<Long>() {
            @Override
            public Long call() {
                return ArrayProcessor.forkJoin(array, op, pool);
            }
        });
        pool.shutdown();

        System.out.println();
        System.out.printf("%-28s %15s %12s %14s%n", "Способ", "Результат", "Время, мс", "Память, КБ");
        for (Result r : new Result[]{sequential, multithreaded, forkJoin}) {
            System.out.printf("%-28s %15d %12d %14d%n", r.name, r.value, r.timeMs, r.memoryKb);
        }
        System.out.println();

        boolean consistent = sequential.value == multithreaded.value && sequential.value == forkJoin.value;
        System.out.println("Результаты совпадают: " + (consistent ? "да" : "НЕТ"));
        System.out.printf("Ускорение Thread: %.2fx, ForkJoin: %.2fx%n",
                (double) sequential.timeMs / multithreaded.timeMs,
                (double) sequential.timeMs / forkJoin.timeMs);
    }

    private static Result measure(String name, Callable<Long> action) throws Exception {
        System.gc();
        long memoryBefore = usedMemory();
        long start = System.nanoTime();

        long value = action.call();

        long timeMs = (System.nanoTime() - start) / 1_000_000;
        long memoryKb = Math.max(0, usedMemory() - memoryBefore) / 1024;
        System.out.println(name + ": готово за " + timeMs + " мс");
        return new Result(name, value, timeMs, memoryKb);
    }

    private static long usedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static final class Result {
        final String name;
        final long value;
        final long timeMs;
        final long memoryKb;

        Result(String name, long value, long timeMs, long memoryKb) {
            this.name = name;
            this.value = value;
            this.timeMs = timeMs;
            this.memoryKb = memoryKb;
        }
    }
}
