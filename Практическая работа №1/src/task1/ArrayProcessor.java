package task1;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * Три реализации одной и той же операции над массивом:
 * последовательная, на потоках Thread и на ForkJoinPool.
 * Обработка каждого элемента сопровождается задержкой {@link #DELAY_MS} мс.
 */
public final class ArrayProcessor {

    public static final long DELAY_MS = 1;

    private ArrayProcessor() {
    }

    /** Последовательная обработка отрезка [from, to). Используется всеми реализациями. */
    static long processRange(int[] array, int from, int to, Operation op) {
        long acc = op.identity();
        for (int i = from; i < to; i++) {
            delay();
            acc = op.combine(acc, array[i]);
        }
        return acc;
    }

    public static long sequential(int[] array, Operation op) {
        return processRange(array, 0, array.length, op);
    }

    /**
     * Массив делится на {@code threadCount} частей, каждая обрабатывается своим потоком.
     * Каждый поток пишет только в свою ячейку results, а чтение происходит после join(),
     * поэтому синхронизация не требуется.
     */
    public static long multithreaded(final int[] array, final Operation op, int threadCount)
            throws InterruptedException {
        final long[] results = new long[threadCount];
        Thread[] workers = new Thread[threadCount];
        int chunk = (array.length + threadCount - 1) / threadCount;

        for (int t = 0; t < threadCount; t++) {
            final int index = t;
            final int from = Math.min(array.length, t * chunk);
            final int to = Math.min(array.length, from + chunk);
            workers[t] = new Thread(new Runnable() {
                @Override
                public void run() {
                    results[index] = processRange(array, from, to, op);
                }
            }, "worker-" + t);
            workers[t].start();
        }

        long acc = op.identity();
        for (int t = 0; t < threadCount; t++) {
            workers[t].join();
            acc = op.combine(acc, results[t]);
        }
        return acc;
    }

    public static long forkJoin(int[] array, Operation op, ForkJoinPool pool) {
        return pool.invoke(new RangeTask(array, 0, array.length, op));
    }

    /** Рекурсивно делит отрезок пополам, пока он не станет меньше порога. */
    static final class RangeTask extends RecursiveTask<Long> {
        private static final long serialVersionUID = 1L;
        private static final int THRESHOLD = 250;

        private final int[] array;
        private final int from;
        private final int to;
        private final Operation op;

        RangeTask(int[] array, int from, int to, Operation op) {
            this.array = array;
            this.from = from;
            this.to = to;
            this.op = op;
        }

        @Override
        protected Long compute() {
            if (to - from <= THRESHOLD) {
                return processRange(array, from, to, op);
            }
            int mid = (from + to) >>> 1;
            RangeTask left = new RangeTask(array, from, mid, op);
            RangeTask right = new RangeTask(array, mid, to, op);
            left.fork();
            long rightResult = right.compute();
            long leftResult = left.join();
            return op.combine(leftResult, rightResult);
        }
    }

    private static void delay() {
        try {
            Thread.sleep(DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Обработка прервана", e);
        }
    }
}
