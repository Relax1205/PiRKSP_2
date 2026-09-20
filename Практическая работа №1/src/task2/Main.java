package task2;

import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Задание 2. Асинхронная обработка запросов пользователя с помощью Future.
 * Пользователь вводит число, программа возводит его в квадрат с задержкой 1–5 секунд.
 * Новые числа можно вводить, не дожидаясь результатов предыдущих запросов.
 */
public class Main {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Request> pending = new CopyOnWriteArrayList<>();
        AtomicInteger ids = new AtomicInteger();
        ResultPrinter printer = new ResultPrinter(pending);
        printer.start();

        System.out.println("Вводите числа (каждое будет возведено в квадрат с задержкой 1-5 с).");
        System.out.println("Команды: status - незавершённые запросы, exit - выход.");

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().replace((char) 0xFEFF, ' ').trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.equalsIgnoreCase("exit")) {
                break;
            }
            if (line.equalsIgnoreCase("status")) {
                System.out.println("В обработке: " + pending.size());
                continue;
            }

            final long number;
            try {
                number = Long.parseLong(line);
            } catch (NumberFormatException e) {
                System.out.println("Ошибка: '" + line + "' не является целым числом");
                continue;
            }

            Future<Long> future = executor.submit(new SquareTask(number));
            Request request = new Request(ids.incrementAndGet(), number, future);
            pending.add(request);
            System.out.println("Запрос #" + request.id + " (" + number + ") принят, ждём результат...");
        }

        executor.shutdown();
        if (!pending.isEmpty()) {
            System.out.println("Ожидание завершения запросов: " + pending.size());
        }
        executor.awaitTermination(1, TimeUnit.MINUTES);
        printer.finish();
        System.out.println("Работа завершена.");
    }

    /** Долгая операция, результат которой возвращается через Future. */
    private static final class SquareTask implements Callable<Long> {
        private final long number;

        SquareTask(long number) {
            this.number = number;
        }

        @Override
        public Long call() throws InterruptedException {
            Thread.sleep(1000 + RANDOM.nextInt(4001));
            return Math.multiplyExact(number, number);
        }
    }

    private static final class Request {
        final int id;
        final long number;
        final Future<Long> future;
        final long submittedAt = System.currentTimeMillis();

        Request(int id, long number, Future<Long> future) {
            this.id = id;
            this.number = number;
            this.future = future;
        }
    }

    /**
     * Фоновый поток, который периодически проверяет isDone() у Future
     * и выводит готовые результаты, не блокируя ввод пользователя.
     */
    private static final class ResultPrinter extends Thread {
        private final List<Request> pending;
        private volatile boolean running = true;

        ResultPrinter(List<Request> pending) {
            super("result-printer");
            this.pending = pending;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running || !pending.isEmpty()) {
                for (Request request : pending) {
                    if (request.future.isDone()) {
                        pending.remove(request);
                        print(request);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        private void print(Request request) {
            long elapsed = System.currentTimeMillis() - request.submittedAt;
            try {
                long square = request.future.get();
                System.out.println("Запрос #" + request.id + ": (" + request.number + ")^2 = " + square
                        + " (через " + elapsed + " мс)");
            } catch (ExecutionException e) {
                System.out.println("Запрос #" + request.id + ": ошибка вычисления - " + e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        void finish() throws InterruptedException {
            running = false;
            join();
        }
    }
}
