package task3;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

final class Log {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Log() {
    }

    static void info(String message) {
        System.out.printf("%s [%-14s] %s%n", LocalTime.now().format(FORMAT), Thread.currentThread().getName(), message);
    }
}
