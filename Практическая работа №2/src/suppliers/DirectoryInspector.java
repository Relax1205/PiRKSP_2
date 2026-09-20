package suppliers;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Работа с каталогом: создание, листинг, поиск по расширению. */
public final class DirectoryInspector {
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DirectoryInspector() { }

    /** Проверяет существование каталога и при необходимости создаёт его. */
    public static void ensureDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            System.out.println("Каталог существует: " + dir.toAbsolutePath());
        } else {
            Files.createDirectories(dir);
            System.out.println("Каталог создан: " + dir.toAbsolutePath());
        }
    }

    /** Выводит имя, размер и время последнего изменения каждого файла каталога. */
    public static void printListing(Path dir) throws IOException {
        System.out.println("Содержимое " + dir + ":");
        List<Path> files = listFiles(dir);
        if (files.isEmpty()) {
            System.out.println("  (файлов нет)");
        }
        for (Path p : files) {
            LocalDateTime t = LocalDateTime.ofInstant(Files.getLastModifiedTime(p).toInstant(), ZoneId.systemDefault());
            System.out.printf("  %-24s %10d bytes   изменён: %s%n", p.getFileName(), Files.size(p), FMT.format(t));
        }
    }

    public static List<Path> listFiles(Path dir) throws IOException {
        List<Path> result = new ArrayList<Path>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (Files.isRegularFile(p)) result.add(p);
            }
        }
        Collections.sort(result);
        return result;
    }

    /** Поиск файлов с заданным расширением (например, ".csv"), без учёта регистра. */
    public static List<Path> findByExtension(Path dir, String extension) throws IOException {
        List<Path> result = new ArrayList<Path>();
        for (Path p : listFiles(dir)) {
            if (hasExtension(p, extension)) result.add(p);
        }
        return result;
    }

    public static boolean hasExtension(Path p, String extension) {
        return p.getFileName().toString().toLowerCase().endsWith(extension.toLowerCase());
    }
}
