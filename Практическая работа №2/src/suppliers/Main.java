package suppliers;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

/**
 * Приложение обработки файлов обмена системы поставщиков.
 *
 * Аргументы: [базовый_каталог] [--seed] [--demo] [--once]
 *   --seed  скопировать samples/initial/* в incoming
 *   --demo  каждые 5 с подбрасывать в incoming файлы из samples/live (демонстрация WatchService)
 *   --once  обработать имеющиеся файлы и выйти без наблюдения
 */
public class Main {
    private static final String EXT = ".csv";

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8"));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, "UTF-8"));

        List<String> a = Arrays.asList(args);
        Path base = Paths.get("data");
        for (String s : args) {
            if (!s.startsWith("--")) base = Paths.get(s);
        }
        Path incoming = base.resolve("incoming");
        Path processed = base.resolve("processed");
        Path failed = base.resolve("failed");

        System.out.println("=== 1. Рабочие каталоги ===");
        DirectoryInspector.ensureDirectory(base);
        DirectoryInspector.ensureDirectory(incoming);
        DirectoryInspector.ensureDirectory(processed);
        DirectoryInspector.ensureDirectory(failed);

        if (a.contains("--seed")) {
            copyAll(Paths.get("samples", "initial"), incoming);
        }

        System.out.println();
        System.out.println("=== 2. Содержимое incoming ===");
        DirectoryInspector.printListing(incoming);

        FileProcessor processor = new FileProcessor(processed, failed, base.resolve("report.txt"));

        try (IncomingWatcher watcher = new IncomingWatcher(incoming, EXT, processor)) {
            System.out.println();
            System.out.println("=== 3. Поиск " + EXT + " и обработка ===");
            List<Path> found = DirectoryInspector.findByExtension(incoming, EXT);
            System.out.println("Найдено файлов " + EXT + ": " + found.size());
            System.out.println();
            for (Path p : found) processor.process(p);

            if (a.contains("--once")) return;

            System.out.println("=== 4. WatchService ===");
            if (a.contains("--demo")) startDemoFeeder(Paths.get("samples", "live"), incoming);
            watcher.run();
        }
    }

    private static void copyAll(Path from, Path to) throws IOException {
        for (Path p : DirectoryInspector.listFiles(from)) {
            Files.copy(p, to.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
        System.out.println("Тестовые файлы скопированы из " + from + " в " + to);
    }

    private static void startDemoFeeder(final Path from, final Path to) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Path p : DirectoryInspector.listFiles(from)) {
                        Thread.sleep(5000);
                        Files.copy(p, to.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (Exception e) {
                    System.err.println("demo: " + e);
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
