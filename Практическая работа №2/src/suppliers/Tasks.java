package suppliers;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;

/** Задания 1-3 из методички: чтение .txt, сравнение способов копирования 100 МБ, 16-битная контрольная сумма. */
public final class Tasks {
    private static final long BIG_SIZE = 100L * 1024 * 1024;

    private Tasks() { }

    public static void runAll(Path workDir) throws IOException, InterruptedException {
        Files.createDirectories(workDir);
        task1(workDir);
        System.out.println();
        task2And3(workDir);
    }

    /** Задание 1. */
    private static void task1(Path dir) throws IOException {
        System.out.println("=== Задание 1. Чтение .txt через java.nio ===");
        Path txt = dir.resolve("sample.txt");
        Files.write(txt, Arrays.asList("Поставщик: ООО Вектор", "Товар: Ноутбук Lenovo IdeaPad", "Цена: 54990.00", "Количество: 12"),
                StandardCharsets.UTF_8);
        for (String line : Files.readAllLines(txt, StandardCharsets.UTF_8)) System.out.println(line);
    }

    /** Задания 2 и 3. */
    private static void task2And3(Path dir) throws IOException, InterruptedException {
        System.out.println("=== Задание 2. Копирование файла 100 МБ ===");
        Path src = dir.resolve("big.bin");
        generate(src);
        System.out.println("Исходный файл: " + Files.size(src) / (1024 * 1024) + " МБ");
        System.out.printf("%-28s %10s %18s%n", "Способ", "Время, мс", "Пик кучи, МБ");

        measure("FileInputStream/OutputStream", src, dir.resolve("copy_streams.bin"), new Copier() {
            public void copy(Path from, Path to) throws IOException {
                try (InputStream in = new FileInputStream(from.toFile()); OutputStream out = new FileOutputStream(to.toFile())) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
                }
            }
        });
        measure("FileChannel.transferTo", src, dir.resolve("copy_channel.bin"), new Copier() {
            public void copy(Path from, Path to) throws IOException {
                try (FileChannel in = FileChannel.open(from, StandardOpenOption.READ);
                     FileChannel out = FileChannel.open(to, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                             StandardOpenOption.TRUNCATE_EXISTING)) {
                    long pos = 0, size = in.size();
                    while (pos < size) pos += in.transferTo(pos, size - pos, out);
                }
            }
        });
        measure("Apache Commons IO", src, dir.resolve("copy_commons.bin"), new Copier() {
            public void copy(Path from, Path to) throws IOException {
                FileUtils.copyFile(from.toFile(), to.toFile());
            }
        });
        measure("Files.copy", src, dir.resolve("copy_files.bin"), new Copier() {
            public void copy(Path from, Path to) throws IOException {
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }
        });

        System.out.println();
        System.out.println("=== Задание 3. 16-битная контрольная сумма (ByteBuffer + побитовые операции) ===");
        int original = Checksum.checksum16(src);
        System.out.printf("big.bin: 0x%04X%n", original);
        int copy = Checksum.checksum16(dir.resolve("copy_files.bin"));
        System.out.printf("copy_files.bin: 0x%04X (%s)%n", copy, copy == original ? "совпадает" : "НЕ совпадает");

        for (String n : new String[] {"big.bin", "copy_streams.bin", "copy_channel.bin", "copy_commons.bin", "copy_files.bin"}) {
            Files.deleteIfExists(dir.resolve(n));
        }
    }

    private interface Copier {
        void copy(Path from, Path to) throws IOException;
    }

    private static void measure(String name, Path from, Path to, Copier copier) throws IOException, InterruptedException {
        Files.deleteIfExists(to);
        final Runtime rt = Runtime.getRuntime();
        System.gc();
        Thread.sleep(200);
        final long base = rt.totalMemory() - rt.freeMemory();
        final AtomicLong peak = new AtomicLong(base);
        final boolean[] stop = {false};
        Thread sampler = new Thread(new Runnable() {
            public void run() {
                while (!stop[0]) {
                    long used = rt.totalMemory() - rt.freeMemory();
                    if (used > peak.get()) peak.set(used);
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        sampler.setDaemon(true);
        sampler.start();
        long t0 = System.nanoTime();
        copier.copy(from, to);
        long ms = (System.nanoTime() - t0) / 1000000;
        stop[0] = true;
        sampler.join();
        System.out.printf("%-28s %10d %18.2f%n", name, ms, (peak.get() - base) / (1024.0 * 1024.0));
    }

    private static void generate(Path file) throws IOException {
        byte[] block = new byte[1024 * 1024];
        new Random(42).nextBytes(block);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(0);
            for (long written = 0; written < BIG_SIZE; written += block.length) raf.write(block);
        }
    }
}
