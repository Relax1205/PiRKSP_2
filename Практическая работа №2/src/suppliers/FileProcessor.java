package suppliers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * Обработка файла: чтение -> разбор -> SHA-256 -> перемещение в processed (или failed).
 */
public class FileProcessor {
    private final Path processedDir;
    private final Path failedDir;
    private final Path reportFile;
    /** Первый файл читается через FileChannel + ByteBuffer, остальные - Files.readAllLines. */
    private boolean channelDemoUsed = false;

    public FileProcessor(Path processedDir, Path failedDir, Path reportFile) {
        this.processedDir = processedDir;
        this.failedDir = failedDir;
        this.reportFile = reportFile;
    }

    public synchronized void process(Path file) {
        if (!Files.exists(file)) {
            return; // файл уже обработан (попал и в начальный скан, и в событие)
        }
        ProcessingResult r = new ProcessingResult(file.getFileName().toString());
        try {
            waitUntilReady(file);
            r.size = Files.size(file);
            r.sha256 = Checksum.sha256(file);

            List<String> lines;
            if (!channelDemoUsed) {
                channelDemoUsed = true;
                r.readMethod = "FileChannel + ByteBuffer";
                lines = Arrays.asList(readWithChannel(file).split("\\R"));
            } else {
                r.readMethod = "Files.readAllLines";
                lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            }
            analyse(lines, r);

            if (r.validRows == 0) {
                r.status = "failed (нет корректных строк), перемещён в failed/";
                moveTo(file, failedDir);
            } else {
                r.status = r.invalidRows == 0 ? "processed" : "processed with errors";
                moveTo(file, processedDir);
            }
        } catch (NoSuchFileException e) {
            return;
        } catch (IOException e) {
            r.status = "error: " + e.getMessage();
        }
        String text = r.format();
        System.out.println(text);
        System.out.println();
        writeReport(text);
    }

    /** Чтение файла целиком через FileChannel и ByteBuffer. */
    static String readWithChannel(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(1024);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (ch.read(buf) != -1) {
                buf.flip();
                out.write(buf.array(), 0, buf.limit());
                buf.clear();
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private void analyse(List<String> lines, ProcessingResult r) {
        int lineNo = 0;
        for (String raw : lines) {
            lineNo++;
            String line = raw;
            if (lineNo == 1 && line.startsWith("﻿")) line = line.substring(1); // BOM
            if (line.trim().isEmpty() || line.startsWith("#")) continue;
            if (line.trim().toLowerCase().startsWith("supplierid")) continue; // заголовок
            try {
                SupplierRecord rec = SupplierRecord.parse(line);
                r.validRows++;
                BigDecimal total = rec.getTotal();
                r.totalValue = r.totalValue.add(total);
                String key = rec.getSupplierId() + " " + rec.getSupplierName();
                BigDecimal prev = r.totalBySupplier.get(key);
                r.totalBySupplier.put(key, prev == null ? total : prev.add(total));
            } catch (IllegalArgumentException e) {
                r.invalidRows++;
                r.errors.add("строка " + lineNo + ": " + e.getMessage());
            }
        }
    }

    /** Ждём, пока файл допишут и освободят (копирование в каталог, который слушает WatchService, не атомарно). */
    private void waitUntilReady(Path file) throws IOException {
        long last = -1;
        for (int i = 0; i < 50; i++) {
            long size = Files.size(file);
            boolean readable;
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
                readable = true;
            } catch (IOException e) {
                readable = false;
            }
            if (readable && size == last) return;
            last = size;
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void moveTo(Path file, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(file.getFileName());
        if (Files.exists(target)) { // не затираем ранее обработанный файл с тем же именем
            String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            target = targetDir.resolve(stamp + "_" + file.getFileName());
        }
        Files.move(file, target);
    }

    private void writeReport(String text) {
        try {
            Files.write(reportFile, (LocalDateTime.now() + "\n" + text + "\n\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Не удалось записать отчёт: " + e.getMessage());
        }
    }
}
