package suppliers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Задание 4: наблюдение за каталогом (создание, изменение, удаление).
 * Удалённый файл прочитать уже нельзя, поэтому для каждого файла хранится снимок (строки, размер, контрольная сумма),
 * обновляемый при создании и изменении; по нему при удалении выводятся размер и 16-битная сумма.
 */
public class DirectoryMonitor {
    private static final class Snapshot {
        final List<String> lines;
        final long size;
        final int checksum;

        Snapshot(List<String> lines, long size, int checksum) {
            this.lines = lines;
            this.size = size;
            this.checksum = checksum;
        }
    }

    private final Path dir;
    private final Map<Path, Snapshot> snapshots = new HashMap<Path, Snapshot>();

    public DirectoryMonitor(Path dir) {
        this.dir = dir;
    }

    public void run() throws IOException, InterruptedException {
        WatchService service = FileSystems.getDefault().newWatchService();
        try {
            dir.register(service, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
            for (Path p : DirectoryInspector.listFiles(dir)) snapshots.put(p, snapshot(p));
            System.out.println("Наблюдение за " + dir.toAbsolutePath() + " запущено (Ctrl+C для выхода)...");
            while (true) {
                WatchKey key = service.take();
                for (WatchEvent<?> ev : key.pollEvents()) {
                    if (ev.kind() == StandardWatchEventKinds.OVERFLOW) continue;
                    Path file = dir.resolve((Path) ev.context());
                    handle(ev.kind(), file);
                }
                if (!key.reset()) break;
            }
        } catch (ClosedWatchServiceException e) {
            // штатное закрытие
        } finally {
            service.close();
        }
    }

    private void handle(WatchEvent.Kind<?> kind, Path file) {
        String name = file.getFileName().toString();
        try {
            if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                if (Files.isDirectory(file)) return;
                snapshots.put(file, snapshot(file));
                System.out.println("[CREATE] Создан файл: " + name);
            } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                if (Files.isDirectory(file)) return;
                Snapshot old = snapshots.get(file);
                Snapshot cur = snapshot(file);
                snapshots.put(file, cur);
                if (old == null) {
                    System.out.println("[CREATE] Создан файл: " + name);
                } else if (!old.lines.equals(cur.lines)) { // Windows шлёт несколько MODIFY на одну запись
                    System.out.println("[MODIFY] Изменён файл: " + name);
                    printDiff(old.lines, cur.lines);
                }
            } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                Snapshot old = snapshots.remove(file);
                if (old == null) return;
                System.out.println("[DELETE] Удалён файл: " + name + ", размер: " + old.size + " байт, контрольная сумма (16 бит): 0x"
                        + String.format("%04X", old.checksum));
            }
        } catch (IOException e) {
            System.out.println("[ERROR] " + name + ": " + e.getMessage());
        }
    }

    private static void printDiff(List<String> oldLines, List<String> newLines) {
        List<String> removed = new ArrayList<String>(oldLines);
        List<String> added = new ArrayList<String>(newLines);
        for (String s : newLines) removed.remove(s);
        for (String s : oldLines) added.remove(s);
        for (String s : added) System.out.println("  + " + s);
        for (String s : removed) System.out.println("  - " + s);
    }

    private static Snapshot snapshot(Path file) throws IOException {
        IOException last = null;
        for (int i = 0; i < 10; i++) { // файл может быть ещё занят записывающим процессом
            try {
                return new Snapshot(Files.readAllLines(file, StandardCharsets.UTF_8), Files.size(file), Checksum.checksum16(file));
            } catch (IOException e) {
                last = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw last;
    }
}
