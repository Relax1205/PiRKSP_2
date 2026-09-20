package suppliers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/** Наблюдение за каталогом incoming через WatchService. */
public class IncomingWatcher implements Closeable {
    private final Path dir;
    private final String extension;
    private final FileProcessor processor;
    private final WatchService service;

    /** Регистрация выполняется в конструкторе, чтобы не потерять файлы, пришедшие во время начального скана. */
    public IncomingWatcher(Path dir, String extension, FileProcessor processor) throws IOException {
        this.dir = dir;
        this.extension = extension;
        this.processor = processor;
        this.service = FileSystems.getDefault().newWatchService();
        dir.register(service, StandardWatchEventKinds.ENTRY_CREATE);
    }

    /** Блокирующий цикл ожидания событий. */
    public void run() throws InterruptedException, IOException {
        System.out.println("Наблюдение за " + dir.toAbsolutePath() + " запущено (Ctrl+C для выхода)...");
        try {
            while (true) {
                WatchKey key = service.take();
                for (WatchEvent<?> ev : key.pollEvents()) {
                    if (ev.kind() == StandardWatchEventKinds.OVERFLOW) {
                        for (Path p : DirectoryInspector.findByExtension(dir, extension)) processor.process(p);
                        continue;
                    }
                    Path file = dir.resolve((Path) ev.context());
                    if (DirectoryInspector.hasExtension(file, extension)) {
                        System.out.println("Обнаружен новый файл: " + file.getFileName());
                        processor.process(file);
                    } else {
                        System.out.println("Пропущен файл с другим расширением: " + file.getFileName());
                    }
                }
                if (!key.reset()) {
                    System.out.println("Каталог больше недоступен, наблюдение остановлено.");
                    break;
                }
            }
        } catch (ClosedWatchServiceException e) {
            // штатное закрытие
        }
    }

    @Override
    public void close() throws IOException {
        service.close();
    }
}
