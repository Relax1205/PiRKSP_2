package task3;

import java.util.concurrent.ThreadLocalRandom;

/** Генерирует файлы случайного типа и размера (10–100) с задержкой 100–1000 мс. */
public final class FileGenerator implements Runnable {
    private final FileQueue queue;
    private final int fileCount;

    public FileGenerator(FileQueue queue, int fileCount) {
        this.queue = queue;
        this.fileCount = fileCount;
    }

    @Override
    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        FileType[] types = FileType.values();
        try {
            for (int id = 1; id <= fileCount; id++) {
                Thread.sleep(random.nextInt(100, 1001));
                File file = new File(id, types[random.nextInt(types.length)], random.nextInt(10, 101));
                Log.info("создан " + file);
                queue.put(file);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Log.info("генерация завершена");
    }
}
