package task3;

/** Обрабатывает файлы только своего типа; время обработки = размер * 7 мс. */
public final class FileProcessor implements Runnable {
    private static final int MS_PER_SIZE_UNIT = 7;

    private final FileQueue queue;
    private final FileType type;
    private int processed;

    public FileProcessor(FileQueue queue, FileType type) {
        this.queue = queue;
        this.type = type;
    }

    @Override
    public void run() {
        try {
            File file;
            while ((file = queue.take(type)) != null) {
                long time = (long) file.getSize() * MS_PER_SIZE_UNIT;
                Log.info("начата обработка " + file + " (" + time + " мс), в очереди: " + queue.size());
                Thread.sleep(time);
                processed++;
                Log.info("обработан " + file);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Log.info("обработчик " + type + " завершён, обработано файлов: " + processed);
    }
}
