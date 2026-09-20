package task3;

import java.util.ArrayList;
import java.util.List;

/**
 * Задание 3. Система обработки файлов: генератор -> очередь (вместимость 5) -> обработчики по типам.
 *
 * Запуск: java task3.Main [количество файлов]
 */
public class Main {
    private static final int QUEUE_CAPACITY = 5;

    public static void main(String[] args) throws InterruptedException {
        int fileCount = args.length > 0 ? Integer.parseInt(args[0]) : 20;
        FileQueue queue = new FileQueue(QUEUE_CAPACITY);

        List<Thread> processors = new ArrayList<>();
        for (FileType type : FileType.values()) {
            Thread thread = new Thread(new FileProcessor(queue, type), "processor-" + type);
            processors.add(thread);
            thread.start();
        }

        Thread generator = new Thread(new FileGenerator(queue, fileCount), "generator");
        long start = System.currentTimeMillis();
        generator.start();

        generator.join();
        queue.close();
        for (Thread processor : processors) {
            processor.join();
        }

        Log.info("все файлы обработаны за " + (System.currentTimeMillis() - start) + " мс");
    }
}
