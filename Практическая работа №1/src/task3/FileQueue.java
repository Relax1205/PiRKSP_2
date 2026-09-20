package task3;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Потокобезопасная ограниченная очередь файлов.
 * put() блокируется, пока очередь заполнена; take(type) блокируется,
 * пока в очереди нет файла нужного типа.
 */
public final class FileQueue {
    private final int capacity;
    private final LinkedList<File> files = new LinkedList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition fileAdded = lock.newCondition();
    private boolean closed;

    public FileQueue(int capacity) {
        this.capacity = capacity;
    }

    public void put(File file) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (closed) {
                throw new IllegalStateException("Очередь закрыта");
            }
            if (files.size() == capacity) {
                Log.info("очередь заполнена (" + capacity + "), генератор ждёт");
            }
            while (files.size() == capacity) {
                notFull.await();
            }
            files.addLast(file);
            fileAdded.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Забирает первый файл указанного типа.
     *
     * @return файл или null, если очередь закрыта и файлов этого типа больше нет
     */
    public File take(FileType type) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                Iterator<File> iterator = files.iterator();
                while (iterator.hasNext()) {
                    File file = iterator.next();
                    if (file.getType() == type) {
                        iterator.remove();
                        notFull.signal();
                        return file;
                    }
                }
                if (closed) {
                    return null;
                }
                fileAdded.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Новых файлов не будет: обработчики доберут оставшиеся и завершатся. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            fileAdded.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return files.size();
        } finally {
            lock.unlock();
        }
    }
}
