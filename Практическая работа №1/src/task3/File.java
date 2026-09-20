package task3;

/** Неизменяемый объект файла, поэтому его можно безопасно передавать между потоками. */
public final class File {
    private final int id;
    private final FileType type;
    private final int size;

    public File(int id, FileType type, int size) {
        this.id = id;
        this.type = type;
        this.size = size;
    }

    public int getId() {
        return id;
    }

    public FileType getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "File#" + id + "[" + type + ", size=" + size + "]";
    }
}
