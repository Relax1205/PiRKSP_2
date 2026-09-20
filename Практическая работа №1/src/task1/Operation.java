package task1;

/**
 * Операция свёртки массива. Каждая операция задаётся нейтральным элементом
 * и ассоциативной функцией объединения, поэтому частичные результаты,
 * посчитанные в разных потоках, можно безопасно объединять.
 */
public enum Operation {
    SUM("Сумма элементов") {
        @Override
        public long identity() {
            return 0L;
        }

        @Override
        public long combine(long a, long b) {
            return a + b;
        }
    },
    MAX("Максимальный элемент") {
        @Override
        public long identity() {
            return Long.MIN_VALUE;
        }

        @Override
        public long combine(long a, long b) {
            return Math.max(a, b);
        }
    },
    MIN("Минимальный элемент") {
        @Override
        public long identity() {
            return Long.MAX_VALUE;
        }

        @Override
        public long combine(long a, long b) {
            return Math.min(a, b);
        }
    };

    private final String title;

    Operation(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public abstract long identity();

    public abstract long combine(long a, long b);
}
