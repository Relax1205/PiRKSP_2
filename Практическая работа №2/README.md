# Практическая работа №2 — Java NIO / NIO.2 (обмен файлами с поставщиками)

Требуется JDK 8+ (нужен `javac`; на машине сейчас установлен только JRE 8 — поставьте JDK, например Temurin 17/21).

```
run.bat --seed            :: скопировать 3 тестовых файла в data/incoming, обработать, слушать каталог
run.bat --seed --demo     :: то же + каждые 5 с сам подбрасывает suppliers_04/05 и broken_06 (для скриншота WatchService)
run.bat --seed --once     :: обработать и выйти
```
Вручную: во время работы скопируйте любой .csv в `data/incoming` — появится «Обнаружен новый файл: ...».

Формат: `supplierId,supplierName,product,price,quantity` (UTF-8, поля без кавычек и без запятых внутри).

## Сценарий
WatchService (ENTRY_CREATE) → ожидание готовности файла → чтение
(первый файл — `FileChannel` + `ByteBuffer`, остальные — `Files.readAllLines`) → валидация и суммы по поставщикам →
SHA-256 → `Files.move` в `data/processed` (файл без единой корректной строки — в `data/failed`).
Отчёты дописываются в `data/report.txt`.

## Структура
- `DirectoryInspector` — проверка/создание каталога, листинг (имя, размер, время изменения), поиск по расширению
- `FileProcessor` — чтение, разбор, SHA-256, перемещение
- `IncomingWatcher` — WatchService
- `SupplierRecord`, `ProcessingResult`, `Checksum`, `Main`

Поля соответствуют сущностям модуля «База данных АС Поставщиков» (поставщик, товар, цена, количество).
