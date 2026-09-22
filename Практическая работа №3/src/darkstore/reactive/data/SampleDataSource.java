package darkstore.reactive.data;

import darkstore.reactive.model.SupplyRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Тестовый набор данных, повторяющий структуру сущностей Darkstore/Good/GoodsList
 * из модуля "Back-end АС Поставщиков" (bootcamp-darkstore-provider), но без обращения
 * к его базе данных: поставщик = дарксторовский склад, товар = Good, количество = count.
 * Среди записей намеренно есть некорректные (отрицательная цена, пустое название,
 * нулевой остаток), чтобы продемонстрировать фильтрацию и обработку ошибок.
 */
public final class SampleDataSource {

    private SampleDataSource() {
    }

    public static List<SupplyRecord> initialBatch() {
        List<SupplyRecord> records = new ArrayList<>();
        records.add(new SupplyRecord(1, "Darkstore Center", "Молоко 1л", 89.90, 120));
        records.add(new SupplyRecord(1, "Darkstore Center", "Хлеб белый", 45.50, 0));      // нет в наличии -> отфильтруется
        records.add(new SupplyRecord(2, "Darkstore North", "Йогурт клубничный", 65.00, 80));
        records.add(new SupplyRecord(2, "Darkstore North", "Сыр Российский", -120.00, 15)); // некорректная цена -> ошибка обработки
        records.add(new SupplyRecord(3, "Darkstore South", "Вода питьевая 1.5л", 35.00, 200));
        records.add(new SupplyRecord(3, "Darkstore South", null, 50.00, 10));               // некорректное название -> ошибка обработки
        records.add(new SupplyRecord(1, "Darkstore Center", "Яйца С1", 110.00, 40));
        records.add(new SupplyRecord(2, "Darkstore North", "Масло сливочное", 210.00, 25));
        records.add(new SupplyRecord(3, "Darkstore South", "Овсянка 500г", 78.00, 0));       // нет в наличии -> отфильтруется
        records.add(new SupplyRecord(1, "Darkstore Center", "Сок апельсиновый", 99.90, 60));
        return records;
    }

    /** Данные, "поступающие" во времени для демонстрации live-обработки. */
    public static List<SupplyRecord> liveBatch() {
        List<SupplyRecord> records = new ArrayList<>();
        records.add(new SupplyRecord(1, "Darkstore Center", "Кефир 1л", 72.00, 30));
        records.add(new SupplyRecord(2, "Darkstore North", "Творог 200г", 58.00, 45));
        records.add(new SupplyRecord(3, "Darkstore South", "Гречка 900г", 89.00, 70));
        records.add(new SupplyRecord(2, "Darkstore North", "Пельмени", -15.00, 20)); // некорректная цена
        records.add(new SupplyRecord(1, "Darkstore Center", "Чай зелёный", 145.00, 12));
        records.add(new SupplyRecord(3, "Darkstore South", "Макароны", 55.00, 0));   // нет в наличии
        return records;
    }
}
