package baas.supplier.config;

import baas.supplier.domain.Product;
import baas.supplier.domain.Supplier;
import baas.supplier.repository.ProductRepository;
import baas.supplier.repository.SupplierRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

/**
 * Заполняет пустую БД поставщиками и товарами (отключается SEED_DEMO_DATA=false).
 * Товары и цены те же, что в ПР №5 и №7, номера товаров 1–5 совпадают.
 */
@Component
@ConditionalOnProperty(name = "app.seed-demo-data", havingValue = "true")
public class DemoDataLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoDataLoader.class);

    private final SupplierRepository suppliers;
    private final ProductRepository products;

    public DemoDataLoader(SupplierRepository suppliers, ProductRepository products) {
        this.suppliers = suppliers;
        this.products = products;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        if (products.count() > 0) {
            log.info("Каталог уже заполнен — демо-данные не загружаются");
            return;
        }

        List<Supplier> s = suppliers.saveAll(List.of(
                new Supplier("ООО «Молочная ферма»", "7701234567", "г. Москва, ул. Складочная, д. 1"),
                new Supplier("АО «Хлебный дом»", "7812345678", "г. Москва, Дмитровское шоссе, д. 25"),
                new Supplier("ООО «ФрешФрукт»", "5029876543", "г. Мытищи, ул. Колпакова, д. 2"),
                new Supplier("ООО «Кофе Импорт»", "6612345678", "г. Москва, ул. Электрозаводская, д. 21")));

        products.saveAll(List.of(
                new Product(s.get(0), "Молоко 3,2%", "Пастеризованное, 1 л", new BigDecimal("89.90"), 500),
                new Product(s.get(1), "Хлеб бородинский", "Ржано-пшеничный, 400 г", new BigDecimal("65.00"), 300),
                new Product(s.get(2), "Яблоки Гала", "Свежие, 1 кг", new BigDecimal("159.00"), 800),
                new Product(s.get(0), "Сыр Российский", "Полутвёрдый, 300 г", new BigDecimal("329.00"), 40),
                new Product(s.get(3), "Кофе в зёрнах", "Арабика 100%, 1 кг", new BigDecimal("1490.00"), 25),
                new Product(s.get(3), "Чай зелёный", "Листовой, 100 г — нет в наличии", new BigDecimal("145.00"), 0)));

        log.info("Загружены демо-данные: {} поставщика, 6 товаров", s.size());
    }
}
