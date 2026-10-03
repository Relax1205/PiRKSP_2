package baas.supplier.repository;

import baas.supplier.domain.Product;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** @EntityGraph подгружает поставщика одним запросом вместе с товаром. */
public interface ProductRepository extends JpaRepository<Product, Long> {

    @EntityGraph(attributePaths = "supplier")
    List<Product> findAllByOrderById();

    @EntityGraph(attributePaths = "supplier")
    List<Product> findByIdInOrderById(Collection<Long> ids);

    @EntityGraph(attributePaths = "supplier")
    List<Product> findBySupplierIdOrderById(Long supplierId);

    @EntityGraph(attributePaths = "supplier")
    Optional<Product> findWithSupplierById(Long id);
}
