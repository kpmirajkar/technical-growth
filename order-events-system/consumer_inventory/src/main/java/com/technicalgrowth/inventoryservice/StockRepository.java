package com.technicalgrowth.inventoryservice;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, String> {

    /**
     * Atomic conditional decrement — returns 1 if reserved, 0 if the sku is
     * missing or short on stock. A read-then-write (findById, subtract, save)
     * would lose updates under concurrent consumers: two transactions read
     * quantity=50, both write 48. Pushing the check-and-decrement into one
     * UPDATE makes the database serialize it via the row lock.
     */
    @Modifying
    @Query("update Stock s set s.quantity = s.quantity - :qty where s.sku = :sku and s.quantity >= :qty")
    int tryReserve(@Param("sku") String sku, @Param("qty") int qty);
}
