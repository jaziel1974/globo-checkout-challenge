package br.com.globo.checkout.bff.domain;

import br.com.globo.checkout.bff.domain.Models.Cart;
import br.com.globo.checkout.bff.domain.Models.PaymentMethod;
import br.com.globo.checkout.bff.domain.Models.Product;
import java.util.List;
import java.util.Optional;

public final class Ports {
    private Ports() {}
    public interface ProductCatalog { Product getProduct(String slug); }
    public interface PaymentMethodCatalog { List<PaymentMethod> getPaymentMethods(); }
    public interface CartRepository {
        void create(Cart cart);
        Optional<Cart> find(String cartId);
        ApprovalResult approve(String cartId);
    }
    public enum ApprovalResult { APPROVED, ALREADY_APPROVED, NOT_FOUND }
}
