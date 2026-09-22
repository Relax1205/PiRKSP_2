package suppliers.rsocket.server;

import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;

/**
 * Точка входа RSocket-сервера АС Поставщиков.
 * Запускает TCP-транспорт на localhost:7000 и обрабатывает запросы
 * по моделям Request-Response, Request-Stream, Fire-and-Forget и Channel.
 */
public class ServerMain {

    public static void main(String[] args) {
        DataStore store = new DataStore();

        CloseableChannel server = RSocketServer.create(SocketAcceptor.with(new SupplierRSocket(store)))
                .bind(TcpServerTransport.create("localhost", 7000))
                .block();

        System.out.println("=========================================================");
        System.out.println(" RSocket-сервер АС Поставщиков запущен на localhost:7000");
        System.out.println(" Поддерживаемые маршруты:");
        System.out.println("   product.get     (Request-Response)");
        System.out.println("   product.stream  (Request-Stream)");
        System.out.println("   order.create    (Fire-and-Forget)");
        System.out.println("   order.track     (Channel)");
        System.out.println("=========================================================");

        server.onClose().block();
    }
}
