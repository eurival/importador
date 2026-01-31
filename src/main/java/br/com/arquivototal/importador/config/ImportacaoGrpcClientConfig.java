package br.com.arquivototal.importador.config;

import br.com.arquivototal.gedtotalapi.grpc.ImportacaoGedServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ImportacaoGrpcClientConfig {

    @Bean(destroyMethod = "shutdownNow")
    public ManagedChannel importacaoGedChannel(
        @Value("${importacao.grpc.host}") String host,
        @Value("${importacao.grpc.port}") int port
    ) {
        // Se o host vier como "dns:///...", o gRPC usará o NameResolver nativo
        return ManagedChannelBuilder.forTarget(host + ":" + port)
            .defaultLoadBalancingPolicy("round_robin") 
            .usePlaintext()
            .build();
    }

    @Bean
    public ImportacaoGedServiceGrpc.ImportacaoGedServiceBlockingStub importacaoGedBlockingStub(ManagedChannel channel) {
        return ImportacaoGedServiceGrpc.newBlockingStub(channel);
    }
}