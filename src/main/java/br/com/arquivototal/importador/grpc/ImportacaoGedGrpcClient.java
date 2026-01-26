package br.com.arquivototal.importador.grpc;

import br.com.arquivototal.gedtotalapi.grpc.ImportacaoGedServiceGrpc;
import br.com.arquivototal.gedtotalapi.grpc.ImportarIndiceRequest;
import br.com.arquivototal.gedtotalapi.grpc.ImportarIndiceResponse;
import org.springframework.stereotype.Service;

@Service
public class ImportacaoGedGrpcClient {

    private final ImportacaoGedServiceGrpc.ImportacaoGedServiceBlockingStub stub;

    public ImportacaoGedGrpcClient(ImportacaoGedServiceGrpc.ImportacaoGedServiceBlockingStub stub) {
        this.stub = stub;
    }

    public ImportarIndiceResponse importarIndice(ImportarIndiceRequest request) {
        return stub.importarIndice(request);
    }
}
