package br.com.arquivototal.importador.service;

import br.com.arquivototal.gedtotalapi.grpc.FormDataItem;
import br.com.arquivototal.gedtotalapi.grpc.ImportacaoFalha;
import br.com.arquivototal.gedtotalapi.grpc.ImportacaoStatus;
import br.com.arquivototal.gedtotalapi.grpc.ImportarIndiceRequest;
import br.com.arquivototal.gedtotalapi.grpc.ImportarIndiceResponse;
import br.com.arquivototal.gedtotalapi.grpc.LegacyIndice;
import br.com.arquivototal.importador.grpc.ImportacaoGedGrpcClient;
 

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class ImportacaoKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(ImportacaoKafkaConsumer.class);

    private final ImportacaoGedGrpcClient grpcClient;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicFalhas;
    private final MeterRegistry meterRegistry; // Registry injetado

    // Contadores Estáticos
    private final Counter mensagensRecebidas;
    private final Counter mensagensInvalidas;
    private final Counter importacoesProcessadas;
    private final Counter importacoesSucesso;
    private final Counter importacoesJaExiste;
    // NOTA: importacoesErro foi removido daqui para ser dinâmico
    private final Counter falhasPublicadas;
    private final Timer importacaoTimer;
    private final AtomicInteger emProcessamento;

    public ImportacaoKafkaConsumer(
        ImportacaoGedGrpcClient grpcClient,
        ObjectMapper objectMapper,
        KafkaTemplate<String, String> kafkaTemplate,
        @Value("${importacao.kafka.topic.falhas}") String topicFalhas,
        MeterRegistry meterRegistry
    ) {
        this.grpcClient = grpcClient;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.topicFalhas = topicFalhas;
        this.meterRegistry = meterRegistry;

        this.mensagensRecebidas = meterRegistry.counter("importador.kafka.mensagens.recebidas");
        this.mensagensInvalidas = meterRegistry.counter("importador.kafka.mensagens.invalidas");
        this.importacoesProcessadas = meterRegistry.counter("importador.importacao.processadas");
        this.importacoesSucesso = meterRegistry.counter("importador.importacao.sucesso");
        this.importacoesJaExiste = meterRegistry.counter("importador.importacao.ja_existe");
        this.falhasPublicadas = meterRegistry.counter("importador.importacao.falhas_publicadas");
        this.importacaoTimer = meterRegistry.timer("importador.importacao.tempo");
        this.emProcessamento = new AtomicInteger(0);
        meterRegistry.gauge("importador.importacao.em_processamento", this.emProcessamento);
    }

    @KafkaListener(topics = "${importacao.kafka.topic.solicitacoes}")
    public void consumir(String mensagem, Acknowledgment acknowledgment) {
        mensagensRecebidas.increment();
        emProcessamento.incrementAndGet();
        Timer.Sample sample = Timer.start();
        ImportacaoPayload payload = null;
        
        try {
            payload = objectMapper.readValue(mensagem, ImportacaoPayload.class);
        } catch (Exception e) {
            log.error("Falha ao desserializar mensagem de importacao: {}", e.getMessage(), e);
            mensagensInvalidas.increment();
            
            // Registra erro de JSON/Parse
            incrementarErro("desserializacao", e.getClass().getSimpleName());
            
            publicarFalha(null, "Payload invalido: " + e.getMessage(), null);
            acknowledgment.acknowledge();
            sample.stop(importacaoTimer);
            emProcessamento.decrementAndGet();
            return;
        }

        try {
            ImportarIndiceRequest request = montarRequest(payload);
            ImportarIndiceResponse response = grpcClient.importarIndice(request);
            importacoesProcessadas.increment();

            if (response.getStatus() == ImportacaoStatus.IMPORTADO) {
                importacoesSucesso.increment();
            } else if (response.getStatus() == ImportacaoStatus.JA_EXISTE) {
                // JA_EXISTE não é erro, é um estado válido de negócio
                importacoesJaExiste.increment();
            } else {
                // Erro de Negócio vindo do gRPC (ex: Validação falhou)
                String erroNegocio = response.getMensagem();
                incrementarErro("negocio", erroNegocio); 
                
                publicarFalha(payload, response.getMensagem(), payload.correlationId());
            }
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Erro ao processar importacao: {}", e.getMessage(), e);
            
            // Erro técnico (NullPointer, Banco fora, etc)
            incrementarErro("processamento", e.getClass().getSimpleName());
            
            publicarFalha(payload, e.getMessage(), payload != null ? payload.correlationId() : null);
            acknowledgment.acknowledge();
        } finally {
            sample.stop(importacaoTimer);
            emProcessamento.decrementAndGet();
        }
    }

    /**
     * Incrementa o contador de erros com tags dinâmicas.
     */
    private void incrementarErro(String tipo, String detalhe) {
        String detalheSeguro = "desconhecido";
        
        if (detalhe != null) {
            // TRUNCAR para evitar erros gigantes que explodem a memória do Prometheus
            detalheSeguro = detalhe.length() > 64 ? detalhe.substring(0, 64) : detalhe;
        }

        meterRegistry.counter("importador.importacao.erro",
            "tipo", tipo,
            "erro", detalheSeguro
        ).increment();
    }

    // --- MÉTODOS ORIGINAIS (Mantidos iguais) ---

    private ImportarIndiceRequest montarRequest(ImportacaoPayload payload) {
        ImportarIndiceRequest.Builder builder = ImportarIndiceRequest.newBuilder();
        if (payload.legacy() != null) {
            builder.setLegacy(montarLegacy(payload.legacy()));
        }
        if (payload.clienteId() != null) {
            builder.setClienteId(payload.clienteId());
        }
        if (payload.departamentoId() != null) {
            builder.setDepartamentoId(payload.departamentoId());
        }
        if (payload.projetoId() != null) {
            builder.setProjetoId(payload.projetoId());
        }
        if (payload.formularioId() != null) {
            builder.setFormularioId(payload.formularioId());
        }
        if (payload.loteId() != null) {
            builder.setLoteId(payload.loteId());
        }
        if (payload.usuarioId() != null) {
            builder.setUsuarioId(payload.usuarioId());
        }
        if (payload.baseMountPath() != null && !payload.baseMountPath().isBlank()) {
            builder.setBaseMountPath(payload.baseMountPath());
        }
        if (payload.correlationId() != null && !payload.correlationId().isBlank()) {
            builder.setCorrelationId(payload.correlationId());
        }
        if (payload.formData() != null) {
            for (FormDataPayload item : payload.formData()) {
                if (item == null || item.campoId() == null) {
                    continue;
                }
                String valor = item.valor() != null ? item.valor() : "";
                builder.addFormData(FormDataItem.newBuilder().setCampoId(item.campoId()).setValor(valor).build());
            }
        }
        return builder.build();
    }

    private LegacyIndice montarLegacy(LegacyIndicePayload legacy) {
        LegacyIndice.Builder builder = LegacyIndice.newBuilder();
        if (legacy.idIndice() != null) {
            builder.setIdIndice(legacy.idIndice());
        }
        if (legacy.idProjeto() != null) {
            builder.setIdProjeto(legacy.idProjeto());
        }
        Map<String, String> campos = legacy.campos();
        if (campos != null && !campos.isEmpty()) {
            builder.putAllCampos(campos);
        }
        if (legacy.arquivo() != null) {
            builder.setArquivo(legacy.arquivo());
        }
        if (legacy.npaginas() != null) {
            builder.setNpaginas(legacy.npaginas());
        }
        if (legacy.tamanho() != null) {
            builder.setTamanho(legacy.tamanho());
        }
        if (legacy.idUsuarioCreate() != null) {
            builder.setIdUsuarioCreate(legacy.idUsuarioCreate());
        }
        if (legacy.ocr() != null) {
            builder.setOcr(legacy.ocr());
        }
        if (legacy.lote() != null) {
            builder.setLote(legacy.lote());
        }
        if (legacy.dataPublicacao() != null) {
            builder.setDataPublicacao(legacy.dataPublicacao());
        }
        if (legacy.horaPublicacao() != null) {
            builder.setHoraPublicacao(legacy.horaPublicacao());
        }
        if (legacy.ext() != null) {
            builder.setExt(legacy.ext());
        }
        if (legacy.ocrStatus() != null) {
            builder.setOcrStatus(legacy.ocrStatus());
        }
        if (legacy.storage() != null) {
            builder.setStorage(legacy.storage());
        }
        return builder.build();
    }

    private void publicarFalha(ImportacaoPayload payload, String mensagem, String correlationId) {
        try {
            long idIndice = payload != null && payload.legacy() != null && payload.legacy().idIndice() != null
                ? payload.legacy().idIndice()
                : 0L;
            ImportacaoFalha falha = ImportacaoFalha.newBuilder()
                .setIdIndice(idIndice)
                .setMensagem(mensagem != null ? mensagem : "Erro nao informado")
                .build();
            Map<String, Object> envelope = new java.util.HashMap<>();
            envelope.put("falha", falha.getMensagem());
            envelope.put("idIndice", falha.getIdIndice());
            if (correlationId != null) {
                envelope.put("correlationId", correlationId);
            }
            String body = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(new ProducerRecord<>(topicFalhas, Long.toString(idIndice), body));
            falhasPublicadas.increment();
        } catch (Exception e) {
            log.error("Falha ao publicar erro no topico de falhas: {}", e.getMessage(), e);
        }
    }
}