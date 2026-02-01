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
    private final MeterRegistry meterRegistry;

    private final Counter mensagensRecebidas;
    private final Counter mensagensInvalidas;
    private final Counter importacoesProcessadas;
    private final Counter importacoesSucesso;
    private final Counter importacoesJaExiste;
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

    @KafkaListener(
        topics = "${importacao.kafka.topic.solicitacoes}",
        containerFactory = "kafkaListenerContainerFactory"  
    )
    public void consumir(String mensagem, Acknowledgment acknowledgment) {
        mensagensRecebidas.increment();
        emProcessamento.incrementAndGet();
        Timer.Sample sample = Timer.start();
        ImportacaoPayload payload = null;
        
        try {
            // 1. Tenta ler o JSON
            try {
                payload = objectMapper.readValue(mensagem, ImportacaoPayload.class);
            } catch (Exception e) {
                log.error("Erro de desserializacao (DLQ): {}", e.getMessage());
                mensagensInvalidas.increment();
                incrementarErro("desserializacao", e.getClass().getSimpleName());
                publicarFalha(null, "Payload invalido: " + e.getMessage(), null);
                acknowledgment.acknowledge(); // Descarta pois o JSON está quebrado
                return;
            }

            // 2. Processa via gRPC
            ImportarIndiceRequest request = montarRequest(payload);
            ImportarIndiceResponse response = grpcClient.importarIndice(request);
            importacoesProcessadas.increment();

            // 3. Verifica Resposta
            if (response.getStatus() == ImportacaoStatus.IMPORTADO || response.getStatus() == ImportacaoStatus.JA_EXISTE) {
                if (response.getStatus() == ImportacaoStatus.JA_EXISTE) {
                    importacoesJaExiste.increment();
                } else {
                    importacoesSucesso.increment();
                }
                acknowledgment.acknowledge(); // Sucesso: Confirma leitura
            } else {
                // Erro de Negócio (ex: Validação de campo falhou no GedTotal)
                log.error("Erro de negocio gRPC (DLQ): {}", response.getMensagem());
                incrementarErro("negocio", response.getMensagem()); 
                publicarFalha(payload, response.getMensagem(), payload.correlationId());
                acknowledgment.acknowledge(); // Descarta para não entrar em loop de erro de validação
            }

        } catch (Exception e) {
            // 4. ERRO TÉCNICO (Rede, gRPC fora, S3 instável, Banco travado)
            // NÃO damos o acknowledge. Relançamos a exceção para o Kafka acionar o Retry.
            log.error("Falha tecnica detectada (RETRY): {}", e.getMessage());
            incrementarErro("tecnico_retry", e.getClass().getSimpleName());
            throw new RuntimeException("Erro tecnico gRPC/Infra - Mensagem voltara para a fila", e);
        } finally {
            sample.stop(importacaoTimer);
            emProcessamento.decrementAndGet();
        }
    }

    private void incrementarErro(String tipo, String detalhe) {
        String detalheSeguro = (detalhe != null && detalhe.length() > 64) ? detalhe.substring(0, 64) : (detalhe != null ? detalhe : "desconhecido");
        meterRegistry.counter("importador.importacao.erro", "tipo", tipo, "erro", detalheSeguro).increment();
    }

    private ImportarIndiceRequest montarRequest(ImportacaoPayload payload) {
        ImportarIndiceRequest.Builder builder = ImportarIndiceRequest.newBuilder();
        if (payload.legacy() != null) builder.setLegacy(montarLegacy(payload.legacy()));
        if (payload.clienteId() != null) builder.setClienteId(payload.clienteId());
        if (payload.departamentoId() != null) builder.setDepartamentoId(payload.departamentoId());
        if (payload.projetoId() != null) builder.setProjetoId(payload.projetoId());
        if (payload.formularioId() != null) builder.setFormularioId(payload.formularioId());
        if (payload.loteId() != null) builder.setLoteId(payload.loteId());
        if (payload.usuarioId() != null) builder.setUsuarioId(payload.usuarioId());
        if (payload.baseMountPath() != null && !payload.baseMountPath().isBlank()) builder.setBaseMountPath(payload.baseMountPath());
        if (payload.correlationId() != null && !payload.correlationId().isBlank()) builder.setCorrelationId(payload.correlationId());
        if (payload.formData() != null) {
            for (FormDataPayload item : payload.formData()) {
                if (item == null || item.campoId() == null) continue;
                builder.addFormData(FormDataItem.newBuilder().setCampoId(item.campoId()).setValor(item.valor() != null ? item.valor() : "").build());
            }
        }
        return builder.build();
    }

    private LegacyIndice montarLegacy(LegacyIndicePayload legacy) {
        LegacyIndice.Builder builder = LegacyIndice.newBuilder();
        if (legacy.idIndice() != null) builder.setIdIndice(legacy.idIndice());
        if (legacy.idProjeto() != null) builder.setIdProjeto(legacy.idProjeto());
        if (legacy.campos() != null) builder.putAllCampos(legacy.campos());
        if (legacy.arquivo() != null) builder.setArquivo(legacy.arquivo());
        if (legacy.npaginas() != null) builder.setNpaginas(legacy.npaginas());
        if (legacy.tamanho() != null) builder.setTamanho(legacy.tamanho());
        if (legacy.idUsuarioCreate() != null) builder.setIdUsuarioCreate(legacy.idUsuarioCreate());
        if (legacy.ocr() != null) builder.setOcr(legacy.ocr());
        if (legacy.lote() != null) builder.setLote(legacy.lote());
        if (legacy.dataPublicacao() != null) builder.setDataPublicacao(legacy.dataPublicacao());
        if (legacy.horaPublicacao() != null) builder.setHoraPublicacao(legacy.horaPublicacao());
        if (legacy.ext() != null) builder.setExt(legacy.ext());
        if (legacy.ocrStatus() != null) builder.setOcrStatus(legacy.ocrStatus());
        if (legacy.storage() != null) builder.setStorage(legacy.storage());
        return builder.build();
    }

    private void publicarFalha(ImportacaoPayload payload, String mensagem, String correlationId) {
        try {
            long idIndice = (payload != null && payload.legacy() != null && payload.legacy().idIndice() != null) ? payload.legacy().idIndice() : 0L;
            Map<String, Object> envelope = new java.util.HashMap<>();
            envelope.put("falha", mensagem != null ? mensagem : "Erro nao informado");
            envelope.put("idIndice", idIndice);
            if (correlationId != null) envelope.put("correlationId", correlationId);
            String body = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(new ProducerRecord<>(topicFalhas, Long.toString(idIndice), body));
            falhasPublicadas.increment();
        } catch (Exception e) {
            log.error("Falha crítica ao publicar no tópico de erro: {}", e.getMessage());
        }
    }
}