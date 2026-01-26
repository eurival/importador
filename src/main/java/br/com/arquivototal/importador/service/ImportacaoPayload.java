package br.com.arquivototal.importador.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ImportacaoPayload(
    LegacyIndicePayload legacy,
    Long clienteId,
    Long departamentoId,
    Long projetoId,
    Long formularioId,
    Long loteId,
    Long usuarioId,
    List<FormDataPayload> formData,
    String baseMountPath,
    String correlationId
) {}

@JsonIgnoreProperties(ignoreUnknown = true)
record LegacyIndicePayload(
    Long idIndice,
    Long idProjeto,
    Map<String, String> campos,
    String arquivo,
    Integer npaginas,
    Double tamanho,
    Integer idUsuarioCreate,
    String ocr,
    String lote,
    String dataPublicacao,
    String horaPublicacao,
    String ext,
    Integer ocrStatus,
    String storage
) {}

@JsonIgnoreProperties(ignoreUnknown = true)
record FormDataPayload(Long campoId, String valor) {}
