"""Coletor genérico para portais CKAN (Prefeitura de SP e ANEEL usam CKAN).

Em vez de fixar o link do arquivo (que muda a cada publicação semestral), o coletor
consulta package_show, escolhe o recurso pela regra configurada e baixa a versão
mais recente. O download bruto fica em cache por (id do recurso, last_modified),
para que a coleta diária não baixe de novo um CSV de centenas de MB que não mudou.
"""
from __future__ import annotations

import csv
import io
import re
import unicodedata
from pathlib import Path
from typing import Iterable

from .comum import destino, get_com_retry, metadados_coleta

csv.field_size_limit(10_000_000)


def _sem_acento(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", texto)
                   if not unicodedata.combining(c)).lower()


def escolher_recurso(recursos: list[dict], padrao: str, formato: str = "CSV") -> dict:
    """Recurso mais recente cujo nome/descrição/url casa com `padrao` e tem o formato pedido."""
    rx = re.compile(padrao, re.IGNORECASE)
    candidatos = []
    for r in recursos:
        fmt = (r.get("format") or "").upper()
        url = r.get("url") or ""
        if fmt != formato.upper() and not url.lower().endswith("." + formato.lower()):
            continue
        texto = " ".join(str(r.get(k) or "") for k in ("name", "description", "url"))
        if rx.search(texto) or rx.search(_sem_acento(texto)):
            candidatos.append(r)
    if not candidatos:
        nomes = [r.get("name") for r in recursos]
        raise LookupError(f"Nenhum recurso {formato} casa com /{padrao}/. Disponíveis: {nomes}")
    return max(candidatos, key=_chave_recencia)


def _chave_recencia(r: dict) -> tuple:
    """Ordena por (ano no nome, período no nome, dia da última modificação).

    A Prefeitura republica vários trimestres no mesmo dia (1º, 2º TRI 2026 e até 2025
    saíram com last_modified 2026-08-10), então a data não serve para escolher.
    O nome "Dados do SP156 - 2º TRI 2026" resolve: ano 2026, período 2. Recursos
    sem ano no nome (bandeiras da ANEEL) caem no desempate pela data.
    """
    dia = (r.get("last_modified") or r.get("metadata_modified") or r.get("created") or "")[:10]
    nome = r.get("name") or ""
    ano = max((int(a) for a in re.findall(r"(20\d{2})", nome)), default=0)
    m = re.search(r"(\d)\s*[º°o]?\s*(tri|sem)", nome, re.IGNORECASE)
    periodo = int(m.group(1)) if m else 0
    return (ano, periodo, dia)


def _decodificar(caminho: Path) -> io.TextIOWrapper:
    """Portais públicos ainda publicam muito CSV em Latin-1; tenta UTF-8 primeiro."""
    with open(caminho, "rb") as f:
        amostra = f.read(200_000)
    try:
        amostra.decode("utf-8")
        enc = "utf-8-sig"
    except UnicodeDecodeError:
        enc = "latin-1"
    return open(caminho, encoding=enc, newline="")


def filtrar_csv(origem: Path, saida: Path, palavras: Iterable[str] | None,
                colunas: Iterable[str] | None = None) -> tuple[int, int]:
    """Copia para `saida` (UTF-8, vírgula) só as linhas que contêm alguma das palavras.

    Retorna (linhas_lidas, linhas_gravadas). Sem palavras, copia tudo normalizado.
    """
    palavras_n = [_sem_acento(p) for p in (palavras or [])]
    with _decodificar(origem) as f:
        amostra = f.read(50_000)
        f.seek(0)
        try:
            dialeto = csv.Sniffer().sniff(amostra, delimiters=";,|\t")
        except csv.Error:
            dialeto = csv.excel
        leitor = csv.reader(f, dialeto)
        cabecalho = next(leitor)
        cabecalho = [c.strip() for c in cabecalho]
        idx = list(range(len(cabecalho)))
        if colunas:
            alvo = {_sem_acento(c) for c in colunas}
            idx = [i for i, c in enumerate(cabecalho) if _sem_acento(c) in alvo] or idx
        lidas = gravadas = 0
        tmp = saida.with_suffix(saida.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8", newline="") as out:
            escritor = csv.writer(out, quoting=csv.QUOTE_MINIMAL)
            escritor.writerow(cabecalho)
            for linha in leitor:
                lidas += 1
                if palavras_n:
                    texto = _sem_acento(" ".join(linha[i] for i in idx if i < len(linha)))
                    if not any(p in texto for p in palavras_n):
                        continue
                escritor.writerow(linha)
                gravadas += 1
        tmp.replace(saida)
    return lidas, gravadas


def coletar(ds: str, chegada: Path, *, fonte: str, base_url: str, dataset: str,
            padrao_recurso: str, palavras: list[str] | None = None,
            colunas_filtro: list[str] | None = None) -> Path:
    pacote = get_com_retry(f"{base_url}/api/3/action/package_show",
                           params={"id": dataset}).json()
    if not pacote.get("success"):
        raise RuntimeError(f"CKAN recusou package_show de {dataset}: {pacote.get('error')}")
    recurso = escolher_recurso(pacote["result"]["resources"], padrao_recurso)

    versao = re.sub(r"[^0-9A-Za-z]", "", recurso.get("last_modified")
                    or recurso.get("created") or "semdata")
    cache = Path(chegada) / "_cache" / f"{fonte}_{recurso['id']}_{versao}.csv"
    if not cache.exists():
        cache.parent.mkdir(parents=True, exist_ok=True)
        r = get_com_retry(recurso["url"], timeout=300, stream=True)
        tmp = cache.with_suffix(".part")
        with open(tmp, "wb") as f:
            for bloco in r.iter_content(1 << 20):
                f.write(bloco)
        tmp.replace(cache)

    saida = destino(chegada, fonte, ds, "csv")
    lidas, gravadas = filtrar_csv(cache, saida, palavras, colunas_filtro)
    if gravadas == 0:
        saida.unlink(missing_ok=True)
        raise ValueError(f"{fonte}: filtro {palavras} não encontrou linhas em {lidas} lidas. "
                         "Confira o nome das colunas/termos no CSV baixado em _cache/.")
    # metadados ao lado do CSV (o CSV em si fica puro para o Auto Loader)
    meta = metadados_coleta(fonte, ds, recurso["url"], recurso=recurso.get("name"),
                            recurso_id=recurso["id"], linhas_lidas=lidas,
                            linhas_gravadas=gravadas)
    import json
    (saida.parent / "_meta").mkdir(exist_ok=True)
    (saida.parent / "_meta" / f"{saida.stem}.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return saida


def coletar_datastore(ds: str, chegada: Path, *, fonte: str, base_url: str, dataset: str,
                      padrao_recurso: str, filtros: dict, ordenar: str | None = None,
                      pagina: int = 1000) -> Path:
    """Consulta a API datastore_search do CKAN já filtrada, em vez de baixar o arquivo todo.

    Usado nas tarifas da ANEEL: o CSV completo tem ~180 MB e 650 mil linhas, das
    quais o produto usa ~400 (Eletropaulo/Enel SP, subgrupo B1, tarifa de aplicação).
    """
    pacote = get_com_retry(f"{base_url}/api/3/action/package_show", params={"id": dataset}).json()
    recurso = escolher_recurso(pacote["result"]["resources"], padrao_recurso)
    if not recurso.get("datastore_active"):
        raise RuntimeError(f"{fonte}: recurso {recurso.get('name')} não tem datastore ativo")
    import json
    registros, offset, total = [], 0, None
    while total is None or offset < total:
        params = {"resource_id": recurso["id"], "filters": json.dumps(filtros, ensure_ascii=False),
                  "limit": pagina, "offset": offset}
        if ordenar:
            params["sort"] = ordenar
        corpo = get_com_retry(f"{base_url}/api/3/action/datastore_search", params=params).json()
        resultado = corpo["result"]
        total = resultado.get("total", 0)
        lote = resultado["records"]
        if not lote:
            break
        registros.extend(lote)
        offset += len(lote)
    if not registros:
        raise ValueError(f"{fonte}: filtros {filtros} não retornaram registros")
    from .comum import gravar_json
    return gravar_json(destino(chegada, fonte, ds, "json"), registros,
                       metadados_coleta(fonte, ds, f"{base_url}/api/3/action/datastore_search",
                                        recurso=recurso.get("name"), recurso_id=recurso["id"],
                                        filtros=filtros, registros=len(registros)))
