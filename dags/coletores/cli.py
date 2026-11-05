"""Roda uma coleta sem Airflow, útil para testar a fonte à mão.

    cd dags && python -m coletores.cli iluminacao luminarias 2026-11-04
    cd dags && python -m coletores.cli iluminacao todas 2026-11-04
"""
from __future__ import annotations

import sys
from pathlib import Path

from .comum import CHEGADA_PADRAO, ds_valida
from .temas import TEMAS


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__)
        return 2
    tema, fonte, ds = argv
    ds_valida(ds)
    fontes = TEMAS[tema]["fontes"]
    alvo = fontes if fonte == "todas" else {fonte: fontes[fonte]}
    for nome, coletor in alvo.items():
        caminho = coletor(ds=ds, chegada=Path(CHEGADA_PADRAO))
        print(f"{nome}: {caminho} ({caminho.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
