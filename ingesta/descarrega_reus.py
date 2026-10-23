"""Ingesta del portal Open Data Reus cap a Databricks.

Per a cada dataset:
  1. Pregunta a l'API CKAN del portal l'enllaç i la data d'actualització del CSV.
  2. Si aquesta versió ja és al volume `landing`, se la salta (càrrega incremental).
  3. Si és nova, la descarrega i la puja a landing/<carpeta>/<carpeta>_<data>.csv.

S'executa des del PC perquè el portal no respon a les IP d'AWS, on corre Databricks.
La connexió amb Databricks es llegeix de ~/.databrickscfg (fora del repo).
"""

import tempfile
import time
from datetime import datetime
from pathlib import Path

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

API_REUS = "https://opendata.reus.cat/api/3/action/package_show"
VOLUME_LANDING = "/Volumes/workspace/reus360/landing"

# Carpeta a landing → identificador del dataset al portal
DATASETS = {
    "atencio": "gestio-a-les-oficines-d-atencio-de-l-ajuntament-de-reus",
    "cita_previa": "cita-previa-a-les-oficines-d-atencio-de-l-ajuntament-de-reus",
    "queixes": "queixes",
}

# El portal té un límit de peticions molt estricte (respon 429), així que espaiem
# les peticions i, si en rebem un, esperem cada cop més abans de reintentar.
PAUSA_ENTRE_PETICIONS = 60  # segons
ESPERES_REINTENT = [60, 120, 240]  # segons


def missatge(text):
    print(f"[{datetime.now():%H:%M:%S}] {text}", flush=True)


def peticio_amb_reintents(url, **opcions):
    """Fa un GET i, si rep un 429 o un timeout, espera i ho torna a provar."""
    for intent, espera in enumerate([0] + ESPERES_REINTENT, start=1):
        if espera:
            missatge(f"  Esperant {espera} s abans de reintentar...")
            time.sleep(espera)
        try:
            resposta = requests.get(url, timeout=120, **opcions)
        except requests.Timeout:
            missatge(f"  Intent {intent}: el portal no ha respost a temps")
            continue
        if resposta.status_code == 429:
            missatge(f"  Intent {intent}: massa peticions (429)")
            continue
        resposta.raise_for_status()
        return resposta
    raise RuntimeError(f"El portal no ha respost després de {intent} intents: {url}")


def info_csv(id_dataset):
    """Retorna l'enllaç, la data de publicació i la mida del CSV del dataset."""
    resposta = peticio_amb_reintents(API_REUS, params={"id": id_dataset})
    recursos = resposta.json()["result"]["resources"]
    csv = next(r for r in recursos if r["format"].upper() == "CSV")
    data = csv["last_modified"][:10]  # "2026-09-30T22:45:02" → "2026-09-30"
    return csv["url"], data, csv.get("size")


def ja_es_a_landing(databricks, ruta):
    try:
        databricks.files.get_metadata(ruta)
        return True
    except NotFound:
        return False


def descarrega(url, fitxer_local, mida_esperada):
    resposta = peticio_amb_reintents(url, stream=True)
    with open(fitxer_local, "wb") as f:
        for tros in resposta.iter_content(chunk_size=1024 * 1024):
            f.write(tros)
    mida = fitxer_local.stat().st_size
    if mida_esperada and mida != mida_esperada:
        raise RuntimeError(f"Descàrrega incompleta: {mida} bytes de {mida_esperada}")
    return mida


def main():
    databricks = WorkspaceClient()
    missatge(f"Connectat a Databricks com a {databricks.current_user.me().user_name}")

    for num, (carpeta, id_dataset) in enumerate(DATASETS.items()):
        if num:
            missatge(f"Pausa de {PAUSA_ENTRE_PETICIONS} s per no saturar el portal...")
            time.sleep(PAUSA_ENTRE_PETICIONS)

        missatge(f"── {carpeta} ──")
        url, data, mida_esperada = info_csv(id_dataset)
        ruta = f"{VOLUME_LANDING}/{carpeta}/{carpeta}_{data}.csv"
        missatge(f"  Versió al portal: {data}")

        if ja_es_a_landing(databricks, ruta):
            missatge("  Ja és a landing, res a fer")
            continue

        time.sleep(PAUSA_ENTRE_PETICIONS)
        with tempfile.TemporaryDirectory() as carpeta_temp:
            fitxer_local = Path(carpeta_temp) / f"{carpeta}.csv"
            missatge("  Descarregant...")
            mida = descarrega(url, fitxer_local, mida_esperada)
            missatge(f"  Descarregat: {mida / 1_000_000:.1f} MB")

            databricks.files.create_directory(f"{VOLUME_LANDING}/{carpeta}")
            with open(fitxer_local, "rb") as f:
                databricks.files.upload(ruta, f, overwrite=False)
            missatge(f"  Pujat a {ruta}")

    missatge("Ingesta acabada")


if __name__ == "__main__":
    main()
