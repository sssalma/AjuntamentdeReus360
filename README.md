# AjuntamentdeReus360

Projecte de Data & Analytics sobre l'**atenció ciutadana de l'Ajuntament de Reus**, construït
amb dades obertes reals: pipeline ETL amb càrrega incremental mensual, model dimensional i
quadre de comandament per a gestors.

> 🚧 En construcció.

## Objectiu

Donar a l'Ajuntament una visió 360º de com atén la ciutadania:

- Temps mitjà d'espera i d'atenció per oficina, tràmit i franja horària.
- Percentatge de persones que demanen cita prèvia i no s'hi presenten.
- Hores punta i evolució de la demanda des del 2018 (inclòs l'efecte COVID).
- Queixes per barri, canal i tipologia.

## Fonts de dades

Portal [Open Data Reus](https://opendata.reus.cat) (llicència CC BY), actualització mensual:

| Dataset | Files | Període |
|---|---|---|
| [Gestió a les oficines d'atenció](https://opendata.reus.cat/dataset/gestio-a-les-oficines-d-atencio-de-l-ajuntament-de-reus) | ~377.000 | oct. 2018 – actualitat |
| [Cita prèvia](https://opendata.reus.cat/dataset/cita-previa-a-les-oficines-d-atencio-de-l-ajuntament-de-reus) | ~298.000 | nov. 2018 – actualitat |
| [Queixes](https://opendata.reus.cat/dataset/queixes) | ~15.000 | 2018 – actualitat |

Les files ja vénen **agregades** (cadascuna porta un comptador de tiquets o queixes), així que
els KPI es calculen sumant aquest comptador i ponderant les mitjanes, mai comptant files.

## Arquitectura

| Capa | Eina |
|---|---|
| Ingesta (API CKAN del portal) i ETL | Databricks · PySpark · SQL |
| Organització de l'ETL | Medallion: bronze → silver → gold |
| Actualització | Job mensual de Databricks |
| Quadre de comandament | Power BI |

## Model de dades

Esquema en **galàxia**: tres taules de fets que comparteixen dimensions.

- **Fets:** `fct_atencio`, `fct_cita_previa`, `fct_queixes`
- **Dimensions compartides:** `dim_data`, `dim_sexe` (les tres); `dim_oficina`, `dim_cua`,
  `dim_tramit`, `dim_franja` (atenció i cita prèvia)
- **Dimensions de queixes:** `dim_barri`, `dim_canal`, `dim_tipus_queixa`

### Decisions de neteja

- **Sexe:** es mantenen separats «Desconegut» (el sistema no l'ha recollit) i «No informat»
  (pot ser una opció triada en un formulari), perquè poden voler dir coses diferents.

## Autora

Salma Jadiani Mahmouh · [github.com/sssalma](https://github.com/sssalma)
