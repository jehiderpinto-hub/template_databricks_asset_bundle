# template_databricks_asset_bundle

Guía rápida en español para ejecutar este proyecto **desde local** (incluyendo cuando se comparte como ZIP).

---

## 1) ¿Qué hace este repositorio?

Este proyecto contiene un pipeline para:

1. Importar o crear un Genie Space.
2. (Opcional) Ejecutar validación/assessment y refactorización.
3. Ejecutar benchmarks.
4. Desplegar (o bloquear despliegue) según el umbral configurado.

Archivo principal del flujo:

- [utils/ejecutar_pipeline_genie.py](utils/ejecutar_pipeline_genie.py)

Archivo de configuración del pipeline:

- [pipeline_config.yml](pipeline_config.yml)

---

## 2) Requisitos para ejecutar en otro equipo

Antes de correr el pipeline, el usuario debe tener:

- Python `>=3.10,<3.13`
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Databricks CLI v1 (`databricks --version`)
- Acceso al workspace de Databricks (host/permisos)
- Permisos para crear/editar/evaluar Genie Spaces

---

## 3) Pasos desde ZIP (otro usuario)

1. Descomprimir el ZIP en una carpeta local.
2. Abrir terminal en la raíz del proyecto.
3. Instalar dependencias:

```bash
uv sync --dev
```

4. Autenticarse en Databricks (ejemplo con perfil `dev`):

```bash
databricks auth login --profile dev --host <TU_HOST_DATABRICKS>
```

5. Verificar que el perfil está válido:

```bash
databricks auth profiles
```

6. Ajustar [pipeline_config.yml](pipeline_config.yml).

7. Ejecutar:

```bash
uv run python utils/ejecutar_pipeline_genie.py --config pipeline_config.yml
```

---

## 4) Cómo configurar `pipeline_config.yml`

Usa la plantilla comentada en [pipeline_config.yml](pipeline_config.yml).

Reglas clave:

- `business_questions`: **obligatorio siempre**.
- Si `existing_id` está definido: usa Genie existente.
- Si `existing_id` está vacío: crea Genie nuevo y `sources` + `warehouse_id` son obligatorios.
- `run_validate=false` (o `run_validation=false`): omite validación/assessment/refactor.
- Benchmarks:
  - Siempre se ejecutan.
  - En Genie existente se suman benchmarks importados + benchmarks del config.
  - En Genie nuevo deben venir en el config.
  - En el YAML usa el formato completo de Genie: `benchmarks.questions`.
- Las claves de `instructions` definidas en el YAML sustituyen la misma clave del JSON importado.
- Las metric views propuestas se consolidan en un solo YAML y se crean en el destino `catalog.schema` definido en `metric_view_destination`.
- Si el benchmark falla, también se eliminan las metric views creadas durante la refactorización.
- El archivo local consolidado se guarda como [genie_assessment/temp/assessment_outputs/genie_proposed_metric_view.yml](genie_assessment/temp/assessment_outputs/genie_proposed_metric_view.yml).
- Agrega en el YAML una clave `metric_view_destination: "catalog.schema"` para crear las metric views.
- Agrega `revert_on_failed_benchmark: true|false` para decidir si el deploy se revierte cuando el benchmark no pasa.
- Los artefactos de `assessment_outputs` se limpian al inicio de cada ejecución y luego se regeneran; el paso de benchmarks no los borra al final.
- IDs vacíos (`id: ""`) en bloques seteables se autogeneran durante el pipeline.

---

## 5) Casos de ejecución

### A. Genie existente

- El pipeline importa definición desde `existing_id`.
- Usa `sources` desde la definición existente.
- Requiere `business_questions` en config.
- Hace deploy, ejecuta benchmark remoto y, si no supera umbral, **restaura el estado remoto previo**.

### B. Genie nuevo

- Requiere `sources`, `warehouse_id`, `business_questions` y `benchmarks`.
- Hace deploy y luego benchmark remoto.
- Si benchmark no supera umbral: **revierte el deploy enviando el Genie a papelera**.

---

## 6) Salidas esperadas

Resultados intermedios (assessment/reportes) quedan bajo:

- [genie_assessment/temp/](genie_assessment/temp)

Reporte de benchmark:

- [genie_assessment/temp/assessment_outputs/genie_benchmark_results.json](genie_assessment/temp/assessment_outputs/genie_benchmark_results.json)

---

## 7) Problemas comunes

- **No autentica Databricks**: revisar `databricks auth profiles` y el `profile` usado en config.
- **No encuentra Genie correcto**: validar que `existing_id` corresponda al space correcto en UI.
- **No pasa benchmark**: revisar el JSON de resultados y el `benchmark_threshold`.
- **Permisos insuficientes**: el usuario debe poder leer/editar/evaluar espacios Genie.
- **Error `Unknown field 'measures'`**: el pipeline ahora elimina automáticamente `instructions.measures` y `instructions.sql_snippets.measures` antes del deploy para compatibilidad con APIs que no soportan esos bloques.

---

## 8) Recomendación para compartir ZIP

Cuando compartas este proyecto, incluye siempre:

- [pipeline_config.yml](pipeline_config.yml) como plantilla comentada.
- Este [README.md](README.md).
- Instrucción explícita del `profile` y `host` que debe usar el nuevo usuario.
