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

- Python `>=3.10,<3.13` instalado y disponible en el PATH (`python --version`).
- Databricks CLI **v1** (`databricks --version` debe mostrar `v1.x`; si muestra `v2.x`, hay que instalar la v1).
- Cuenta de Databricks con acceso al workspace declarado en [databricks.yml](databricks.yml) (host `https://adb-2339714903823198.18.azuredatabricks.net` para el target `dev`).
- Permisos en ese workspace para: crear/editar/evaluar Genie Spaces, leer las tablas listadas en `sources` de [pipeline_config.yml](pipeline_config.yml), y usar el SQL Warehouse indicado en `warehouse_id`.

### 2.1) Cómo se manejan las dependencias: `requirements.txt`

Este proyecto instala dependencias con el flujo estándar de Python: `python -m venv` + `pip install -r requirements.txt`. Hay dos archivos:

- **[requirements.txt](requirements.txt)**: dependencias de **ejecución** del pipeline (lo mínimo para correr `ejecutar_pipeline_genie.py` y afines).
- **[requirements-dev.txt](requirements-dev.txt)**: dependencias de **desarrollo** (tests, lint, notebooks, Spark local). Incluye automáticamente `requirements.txt` (tiene `-r requirements.txt` en la primera línea), así que instalar el de dev ya trae todo.

**Todas las versiones están fijadas de forma exacta (`paquete==x.y.z`)**, incluyendo dependencias transitivas — no solo las librerías que se importan directamente, sino también las que ellas mismas necesitan. Esto es intencional: `pip install -r requirements.txt` no valida ni bloquea versiones por sí solo (a diferencia de un lockfile), así que la única forma de garantizar que todo el equipo instale exactamente lo mismo es dejar cada versión escrita explícitamente en el archivo. **Nunca instales una librería del proyecto sin `==versión`** (ni edites estos archivos para quitar el pin) — hacerlo reintroduce el problema de que cada persona resuelva versiones distintas.

**Para qué sirve cada paquete:**

| Paquete | Archivo | Para qué se usa en este proyecto |
|---|---|---|
| `PyYAML` | `requirements.txt` | Leer/escribir los YAML del proyecto (`databricks.yml`, `pipeline_config.yml`, `resources/genie_spaces/*.yml`). |
| `databricks-sdk` | `requirements.txt` | Hablar con la API de Databricks desde Python (crear/leer Genie Spaces, ejecutar SQL Statements para las Metric Views, etc.), usado en `utils/refactorizar_genie.py` y otros scripts. |
| `google-auth`, `protobuf`, `requests`, `urllib3`, `cryptography`, `pyasn1`, `pyasn1-modules`, `cffi`, `pycparser`, `certifi`, `charset-normalizer`, `idna` | `requirements.txt` | Dependencias internas de `databricks-sdk` (autenticación, llamadas HTTP, TLS). No se importan directamente, pero deben instalarse en la versión exacta para que `databricks-sdk` funcione igual en todos los equipos. |
| `pytest` | `requirements-dev.txt` | Ejecutar la suite de pruebas unitarias (`tests/`) que valida la lógica del pipeline sin necesidad de conectarse a Databricks. |
| `ruff` | `requirements-dev.txt` | Linter/formateador de código Python; mantiene el estilo del proyecto consistente. |
| `databricks-connect` | `requirements-dev.txt` | Permite ejecutar código Spark localmente contra un cluster remoto de Databricks (usado por el fixture `spark` en los tests). |
| `ipykernel` | `requirements-dev.txt` | Permite abrir y ejecutar los notebooks (`.ipynb`) del proyecto (por ejemplo `src/notebooks/genie_assessment.ipynb`) desde un IDE local (VS Code, Jupyter). |
| `numpy`, `pandas`, `pyarrow`, `py4j`, `grpcio`, `grpcio-status`, `googleapis-common-protos`, `python-dateutil`, `pytz`, `tzdata`, `six`, `zstandard`, `setuptools`, y el resto de paquetes de `requirements-dev.txt` | `requirements-dev.txt` | Dependencias internas de `databricks-connect`, `pytest` e `ipykernel` (Spark local, ejecución de tests, notebooks/Jupyter). |

> `databricks-dlt` estaba en la configuración original pero no se pudo fijar su versión porque no está instalado en el entorno de referencia. Si tu caso de uso usa pipelines DLT, instálalo aparte y agrega su versión exacta a `requirements-dev.txt` (ver nota al final de ese archivo).

### 2.2) Regla obligatoria: todos deben instalar exactamente las mismas versiones

- **Nunca** corras `pip install <paquete>` suelto (sin `-r requirements.txt`) para agregar o actualizar algo — eso instala la versión más nueva disponible ese día, distinta a la de tus compañeros.
- Si el proyecto necesita una librería nueva o una versión distinta, quien haga el cambio debe: agregarla a `requirements.txt` o `requirements-dev.txt` **con su versión exacta**, probarlo, y commitear el archivo actualizado para que todo el equipo lo reciba igual.
- Si alguien del equipo reporta un error de dependencias distinto al tuyo, lo primero que hay que verificar es que ambos tengan exactamente el mismo `requirements.txt` / `requirements-dev.txt` (mismo commit) y que instalaron con `pip install -r ...`, no paquete por paquete.

---

## 3) Pasos desde ZIP (otro usuario)

1. Descomprimir el ZIP en una carpeta local (o clonar el repo).

2. Abrir una terminal en la raíz del proyecto (donde está `pyproject.toml`).

3. Crear el entorno virtual e instalar dependencias **exactamente como están fijadas** en `requirements.txt`/`requirements-dev.txt`:

   ```bash
   python -m venv .venv
   ```

   Activar el entorno virtual:

   ```bash
   # Windows (PowerShell / Git Bash)
   .venv\Scripts\activate

   # macOS / Linux
   source .venv/bin/activate
   ```

   Instalar dependencias (incluye las de desarrollo, ya que trae `requirements.txt` por dentro):

   ```bash
   pip install -r requirements-dev.txt
   ```

   Si solo necesitas ejecutar el pipeline (sin correr tests, lint o notebooks), basta con:

   ```bash
   pip install -r requirements.txt
   ```

4. **Verificación rápida del entorno** (opcional pero recomendado, no requiere acceso a Databricks): corre la suite de pruebas unitarias del pipeline.

   ```bash
   python -m pytest tests/test_ejecutar_pipeline_genie.py tests/test_ejecutar_benchmarks.py tests/test_refactorizar_genie.py
   ```

   Todas deben pasar. Si falla por un `ModuleNotFoundError`, revisa que el paso 3 haya terminado sin errores (y que el entorno virtual esté activado).

5. **Antes de autenticarte**, revisa que no tengas ya en tu equipo otro perfil apuntando al mismo host del proyecto. Esto es la causa más común de fallos de conexión al importar el proyecto en un equipo nuevo.

   ```bash
   databricks auth profiles
   ```

   - El comando anterior lista todos los perfiles guardados en tu `~/.databrickscfg` (o `%USERPROFILE%\.databrickscfg` en Windows) junto a su `Host`.
   - Si **algún otro perfil** (con nombre distinto a `dev`) ya muestra el host `https://adb-2339714903823198.18.azuredatabricks.net`, el CLI no podrá decidir cuál usar y el bundle fallará con el error `multiple profiles matched`.
   - En ese caso, renombra o elimina el perfil duplicado (edita `~/.databrickscfg` a mano) antes de continuar, o asegúrate de que el `[dev]` sea el único que apunte a ese host.

6. Crear/actualizar tu perfil `dev` autenticándote contra el **host exacto** que exige el proyecto (declarado en [databricks.yml](databricks.yml), target `dev`). El nombre del perfil debe ser literalmente `dev` porque `databricks.yml` lo referencia de forma explícita (`workspace.profile: dev`) — así el bundle nunca depende de qué usuario de Windows/Mac lo ejecute, solo de que exista un perfil local llamado `dev` autenticado contra el host correcto:

   ```bash
   databricks auth login --profile dev --host https://adb-2339714903823198.18.azuredatabricks.net
   ```

   Esto abre el navegador para iniciar sesión con **tu propia cuenta de Databricks** (no con la del equipo/persona que compartió el ZIP). Si el navegador abre una sesión SSO de otra persona, cierra esa sesión (o usa una ventana de incógnito) y repite el comando.

7. Verificar que el perfil quedó activo y sin ambigüedad:

   ```bash
   databricks auth profiles
   databricks bundle validate -t dev
   ```

   - `auth profiles` debe listar el perfil `dev` como `VALID`.
   - `bundle validate -t dev` debe imprimir, sin errores, el `Host` esperado y el `User` que corresponde a **tu propia cuenta**, no a la de otra persona. Si el `User` mostrado no es el tuyo, repite el paso 6 tras cerrar sesión en el navegador.

8. Ajustar [pipeline_config.yml](pipeline_config.yml) con las preguntas de negocio, fuentes, warehouse y demás valores propios de tu caso de uso (ver sección 4). Confirma que tienes permisos de lectura sobre **todas** las tablas listadas en `sources`.

9. Ejecutar el pipeline:

   ```bash
   python utils/ejecutar_pipeline_genie.py --config pipeline_config.yml
   ```

   (con el entorno virtual del paso 3 activado)

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
- **Error `multiple profiles matched: dev, <otro>` (o `cannot resolve bundle auth configuration`)**: tienes dos o más perfiles en `~/.databrickscfg` apuntando al mismo host. Elimina o renombra el perfil sobrante para que solo `dev` apunte al host del proyecto (ver paso 5 de la sección 3). No soluciones esto con `DATABRICKS_CONFIG_PROFILE` de forma permanente: el bundle debe funcionar solo con el perfil `dev`, sin variables de entorno extra, para que sea igual en cualquier equipo.
- **`databricks bundle validate -t dev` muestra un `User` que no es el tuyo**: el login abrió una sesión SSO de otra cuenta en el navegador. Cierra esa sesión (o usa incógnito) y repite `databricks auth login --profile dev --host <host>` (paso 6 de la sección 3). El proyecto nunca "quema" un usuario: el `host` en [databricks.yml](databricks.yml) es del workspace (no de una persona) y cada quien debe autenticar el perfil `dev` con su propia identidad.
- **No encuentra Genie correcto**: validar que `existing_id` corresponda al space correcto en UI.
- **No pasa benchmark**: revisar el JSON de resultados y el `benchmark_threshold`.
- **Permisos insuficientes**: el usuario debe poder leer/editar/evaluar espacios Genie.
- **Error `Unknown field 'measures'`**: el pipeline ahora elimina automáticamente `instructions.measures` y `instructions.sql_snippets.measures` antes del deploy para compatibilidad con APIs que no soportan esos bloques.
- **`ModuleNotFoundError` al ejecutar scripts o tests**: normalmente falta activar el entorno virtual o correr `pip install -r requirements-dev.txt` (paso 3 de la sección 3).
- **Alguien del equipo tiene versiones de dependencias distintas a las mías**: verifica que ambos tengan exactamente el mismo `requirements.txt`/`requirements-dev.txt` (mismo commit) y que instalaron con `pip install -r requirements-dev.txt`, no instalando paquetes sueltos sin versión.

---

## 8) Recomendación para compartir ZIP

Cuando compartas este proyecto, incluye siempre:

- [pipeline_config.yml](pipeline_config.yml) como plantilla comentada.
- Este [README.md](README.md).
- Instrucción explícita del `profile` y `host` que debe usar el nuevo usuario.
- [requirements.txt](requirements.txt) y [requirements-dev.txt](requirements-dev.txt), para que todos instalen exactamente las mismas versiones (ver sección 2).
