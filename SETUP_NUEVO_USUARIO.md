# Guía de configuración para un nuevo usuario (proyecto exportado en .zip)

Requisito: Databricks CLI **v1.11.0**.

## 1. Descomprimir el proyecto

```
Expand-Archive -Path template_databricks_asset_bundle.zip -DestinationPath C:\ruta\destino
cd C:\ruta\destino\template_databricks_asset_bundle
```

## 2. Eliminar artefactos no portables del zip original (si vienen incluidos)

```
Remove-Item -Recurse -Force .venv -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .databricks -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .pytest_cache -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .ruff_cache -ErrorAction SilentlyContinue
```

## 3. Instalar Databricks CLI v1.11.0

```
winget install Databricks.DatabricksCLI --version 1.11.0
```

Verificar versión:

```
databricks -v
```

Debe mostrar `Databricks CLI v1.11.0`.

## 4. Autenticarse contra el workspace (perfil `dev`)

El bundle usa el perfil `dev` apuntando a `https://adb-2339714903823198.18.azuredatabricks.net` (ver `databricks.yml`). Cada usuario debe crear este perfil en su propia máquina:

```
databricks auth login --host https://adb-2339714903823198.18.azuredatabricks.net --profile dev
```

Esto abre el navegador para autenticación OAuth y guarda las credenciales en `~/.databrickscfg`.

Verificar el perfil creado:

```
databricks auth env --profile dev
```

## 5. Crear entorno virtual de Python e instalar dependencias

```
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt -r requirements-dev.txt
```

## 6. Validar el bundle

```
databricks bundle validate -t dev
```

## 7. Desplegar el bundle al workspace

```
databricks bundle deploy -t dev
```

## 8. (Opcional) Ejecutar un job del bundle

```
databricks bundle run -t dev <nombre_del_job>
```

Listar los jobs disponibles en el bundle:

```
databricks bundle validate -t dev --output json
```
