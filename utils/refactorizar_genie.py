"""Crea una Metric View en Unity Catalog y actualiza el JSON del Genie."""

import argparse
import json
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from comun import read_yaml_file


def create_metric_view(
    metric_view_yaml: Path,
    metric_view_name: str,
    warehouse_id: str,
    profile: str,
) -> str:
    """Crea o reemplaza una Metric View mediante SQL Statements API."""
    metric_view = read_yaml_file(metric_view_yaml)
    source = metric_view.get("source")
    if not source or len(source.split(".")) != 3:
        raise ValueError("El YAML debe contener source con formato catalog.schema.table")

    catalog, schema, _ = source.split(".")
    qualified_name = ".".join([catalog, schema, metric_view_name])
    yaml_content = metric_view_yaml.read_text(encoding="utf-8")
    escaped_yaml = yaml_content.replace("$$", "$$$$")
    statement = (
        f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{metric_view_name}` "
        f"WITH METRICS LANGUAGE YAML AS $$\n{escaped_yaml}\n$$"
    )

    client = WorkspaceClient(profile=profile)
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    if response.status and response.status.state and response.status.state.value != "SUCCEEDED":
        error = response.status.error.message if response.status.error else "estado desconocido"
        raise RuntimeError(f"No se pudo crear la Metric View: {error}")

    return qualified_name


def update_genie_json(json_file: Path, metric_view_identifier: str) -> None:
    """Actualiza el JSON del Genie para referenciar la Metric View creada."""
    with json_file.open(encoding="utf-8") as file:
        genie_space = json.load(file)

    tables = genie_space.setdefault("data_sources", {}).setdefault("tables", [])
    replaced = False
    for table in tables:
        identifier = table.get("identifier", "")
        if identifier.split(".")[-1].startswith("mv_"):
            table["identifier"] = metric_view_identifier
            replaced = True
            break

    if not replaced:
        tables.append({"identifier": metric_view_identifier, "column_configs": []})

    tables.sort(key=lambda table: table.get("identifier", ""))

    with json_file.open("w", encoding="utf-8") as file:
        json.dump(genie_space, file, ensure_ascii=False, indent=2)
        file.write("\n")


def main() -> None:
    """Parsea argumentos, crea la Metric View y actualiza el Genie JSON."""
    parser = argparse.ArgumentParser(
        description="Crea una Metric View y la referencia desde un Genie Space."
    )
    parser.add_argument("--metric-view-yaml", type=Path, required=True)
    parser.add_argument("--genie-json", type=Path, required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--metric-view-name", default="mv_genie_assessment")
    parser.add_argument("--profile", default="dev")
    args = parser.parse_args()

    identifier = create_metric_view(
        args.metric_view_yaml,
        args.metric_view_name,
        args.warehouse_id,
        args.profile,
    )
    update_genie_json(args.genie_json, identifier)
    print(f"Metric View creada: {identifier}")
    print(f"Genie Space actualizado: {args.genie_json}")


if __name__ == "__main__":
    main()
