"""Crea una Metric View en Unity Catalog y actualiza el JSON del Genie."""

import argparse
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from comun import read_yaml_file
import yaml


MANIFEST_FILE_NAME = "genie_metric_views_manifest.json"

# Valores válidos de `format` según com.databricks.sql.serde.v11.ColumnFormat
VALID_COLUMN_FORMATS = {"byte", "currency", "date", "date_time", "number", "percentage"}
COLUMN_FORMAT_ALIASES = {
    "percent": "percentage",
    "pct": "percentage",
    "%": "percentage",
}


def _parse_destination(destination: str) -> tuple[str, str]:
    parts = [part.strip() for part in destination.split(".") if part.strip()]
    if len(parts) != 2:
        raise ValueError("metric_view_destination debe tener formato catalog.schema")
    return parts[0], parts[1]


def _sanitize_metric_view_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_").lower()
    if not sanitized:
        sanitized = "genie_assessment"
    if not sanitized.startswith("mv_"):
        sanitized = f"mv_{sanitized}"
    return sanitized


def _sanitize_identifier(value: str, default: str = "item") -> str:
    """Convierte un identificador potencialmente no ASCII en uno compatible."""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", ascii_value).strip("_").lower()
    if not sanitized:
        sanitized = default
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def build_metric_view_name(base_name: str | Path | None, proposal: dict[str, Any]) -> str:
    """Genera un nombre estable para una metric view propuesta."""
    candidate = ""
    for key in ("metric_view_name", "name", "display_name", "alias"):
        value = proposal.get(key)
        if isinstance(value, str) and value.strip():
            candidate = value.strip()
            break
    if not candidate:
        source = proposal.get("source")
        if isinstance(source, str) and source.strip():
            candidate = source.strip().split(".")[-1]
    if not candidate and base_name is not None:
        candidate = Path(base_name).stem if isinstance(base_name, (str, Path)) else str(base_name)
    if not candidate:
        candidate = "genie_assessment"
    return _sanitize_metric_view_name(candidate)


def _load_metric_view_proposals(metric_view_yaml: Path) -> list[dict[str, Any]]:
    raw = read_yaml_file(metric_view_yaml)
    if isinstance(raw, dict) and isinstance(raw.get("metric_views"), list):
        proposals = raw["metric_views"]
    elif isinstance(raw, list):
        proposals = raw
    elif isinstance(raw, dict):
        proposals = [raw]
    else:
        raise ValueError(
            "El YAML de metric views debe ser un objeto, una lista o contener metric_views"
        )

    normalized: list[dict[str, Any]] = []
    for index, proposal in enumerate(proposals):
        if not isinstance(proposal, dict):
            raise ValueError(f"metric_views[{index}] debe ser un objeto")
        source = proposal.get("source")
        if not isinstance(source, str) or not source.strip():
            raise ValueError(f"metric_views[{index}].source debe ser texto no vacío")
        if len(source.split(".")) != 3:
            raise ValueError(
                f"metric_views[{index}].source debe tener formato catalog.schema.table"
            )
        normalized.append(proposal)
    if not normalized:
        raise ValueError("El YAML no contiene metric views propuestas")
    return normalized


def _sanitize_metric_view_payload(proposal: dict[str, Any]) -> dict[str, Any]:
    """Limpia nombres no ASCII en la definición de metric view."""
    sanitized = json.loads(json.dumps(proposal, ensure_ascii=False))
    for collection_name in ("dimensions", "measures", "joins", "filters", "sql_functions"):
        collection = sanitized.get(collection_name)
        if not isinstance(collection, list):
            continue
        used_names: set[str] = set()
        for item in collection:
            if not isinstance(item, dict):
                continue
            candidate_source = item.get("name")
            if not isinstance(candidate_source, str) or not candidate_source.strip():
                candidate_source = item.get("alias")
            if not isinstance(candidate_source, str) or not candidate_source.strip():
                candidate_source = item.get("display_name")
            if not isinstance(candidate_source, str) or not candidate_source.strip():
                continue
            sanitized_name = _sanitize_identifier(candidate_source)
            base_name = sanitized_name
            suffix = 2
            while sanitized_name in used_names:
                sanitized_name = f"{base_name}_{suffix}"
                suffix += 1
            used_names.add(sanitized_name)
            item["name"] = sanitized_name
            if isinstance(item.get("alias"), str) and item["alias"].strip():
                item["alias"] = _sanitize_identifier(str(item["alias"]))

            format_value = item.get("format")
            if isinstance(format_value, str):
                normalized_format = COLUMN_FORMAT_ALIASES.get(
                    format_value.strip().lower(), format_value.strip().lower()
                )
                if normalized_format in VALID_COLUMN_FORMATS:
                    item["format"] = normalized_format
                else:
                    del item["format"]

    return sanitized


def create_metric_view(
    proposal: dict[str, Any],
    metric_view_name: str,
    destination: str,
    warehouse_id: str,
    profile: str,
) -> str:
    """Crea o reemplaza una Metric View mediante SQL Statements API."""
    catalog, schema = _parse_destination(destination)
    qualified_name = ".".join([catalog, schema, metric_view_name])
    sanitized_proposal = _sanitize_metric_view_payload(proposal)
    yaml_content = yaml.safe_dump(sanitized_proposal, allow_unicode=True, sort_keys=False)
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


def create_metric_views(
    metric_view_yaml: Path,
    destination: str,
    warehouse_id: str,
    profile: str,
    base_name: str | Path | None = None,
) -> list[str]:
    """Crea una o varias Metric Views desde un YAML consolidado."""
    proposals = _load_metric_view_proposals(metric_view_yaml)
    created_identifiers: list[str] = []
    used_names: set[str] = set()
    for proposal in proposals:
        metric_view_name = build_metric_view_name(base_name, proposal)
        candidate_name = metric_view_name
        suffix = 2
        while candidate_name in used_names:
            candidate_name = f"{metric_view_name}_{suffix}"
            suffix += 1
        used_names.add(candidate_name)
        created_identifiers.append(
            create_metric_view(
                proposal,
                candidate_name,
                destination,
                warehouse_id,
                profile,
            )
        )
    return created_identifiers


def update_genie_json(json_file: Path, metric_view_identifiers: list[str]) -> None:
    """Actualiza el JSON del Genie para referenciar las Metric Views creadas."""
    with json_file.open(encoding="utf-8") as file:
        genie_space = json.load(file)

    tables = genie_space.setdefault("data_sources", {}).setdefault("tables", [])
    mv_indexes = [
        index
        for index, table in enumerate(tables)
        if str(table.get("identifier", "")).split(".")[-1].startswith("mv_")
    ]

    for index, metric_view_identifier in enumerate(metric_view_identifiers):
        if index < len(mv_indexes):
            tables[mv_indexes[index]]["identifier"] = metric_view_identifier
        else:
            tables.append({"identifier": metric_view_identifier, "column_configs": []})

    if len(mv_indexes) > len(metric_view_identifiers):
        for index in reversed(mv_indexes[len(metric_view_identifiers) :]):
            del tables[index]

    tables.sort(key=lambda table: table.get("identifier", ""))

    with json_file.open("w", encoding="utf-8") as file:
        json.dump(genie_space, file, ensure_ascii=False, indent=2)
        file.write("\n")


def write_manifest(output_directory: Path, identifiers: list[str], destination: str) -> Path:
    """Escribe un manifiesto con las Metric Views creadas para limpieza posterior."""
    output_directory.mkdir(parents=True, exist_ok=True)
    manifest_file = output_directory / MANIFEST_FILE_NAME
    with manifest_file.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "metric_view_identifiers": identifiers,
                "metric_view_destination": destination,
            },
            file,
            ensure_ascii=False,
            indent=2,
        )
        file.write("\n")
    return manifest_file


def main() -> None:
    """Parsea argumentos, crea la Metric View y actualiza el Genie JSON."""
    parser = argparse.ArgumentParser(
        description="Crea una Metric View y la referencia desde un Genie Space."
    )
    parser.add_argument("--metric-view-yaml", type=Path, required=True)
    parser.add_argument("--genie-json", type=Path, required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--metric-view-destination", required=True)
    parser.add_argument("--metric-view-base-name")
    parser.add_argument("--profile", default="dev")
    args = parser.parse_args()

    created_identifiers = create_metric_views(
        args.metric_view_yaml,
        args.metric_view_destination,
        args.warehouse_id,
        args.profile,
        base_name=args.metric_view_base_name or args.genie_json,
    )
    update_genie_json(args.genie_json, created_identifiers)
    manifest_file = write_manifest(
        Path("genie_assessment") / "temp" / "assessment_outputs",
        created_identifiers,
        args.metric_view_destination,
    )
    print(f"Metric Views creadas: {', '.join(created_identifiers)}")
    print(f"Manifiesto: {manifest_file}")
    print(f"Genie Space actualizado: {args.genie_json}")


if __name__ == "__main__":
    main()
