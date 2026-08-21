"""Crea las definiciones YAML, JSON y config de un Genie Space nuevo."""

import argparse
import json
import re
from pathlib import Path
from typing import Any

import yaml


SOURCE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
TITLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ask_list(label: str) -> list[str]:
    """Solicita valores de una lista hasta recibir una línea vacía."""
    print(f"{label}. Deja una línea vacía para terminar.")
    values: list[str] = []
    while True:
        value = input("> ").strip()
        if not value:
            return values
        values.append(value)


def validate_inputs(title: str, sources: list[str], questions: list[str], warehouse_id: str) -> None:
    """Valida los datos necesarios antes de crear las definiciones del Genie."""
    if not warehouse_id.strip():
        raise ValueError("warehouse_id es obligatorio")
    if not TITLE_PATTERN.fullmatch(title):
        raise ValueError("title solo puede contener letras, números y guion bajo")
    if not sources:
        raise ValueError("Debe indicar al menos una fuente")
    if not questions:
        raise ValueError("Debe indicar al menos una pregunta")
    invalid_sources = [source for source in sources if not SOURCE_PATTERN.fullmatch(source)]
    if invalid_sources:
        raise ValueError(
            "Fuentes inválidas; use el formato catalog.schema.table: "
            + ", ".join(invalid_sources)
        )


def build_genie_json(sources: list[str]) -> dict[str, Any]:
    """Construye un Genie Space base con fuentes y preguntas de negocio."""
    genie_json = {
        "version": 2,
        "data_sources": {
            "tables": [
                {"identifier": source, "column_configs": []}
                for source in sources
            ]
        },
        "instructions": {},
    }
    genie_json["data_sources"]["tables"].sort(
        key=lambda table: table["identifier"]
    )
    return genie_json


def build_resource_yaml(title: str, json_name: str) -> dict[str, Any]:
    """Construye el recurso YAML que referencia el JSON del Genie Space."""
    return {
        "resources": {
            "genie_spaces": {
                title: {
                    "title": title,
                    "warehouse_id": "${var.assessment_warehouse_id}",
                    "file_path": f"../../src/genie_spaces/{json_name}",
                    "parent_path": "${var.genie_parent_path}",
                }
            }
        }
    }


def build_config(sources: list[str], questions: list[str], warehouse_id: str) -> dict[str, Any]:
    """Construye el config usado por el Job de assessment."""
    catalogs = list(dict.fromkeys(source.split(".")[0] for source in sources))
    schemas = list(dict.fromkeys(source.split(".")[1] for source in sources))
    return {
        "catalogs": catalogs,
        "schemas": schemas,
        "tables": sources,
        "business_questions": questions,
        "warehouse_id": warehouse_id,
        "llm_endpoint": "databricks-claude-sonnet-4-6",
        "governance_transcript_path": None,
    }


def create_files(
    title: str,
    sources: list[str],
    questions: list[str],
    warehouse_id: str,
    project_root: Path,
) -> tuple[Path, Path, Path]:
    """Crea JSON, YAML y config local para un Genie Space nuevo."""
    json_name = f"{title}.geniespace.json"
    yaml_name = f"{title}.genie_space.yml"
    json_file = project_root / "src" / "genie_spaces" / json_name
    yaml_file = project_root / "resources" / "genie_spaces" / yaml_name
    config_file = project_root / "genie_assessment" / "temp" / "config.json"

    json_file.parent.mkdir(parents=True, exist_ok=True)
    yaml_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.parent.mkdir(parents=True, exist_ok=True)

    with json_file.open("w", encoding="utf-8") as file:
        json.dump(build_genie_json(sources), file, ensure_ascii=False, indent=2)
        file.write("\n")
    with yaml_file.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            build_resource_yaml(title, json_name),
            file,
            allow_unicode=True,
            sort_keys=False,
        )
    with config_file.open("w", encoding="utf-8") as file:
        json.dump(build_config(sources, questions, warehouse_id), file, ensure_ascii=False, indent=2)
        file.write("\n")

    return yaml_file, json_file, config_file


def main() -> None:
    """Solicita datos y crea los archivos base de un Genie Space nuevo."""
    parser = argparse.ArgumentParser(description="Crea un Genie Space desde entradas de consola.")
    parser.add_argument("--title", default="genie_space_manual")
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    args = parser.parse_args()

    sources = ask_list("Fuentes catalog.schema.table")
    questions = ask_list("Preguntas de negocio")
    validate_inputs(args.title, sources, questions, args.warehouse_id)
    create_files(args.title, sources, questions, args.warehouse_id, args.project_root)
    print("Genie Space base, YAML y config creados correctamente.")


if __name__ == "__main__":
    main()
