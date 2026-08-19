import argparse
import json
from pathlib import Path
from typing import Any

import yaml


def ask_list(label: str) -> list[str]:
    """Solicita valores de una lista hasta recibir una línea vacía."""
    print(f"{label}. Deja una línea vacía para terminar.")
    values: list[str] = []
    while True:
        value = input("> ").strip()
        if not value:
            return values
        values.append(value)


def build_genie_json(sources: list[str], questions: list[str]) -> dict[str, Any]:
    """Construye un Genie Space base con fuentes y preguntas de negocio."""
    return {
        "version": 2,
        "benchmarks": {"questions": [{"question": [question]} for question in questions]},
        "data_sources": {
            "tables": [
                {"identifier": source, "column_configs": []} for source in sources
            ]
        },
        "instructions": {},
    }


def build_resource_yaml(title: str, warehouse_id: str, json_name: str) -> dict[str, Any]:
    """Construye el recurso YAML que referencia el JSON del Genie Space."""
    return {
        "resources": {
            "genie_spaces": {
                title: {
                    "title": title,
                    "warehouse_id": warehouse_id,
                    "file_path": f"../../src/genie_spaces/{json_name}",
                    "parent_path": "/Workspace/Users/${workspace.current_user.userName}/GenieSpaces",
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
        json.dump(build_genie_json(sources, questions), file, ensure_ascii=False, indent=2)
        file.write("\n")
    with yaml_file.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            build_resource_yaml(title, warehouse_id, json_name),
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
    if not sources or not questions:
        raise ValueError("Debe indicar al menos una fuente y una pregunta")
    create_files(args.title, sources, questions, args.warehouse_id, args.project_root)
    print("Genie Space base, YAML y config creados correctamente.")


if __name__ == "__main__":
    main()
