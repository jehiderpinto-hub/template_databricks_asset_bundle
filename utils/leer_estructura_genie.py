"""Lee la definición de un Genie Space y construye su config de assessment."""

import argparse
import json
from pathlib import Path
from typing import Any

import yaml
from comun import read_json_file, read_yaml_file, validate_file


def get_genie_resource(yaml_structure: dict[str, Any]) -> dict[str, Any]:
    """Obtiene la primera definición de Genie Space del YAML del bundle."""
    resources = yaml_structure.get("resources", {})
    genie_spaces = resources.get("genie_spaces", {})

    if not genie_spaces:
        raise ValueError("El YAML no contiene resources.genie_spaces")

    return next(iter(genie_spaces.values()))


def get_source_identifiers(json_structure: dict[str, Any]) -> list[str]:
    """Extrae los identificadores de tablas declarados en el JSON del Genie."""
    tables = json_structure.get("data_sources", {}).get("tables", [])
    identifiers = [table.get("identifier") for table in tables]
    return [identifier for identifier in identifiers if identifier]


def build_config(
    yaml_structure: dict[str, Any],
    json_structure: dict[str, Any],
    business_questions: list[str],
) -> dict[str, Any]:
    """Construye el config de assessment a partir del YAML, JSON y preguntas."""
    resource = get_genie_resource(yaml_structure)
    table_identifiers = get_source_identifiers(json_structure)

    catalogs: list[str] = []
    schemas: list[str] = []
    for identifier in table_identifiers:
        parts = identifier.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"La fuente '{identifier}' no tiene el formato catalog.schema.table"
            )

        catalog, schema, _ = parts
        if catalog not in catalogs:
            catalogs.append(catalog)
        if schema not in schemas:
            schemas.append(schema)

    return {
        "catalogs": catalogs,
        "schemas": schemas,
        "tables": table_identifiers,
        "business_questions": business_questions,
        "warehouse_id": resource.get("warehouse_id", ""),
        "llm_endpoint": "databricks-claude-sonnet-4-6",
        "governance_transcript_path": None,
    }


def ask_business_questions() -> list[str]:
    """Solicita preguntas de negocio hasta recibir una línea vacía."""
    print("Escribe las preguntas de negocio. Deja una línea vacía para terminar.")
    questions: list[str] = []

    while True:
        question = input("Pregunta: ").strip()
        if not question:
            return questions
        questions.append(question)


def write_config(config: dict[str, Any]) -> Path:
    """Guarda el config en ``genie_assessment/temp/config.json``."""
    output_directory = Path("genie_assessment") / "temp"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_file = output_directory / "config.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(config, file, ensure_ascii=False, indent=2)
        file.write("\n")

    return output_file


def main() -> None:
    """Parsea argumentos, construye el config y lo guarda en disco."""
    parser = argparse.ArgumentParser(
        description="Lee y muestra la estructura de un YAML y un JSON de Genie Space."
    )
    parser.add_argument(
        "--yml",
        required=True,
        type=Path,
        help="Ruta al archivo de definición YAML del Genie Space.",
    )
    parser.add_argument(
        "--json",
        required=True,
        dest="json_file",
        type=Path,
        help="Ruta al archivo JSON serializado del Genie Space.",
    )
    args = parser.parse_args()

    validate_file(args.yml, ".yml")
    validate_file(args.json_file, ".json")

    yaml_structure = read_yaml_file(args.yml)
    json_structure = read_json_file(args.json_file)
    business_questions = ask_business_questions()
    config = build_config(yaml_structure, json_structure, business_questions)
    output_file = write_config(config)

    print(f"Configuración guardada en: {output_file}")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, json.JSONDecodeError, yaml.YAMLError) as error:
        raise SystemExit(f"Error leyendo la estructura: {error}") from error