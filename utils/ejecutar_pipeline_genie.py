"""Orquesta el flujo completo de generación, validación y despliegue de un Genie Space."""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from comun import clear_directory_contents, run_subprocess
from crear_genie_desde_entradas import create_files
from databricks.sdk import WorkspaceClient
from leer_estructura_genie import build_config as build_imported_genie_config
from leer_estructura_genie import write_config
from transaccion_proyecto import LocalProjectTransaction


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESOURCES_DIRECTORY = PROJECT_ROOT / "resources" / "genie_spaces"
SOURCE_DIRECTORY = PROJECT_ROOT / "src" / "genie_spaces"
MANAGED_DIRECTORIES = [
    RESOURCES_DIRECTORY,
    SOURCE_DIRECTORY,
]
METRIC_VIEW_OUTPUT_FILE = (
    PROJECT_ROOT / "genie_assessment" / "temp" / "assessment_outputs" / "genie_proposed_metric_view.yml"
)
METRIC_VIEW_MANIFEST_FILE = (
    PROJECT_ROOT
    / "genie_assessment"
    / "temp"
    / "assessment_outputs"
    / "genie_metric_views_manifest.json"
)
ASSESSMENT_OUTPUTS_DIRECTORY = PROJECT_ROOT / "genie_assessment" / "temp" / "assessment_outputs"


class PipelineConsole:
    """Presenta el avance del pipeline de forma consistente en consola."""

    width = 76

    def __init__(self) -> None:
        self.step_number = 0
        self.started_at = time.perf_counter()

    def line(self, character: str = "=") -> None:
        print(character * self.width)

    def start(self, args: argparse.Namespace, config_file: Path | None) -> None:
        self.line()
        print("PIPELINE GENIE")
        self.line()
        print(f"  Target:  {args.target}")
        print(f"  Perfil:  {args.profile}")
        print(f"  Origen:  {'Genie existente' if args.existing_id else 'Genie nuevo'}")
        if args.existing_id:
            print(f"  ID:      {args.existing_id}")
        if config_file:
            print(f"  Config:  {config_file}")
        self.line("-")

    def stage(self, description: str, command: list[str] | None = None) -> None:
        self.step_number += 1
        print(f"\n[ETAPA {self.step_number}] {description}")
        if command:
            print(f"  Comando: {' '.join(map(str, command))}")

    def success(self, detail: str | None = None) -> None:
        message = "  Resultado: OK"
        if detail:
            message += f" - {detail}"
        print(message)

    def skipped(self, reason: str) -> None:
        print(f"\n[OMITIDO] {reason}")

    def completed(self) -> None:
        elapsed = time.perf_counter() - self.started_at
        print()
        self.line()
        print(f"PIPELINE COMPLETADO CORRECTAMENTE en {elapsed:.1f} s")
        self.line()


CONSOLE = PipelineConsole()


def build_metric_view_name(genie_json: Path) -> str:
    """Genera un nombre de Metric View único y específico para cada Genie."""
    name = genie_json.stem
    if name.endswith(".geniespace"):
        name = name[: -len(".geniespace")]
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_").lower()
    if not sanitized:
        sanitized = "genie_assessment"
    if not sanitized.startswith("mv_"):
        sanitized = f"mv_{sanitized}"
    return sanitized


def run_command(command: list[str], description: str) -> None:
    """Ejecuta un comando desde la raíz del proyecto y falla si no termina bien."""
    CONSOLE.stage(description, command)
    result = run_subprocess(command, PROJECT_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"El comando terminó con código {result.returncode}")

    CONSOLE.success()


def get_files(directory: Path, pattern: str) -> set[Path]:
    """Devuelve los archivos que coinciden con un patrón en un directorio."""
    return set(directory.glob(pattern))


def find_generated_file(
    directory: Path,
    pattern: str,
    files_before_generation: set[Path],
) -> Path:
    """Selecciona el archivo nuevo más reciente o el existente más reciente."""
    files_after_generation = get_files(directory, pattern)
    new_files = files_after_generation - files_before_generation
    candidates = new_files or files_after_generation

    if not candidates:
        raise FileNotFoundError(
            f"No se encontró un archivo generado con el patrón '{pattern}' en {directory}"
        )

    return max(candidates, key=lambda file_path: file_path.stat().st_mtime)


def parse_arguments() -> argparse.Namespace:
    """Construye y devuelve los argumentos del pipeline principal."""
    parser = argparse.ArgumentParser(
        description="Genera, evalúa y recupera los resultados de un Genie Space."
    )
    parser.add_argument(
        "--existing-id",
        help="ID del Genie Space existente; si se omite, se crea desde consola.",
    )
    parser.add_argument(
        "--profile",
        default="dev",
        help="Perfil de Databricks utilizado por la CLI.",
    )
    parser.add_argument(
        "--target",
        default="dev",
        help="Target del bundle que se validará y cuyo Job se ejecutará.",
    )
    parser.add_argument(
        "--benchmark-threshold",
        type=float,
        default=0.8,
        help="Porcentaje mínimo de benchmarks buenos para permitir el deploy (0 a 1).",
    )
    parser.add_argument(
        "--warehouse-id",
        help="Warehouse usado al crear un Genie Space nuevo.",
    )
    parser.add_argument(
        "--title",
        default="genie_space_manual",
        help="Título del Genie Space creado cuando no se proporciona existing-id.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Archivo YAML declarativo para ejecutar el pipeline sin interacción.",
    )
    return parser.parse_args()


def load_pipeline_config(config_file: Path) -> dict:
    """Lee la configuración declarativa del pipeline."""
    with config_file.open(encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}
    if not isinstance(config, dict):
        raise ValueError("La configuración del pipeline debe ser un objeto YAML")
    return config


def apply_pipeline_config(args: argparse.Namespace, config: dict) -> None:
    """Aplica valores declarativos del YAML sobre los argumentos de ejecución."""
    for name in [
        "profile",
        "target",
        "existing_id",
        "title",
        "warehouse_id",
        "benchmark_threshold",
        "revert_on_failed_benchmark",
    ]:
        if name in config and config[name] is not None:
            setattr(args, name, config[name])


def get_config_questions(config: dict, require_non_empty: bool = False) -> list[str]:
    """Valida y devuelve las preguntas declaradas en el YAML."""
    questions = config.get("business_questions", [])
    if not isinstance(questions, list) or not all(isinstance(item, str) for item in questions):
        raise ValueError("business_questions debe ser una lista de textos")
    if require_non_empty and not questions:
        raise ValueError(
            "business_questions es obligatorio en pipeline_config.yml para ejecutar el pipeline"
        )
    return questions


def get_config_sources(config: dict) -> list[str]:
    """Valida y devuelve las fuentes declaradas en el YAML."""
    sources = config.get("sources", [])
    if not isinstance(sources, list) or not all(isinstance(item, str) for item in sources):
        raise ValueError("sources debe ser una lista de textos")
    return sources


def get_config_benchmarks(config: dict | None) -> list[dict[str, Any]]:
    """Valida y devuelve benchmarks declarados en el YAML de pipeline."""
    if not config:
        return []

    raw_benchmarks = config.get("benchmarks", [])
    if raw_benchmarks is None:
        return []

    benchmark_items: list[dict[str, Any]] = []
    if isinstance(raw_benchmarks, list):
        benchmark_items = raw_benchmarks
    elif isinstance(raw_benchmarks, dict):
        questions = raw_benchmarks.get("questions", [])
        if not isinstance(questions, list):
            raise ValueError("benchmarks.questions debe ser una lista")
        benchmark_items = questions
    else:
        raise ValueError("benchmarks debe ser una lista o un objeto con questions")

    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(benchmark_items):
        if not isinstance(item, dict):
            raise ValueError(f"benchmarks[{index}] debe ser un objeto")

        question_values = item.get("question")
        answer_values = item.get("answer")
        benchmark_id = item.get("id")
        evaluation_note = item.get("evaluation_note")

        if isinstance(question_values, list):
            question = question_values[0].strip() if question_values else ""
        elif isinstance(question_values, str):
            question = question_values.strip()
            question_values = [question]
        else:
            question = ""

        expected_sql = item.get("expected_sql") or item.get("sql") or item.get("answer_sql")
        normalized_answer_values: list[dict[str, Any]] = []
        if isinstance(answer_values, list):
            for answer in answer_values:
                if not isinstance(answer, dict):
                    normalized_answer_values.append(answer)
                    continue
                content = answer.get("content")
                if isinstance(content, str):
                    answer = {**answer, "content": [content]}
                normalized_answer_values.append(answer)
            if not expected_sql:
                for answer in normalized_answer_values:
                    if not isinstance(answer, dict):
                        continue
                    if answer.get("format") != "SQL":
                        continue
                    content = answer.get("content", [])
                    if isinstance(content, list):
                        expected_sql = "".join(content)
                    if expected_sql:
                        break

        if not isinstance(question, str) or not question.strip():
            raise ValueError(f"benchmarks[{index}].question debe ser texto o lista no vacía")
        if not isinstance(expected_sql, str) or not expected_sql.strip():
            raise ValueError(
                f"benchmarks[{index}] debe incluir answer SQL o expected_sql/sql/answer_sql"
            )

        benchmark: dict[str, Any] = {
            "question": question_values if isinstance(question_values, list) else [question.strip()],
            "answer": normalized_answer_values
            if normalized_answer_values
            else [
                {
                    "format": "SQL",
                    "content": [expected_sql.strip()],
                }
            ],
        }
        if isinstance(benchmark_id, str) and _is_valid_benchmark_id(benchmark_id.strip()):
            benchmark["id"] = benchmark_id.strip()
        if evaluation_note is not None:
            if isinstance(evaluation_note, list):
                note_text = str(evaluation_note[0]).strip() if evaluation_note else ""
            else:
                note_text = str(evaluation_note).strip()
            if note_text:
                benchmark["evaluation_note"] = [note_text]
        normalized.append(benchmark)

    return normalized


def _normalize_sql_text(sql_text: str) -> str:
    return re.sub(r"\s+", " ", sql_text.strip()).lower()


def _is_valid_benchmark_id(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{32}", value))


def _extract_benchmark_entry_fields(entry: dict[str, Any]) -> tuple[str, str]:
    question_values = entry.get("question", [])
    answer_values = entry.get("answer", [])
    if isinstance(question_values, list):
        question = question_values[0].strip() if question_values else ""
    elif isinstance(question_values, str):
        question = question_values.strip()
    else:
        question = ""
    expected_sql = ""
    for answer in answer_values:
        if not isinstance(answer, dict):
            continue
        if answer.get("format") == "SQL":
            content = answer.get("content", [])
            if isinstance(content, list):
                expected_sql = "".join(content).strip()
            elif isinstance(content, str):
                expected_sql = content.strip()
            if expected_sql:
                break
    return question, expected_sql


def _generate_benchmark_id() -> str:
    return uuid4().hex


def _autogenerate_empty_ids(node: Any) -> None:
    """Genera IDs para cualquier campo ``id`` vacío dentro del payload del Genie."""
    if isinstance(node, dict):
        if "id" in node:
            current_id = node.get("id")
            if not isinstance(current_id, str) or not current_id.strip():
                node["id"] = _generate_benchmark_id()
        for value in node.values():
            _autogenerate_empty_ids(value)
        return
    if isinstance(node, list):
        for item in node:
            _autogenerate_empty_ids(item)


def _merge_json_values(base_value: Any, override_value: Any) -> Any:
    """Hace merge recursivo: dict recursivo, listas por anexado, escalares reemplazo."""
    if isinstance(base_value, dict) and isinstance(override_value, dict):
        merged = dict(base_value)
        for key, value in override_value.items():
            if key in merged:
                merged[key] = _merge_json_values(merged[key], value)
            else:
                merged[key] = value
        return merged
    if isinstance(base_value, list) and isinstance(override_value, list):
        return [*base_value, *override_value]
    return override_value


def _merge_instruction_values(base_value: Any, override_value: Any) -> Any:
    """Fusiona instrucciones reemplazando cualquier clave definida en el override."""
    if isinstance(base_value, dict) and isinstance(override_value, dict):
        merged = dict(base_value)
        for key, value in override_value.items():
            merged[key] = _merge_instruction_values(base_value.get(key), value)
        return merged
    return override_value


def merge_benchmarks_into_genie_json(
    json_file: Path,
    pipeline_config: dict | None,
    configured_benchmarks: list[dict[str, Any]],
    require_configured: bool,
) -> tuple[int, int]:
    """Combina benchmarks del JSON con los del config y persiste el resultado."""
    if require_configured and not configured_benchmarks:
        raise ValueError(
            "Para Genies nuevos es obligatorio especificar benchmarks en pipeline_config.yml"
        )

    with json_file.open(encoding="utf-8") as file:
        genie_space = json.load(file)

    if pipeline_config:
        for key in ("version", "data_sources", "instructions"):
            if key in pipeline_config and pipeline_config[key] is not None:
                if key in genie_space:
                    if key == "instructions":
                        genie_space[key] = _merge_instruction_values(
                            genie_space[key],
                            pipeline_config[key],
                        )
                    else:
                        genie_space[key] = _merge_json_values(
                            genie_space[key],
                            pipeline_config[key],
                        )
                else:
                    genie_space[key] = pipeline_config[key]

    _autogenerate_empty_ids(genie_space)

    benchmark_container = genie_space.setdefault("benchmarks", {})
    benchmark_questions = benchmark_container.setdefault("questions", [])
    if not isinstance(benchmark_questions, list):
        raise ValueError("benchmarks.questions debe ser una lista en el JSON del Genie")

    existing_signatures: set[tuple[str, str]] = set()
    for question_entry in benchmark_questions:
        if not isinstance(question_entry, dict):
            continue
        existing_id = str(question_entry.get("id", "")).strip()
        if not _is_valid_benchmark_id(existing_id):
            question_entry["id"] = _generate_benchmark_id()
        question, expected_sql = _extract_benchmark_entry_fields(question_entry)
        if question and expected_sql:
            existing_signatures.add(
                (question.strip().lower(), _normalize_sql_text(expected_sql))
            )

    added_count = 0
    for benchmark in configured_benchmarks:
        signature = _extract_benchmark_entry_fields(benchmark)
        signature = (signature[0].strip().lower(), _normalize_sql_text(signature[1]))
        if signature in existing_signatures:
            continue

        new_entry = dict(benchmark)
        new_entry["id"] = benchmark.get("id") or _generate_benchmark_id()
        if "evaluation_note" in new_entry and not isinstance(new_entry["evaluation_note"], list):
            new_entry["evaluation_note"] = [str(new_entry["evaluation_note"])]

        benchmark_questions.append(new_entry)
        existing_signatures.add(signature)
        added_count += 1

    if not benchmark_questions:
        raise ValueError("El Genie no contiene benchmarks para evaluar")

    with json_file.open("w", encoding="utf-8") as file:
        json.dump(genie_space, file, ensure_ascii=False, indent=2)
        file.write("\n")

    return len(benchmark_questions), added_count


def resolve_run_validation(config: dict | None, use_interactive_prompt: bool) -> bool:
    """Resuelve la bandera de validación soportando run_validate y run_validation."""
    if config:
        if "run_validate" in config:
            return bool(config.get("run_validate"))
        return bool(config.get("run_validation", True))
    if use_interactive_prompt:
        return ask_yes_no("¿Deseas ejecutar la validación del Job?")
    return True


def resolve_refactor(
    config: dict | None,
    should_validate: bool,
    use_interactive_prompt: bool,
) -> bool:
    """Resuelve si debe refactorizarse; si no hay validación, fuerza False."""
    if not should_validate:
        return False
    if config:
        return bool(config.get("refactor", True))
    if use_interactive_prompt:
        return ask_yes_no("¿Deseas refactorizar el Genie usando la propuesta recuperada?")
    return True


def resolve_revert_on_failed_benchmark(config: dict | None) -> bool:
    """Resuelve si el pipeline debe revertir cambios ante benchmark fallido."""
    if config and "revert_on_failed_benchmark" in config:
        return bool(config.get("revert_on_failed_benchmark"))
    return True


def resolve_metric_view_destination(config: dict | None) -> str:
    """Obtiene el destino catalog.schema para crear metric views."""
    if not config:
        raise ValueError("metric_view_destination es obligatorio en pipeline_config.yml")
    destination = config.get("metric_view_destination")
    if not isinstance(destination, str) or not destination.strip():
        raise ValueError("metric_view_destination es obligatorio y debe tener formato catalog.schema")
    parts = [part.strip() for part in destination.split(".") if part.strip()]
    if len(parts) != 2:
        raise ValueError("metric_view_destination debe tener formato catalog.schema")
    return ".".join(parts)


def resolve_deployed_genie_space_id(
    resource_name: str,
    target: str,
    profile: str,
    fallback_space_id: str | None = None,
) -> str:
    """Obtiene el ID del Genie desplegado desde el resumen JSON del bundle."""
    command = [
        "databricks",
        "-o",
        "json",
        "bundle",
        "summary",
        "--target",
        target,
        "--profile",
        profile,
    ]
    CONSOLE.stage("Resolviendo ID del Genie desplegado", command)
    result = run_subprocess(command, PROJECT_ROOT, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"No se pudo obtener bundle summary para resolver el Genie desplegado: {result.stderr}"
        )
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("La salida de bundle summary no es JSON válido") from error

    genie_spaces = summary.get("resources", {}).get("genie_spaces", {})
    if not isinstance(genie_spaces, dict):
        raise RuntimeError("No se encontró resources.genie_spaces en bundle summary")

    candidate = genie_spaces.get(resource_name)
    if candidate and candidate.get("id"):
        CONSOLE.success(f"ID resuelto: {candidate['id']}")
        return str(candidate["id"])
    if candidate and fallback_space_id:
        CONSOLE.skipped(
            f"El recurso '{resource_name}' no tiene ID en bundle summary; usando fallback {fallback_space_id}."
        )
        return fallback_space_id

    for name, resource in genie_spaces.items():
        if name == resource_name and resource.get("id"):
            CONSOLE.success(f"ID resuelto: {resource['id']}")
            return str(resource["id"])

    if fallback_space_id:
        CONSOLE.skipped(
            f"No se encontró ID para '{resource_name}' en bundle summary; usando fallback {fallback_space_id}."
        )
        return fallback_space_id

    raise RuntimeError(f"No se encontró el recurso Genie '{resource_name}' con ID en bundle summary")


def resolve_assessment_workspace_path(target: str, profile: str) -> str:
    """Resuelve la ruta remota de salidas del assessment desde bundle summary."""
    command = [
        "databricks",
        "-o",
        "json",
        "bundle",
        "summary",
        "--target",
        target,
        "--profile",
        profile,
    ]
    CONSOLE.stage("Resolviendo ruta remota de salidas del assessment", command)
    result = run_subprocess(command, PROJECT_ROOT, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"No se pudo obtener bundle summary para resolver assessment outputs: {result.stderr}"
        )
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("La salida de bundle summary no es JSON válido") from error

    workspace_root = summary.get("workspace", {}).get("root_path")
    if not isinstance(workspace_root, str) or not workspace_root.strip():
        raise RuntimeError("No se encontró workspace.root_path en bundle summary")
    workspace_path = f"{workspace_root.rstrip('/')}/files/src/notebooks"
    CONSOLE.success(f"Ruta remota: {workspace_path}")
    return workspace_path


def get_genie_resource_name(yaml_file: Path) -> str:
    """Obtiene la clave del recurso Genie a partir del YAML generado/importado."""
    with yaml_file.open(encoding="utf-8") as file:
        resource_yaml = yaml.safe_load(file) or {}
    genie_spaces = resource_yaml.get("resources", {}).get("genie_spaces", {})
    if not isinstance(genie_spaces, dict) or not genie_spaces:
        raise ValueError(f"El archivo {yaml_file} no contiene resources.genie_spaces")
    return next(iter(genie_spaces.keys()))


def snapshot_genie_space(space_id: str, profile: str) -> dict[str, Any]:
    """Captura el estado remoto actual de un Genie para poder restaurarlo."""
    CONSOLE.stage("Capturando snapshot remoto del Genie existente")
    client = WorkspaceClient(profile=profile)
    space = client.genie.get_space(space_id, include_serialized_space=True)
    if not space.serialized_space:
        raise RuntimeError(
            f"No se pudo obtener serialized_space para snapshot del Genie {space_id}"
        )
    CONSOLE.success(f"Snapshot capturado para Genie {space_id}")
    return {
        "space_id": space_id,
        "serialized_space": space.serialized_space,
        "title": space.title,
        "description": space.description,
        "parent_path": space.parent_path,
        "warehouse_id": space.warehouse_id,
        "etag": space.etag,
    }


def restore_genie_space(
    snapshot: dict[str, Any],
    profile: str,
    target_space_id: str | None = None,
) -> None:
    """Restaura un Genie remoto a partir de un snapshot."""
    space_id = target_space_id or str(snapshot["space_id"])
    CONSOLE.stage(f"Restaurando Genie remoto {space_id} al estado previo")
    client = WorkspaceClient(profile=profile)
    update_kwargs: dict[str, Any] = {
        "serialized_space": snapshot["serialized_space"],
        "title": snapshot["title"],
        "description": snapshot["description"],
        "parent_path": snapshot["parent_path"],
        "warehouse_id": snapshot["warehouse_id"],
    }
    if snapshot.get("etag"):
        update_kwargs["etag"] = snapshot["etag"]
    client.genie.update_space(space_id, **update_kwargs)
    CONSOLE.success("Genie remoto restaurado")


def discard_deployed_genie_space(space_id: str, profile: str, reason: str) -> None:
    """Descarta el Genie desplegado por el bundle sin tocar el Genie original."""
    CONSOLE.stage(reason)
    client = WorkspaceClient(profile=profile)
    client.genie.trash_space(space_id)
    CONSOLE.success(f"Genie desplegado {space_id} enviado a la papelera")


def delete_genie_space(space_id: str, profile: str, reason: str) -> None:
    """Elimina (trash) un Genie remoto desplegado cuando no debe conservarse."""
    CONSOLE.stage(reason)
    client = WorkspaceClient(profile=profile)
    client.genie.trash_space(space_id)
    CONSOLE.success(f"Genie {space_id} enviado a la papelera")


def ask_yes_no(question: str) -> bool:
    """Solicita una decisión binaria y devuelve True para una respuesta afirmativa."""
    while True:
        answer = input(f"{question} [y/n]: ").strip().lower()
        if answer in {"y", "yes", "s", "si", "sí"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Respuesta inválida. Usa y/n.")


def generar_genie_space_existente(existing_id: str, profile: str) -> tuple[Path, Path]:
    """Genera el Genie Space y devuelve sus rutas YAML y JSON organizadas."""
    yaml_files_before = get_files(RESOURCES_DIRECTORY, "*.genie_space.yml")
    json_files_before = get_files(SOURCE_DIRECTORY, "*.geniespace.json")
    run_command(
        [
            sys.executable,
            "utils/generar_genie.py",
            "--existing-id",
            existing_id,
            "--profile",
            profile,
        ],
        "Generando Genie Space",
    )
    return (
        find_generated_file(RESOURCES_DIRECTORY, "*.genie_space.yml", yaml_files_before),
        find_generated_file(SOURCE_DIRECTORY, "*.geniespace.json", json_files_before),
    )


def generate_config(yaml_file: Path, json_file: Path) -> None:
    """Ejecuta el lector interactivo para crear el config del assessment."""
    run_command(
        [
            sys.executable,
            "utils/leer_estructura_genie.py",
            "--yml",
            str(yaml_file.relative_to(PROJECT_ROOT)),
            "--json",
            str(json_file.relative_to(PROJECT_ROOT)),
        ],
        "Generando config.json",
    )


def generate_config_from_import(
    yaml_file: Path,
    json_file: Path,
    pipeline_config: dict,
) -> None:
    """Crea el config declarativo usando las fuentes y warehouse importados."""
    with yaml_file.open(encoding="utf-8") as file:
        yaml_structure = yaml.safe_load(file) or {}
    with json_file.open(encoding="utf-8") as file:
        json_structure = json.load(file)

    config = build_imported_genie_config(
        yaml_structure,
        json_structure,
        get_config_questions(pipeline_config, require_non_empty=True),
    )
    if not config["warehouse_id"]:
        raise ValueError("El YAML importado no define warehouse_id")
    write_config(config)


def create_manual_genie_space(
    title: str,
    warehouse_id: str,
    config: dict | None = None,
) -> tuple[Path, Path]:
    """Solicita fuentes/preguntas y crea un Genie Space base local."""
    if not warehouse_id:
        warehouse_id = input("Warehouse ID: ").strip()
    if not warehouse_id:
        raise ValueError("warehouse_id es obligatorio para crear un Genie nuevo")
    if config is not None:
        CONSOLE.stage("Creando Genie Space desde configuracion declarativa")
        sources = get_config_sources(config)
        questions = get_config_questions(config, require_non_empty=True)
        if not sources or not questions:
            raise ValueError("La configuración debe incluir sources y business_questions")
        yaml_file, json_file, _ = create_files(
            title,
            sources,
            questions,
            warehouse_id,
            PROJECT_ROOT,
        )
        CONSOLE.success(f"Definiciones creadas: {yaml_file.name} y {json_file.name}")
        return yaml_file, json_file

    before_yaml = get_files(RESOURCES_DIRECTORY, "*.genie_space.yml")
    before_json = get_files(SOURCE_DIRECTORY, "*.geniespace.json")
    run_command(
        [
            sys.executable,
            "utils/crear_genie_desde_entradas.py",
            "--title",
            title,
            "--warehouse-id",
            warehouse_id,
            "--project-root",
            str(PROJECT_ROOT),
        ],
        "Creando Genie Space desde fuentes",
    )
    return (
        find_generated_file(RESOURCES_DIRECTORY, "*.genie_space.yml", before_yaml),
        find_generated_file(SOURCE_DIRECTORY, "*.geniespace.json", before_json),
    )


def validate_and_run_job(target: str, profile: str) -> None:
    """Valida, despliega el Job de assessment y lo ejecuta."""
    run_command(
        ["databricks", "bundle", "validate", "--target", target, "--profile", profile],
        "Validando bundle",
    )
    run_command(
        ["databricks", "bundle", "deploy", "--target", target, "--profile", profile],
        "Desplegando bundle para el assessment",
    )
    run_command(
        ["databricks", "bundle", "sync", "--target", target, "--profile", profile],
        "Sincronizando config y notebook",
    )
    run_command(
        [
            "databricks",
            "bundle",
            "run",
            "genie_assessment",
            "--target",
            target,
            "--profile",
            profile,
        ],
        "Ejecutando assessment",
    )


def refactor_genie(json_file: Path, profile: str, pipeline_config: dict | None) -> list[str]:
    """Crea la Metric View recuperada y actualiza el JSON del Genie."""
    config_file = PROJECT_ROOT / "genie_assessment" / "temp" / "config.json"
    with config_file.open(encoding="utf-8") as file:
        config = json.load(file)
    metric_view_destination = resolve_metric_view_destination(pipeline_config)
    run_command(
        [
            sys.executable,
            "utils/refactorizar_genie.py",
            "--metric-view-yaml",
            str(METRIC_VIEW_OUTPUT_FILE),
            "--genie-json",
            str(json_file.relative_to(PROJECT_ROOT)),
            "--warehouse-id",
            config["warehouse_id"],
            "--metric-view-destination",
            metric_view_destination,
            "--metric-view-base-name",
            build_metric_view_name(json_file),
            "--profile",
            profile,
        ],
        "Refactorizando Genie Space",
    )
    if METRIC_VIEW_MANIFEST_FILE.exists():
        with METRIC_VIEW_MANIFEST_FILE.open(encoding="utf-8") as file:
            manifest = json.load(file)
        identifiers = manifest.get("metric_view_identifiers", [])
        if not isinstance(identifiers, list) or not all(isinstance(item, str) for item in identifiers):
            raise ValueError("El manifiesto de metric views es inválido")
        return identifiers
    return []


def cleanup_metric_views(metric_view_identifiers: list[str], profile: str) -> None:
    """Elimina las metric views creadas si el benchmark no supera el umbral."""
    if not metric_view_identifiers:
        return

    config_file = PROJECT_ROOT / "genie_assessment" / "temp" / "config.json"
    with config_file.open(encoding="utf-8") as file:
        config = json.load(file)
    warehouse_id = config.get("warehouse_id")
    if not isinstance(warehouse_id, str) or not warehouse_id.strip():
        raise ValueError("No se pudo resolver warehouse_id para eliminar metric views")

    client = WorkspaceClient(profile=profile)
    for metric_view_identifier in metric_view_identifiers:
        parts = [part.strip() for part in metric_view_identifier.split(".") if part.strip()]
        if len(parts) != 3:
            raise ValueError(f"Identificador de metric view inválido: {metric_view_identifier}")
        catalog, schema, name = parts
        statement = f"DROP VIEW IF EXISTS `{catalog}`.`{schema}`.`{name}`"
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            wait_timeout="50s",
        )
        if response.status and response.status.state and response.status.state.value != "SUCCEEDED":
            error = response.status.error.message if response.status.error else "estado desconocido"
            raise RuntimeError(
                f"No se pudo eliminar la Metric View {metric_view_identifier}: {error}"
            )


def retrieve_assessment_outputs(profile: str, target: str) -> None:
    """Descarga las salidas del Job a ``genie_assessment/temp/assessment_outputs``."""
    workspace_path = resolve_assessment_workspace_path(target, profile)
    run_command(
        [
            sys.executable,
            "utils/recuperar_salidas_assessment.py",
            "--profile",
            profile,
            "--workspace-path",
            workspace_path,
        ],
        "Recuperando salidas",
    )


def run_benchmarks(
    genie_space_id: str,
    json_file: Path,
    profile: str,
    threshold: float,
    block_deploy: bool,
) -> bool:
    """Ejecuta benchmarks y devuelve si el Genie supera el umbral."""
    command = [
        sys.executable,
        "utils/ejecutar_benchmarks.py",
        "--genie-space-id",
        genie_space_id,
        "--genie-json",
        str(json_file.relative_to(PROJECT_ROOT)),
        "--threshold",
        str(threshold),
        "--profile",
        profile,
    ]
    CONSOLE.stage("Ejecutando benchmarks", command)
    result = run_subprocess(command, PROJECT_ROOT, capture_output=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode == 0:
        CONSOLE.success(f"Umbral de calidad alcanzado ({threshold:.2%})")
        return True
    if result.returncode == 2:
        if block_deploy:
            CONSOLE.skipped(
                f"Benchmarks no superan el umbral requerido ({threshold:.2%}); deploy cancelado."
            )
        else:
            print(
                f"\n[WARNING] Benchmarks no superan el umbral requerido ({threshold:.2%})."
            )
        return False
    raise RuntimeError(f"Error ejecutando benchmarks: código {result.returncode}")


def deploy_bundle(target: str, profile: str) -> None:
    """Despliega el bundle después de superar la validación de benchmarks."""
    run_command(
        ["databricks", "bundle", "deploy", "--target", target, "--profile", profile],
        "Desplegando bundle después de benchmarks",
    )


def main() -> None:
    """Ejecuta el flujo y conserva las definiciones solo tras un deploy exitoso."""
    args = parse_arguments()
    pipeline_config: dict | None = None
    if args.config:
        pipeline_config = load_pipeline_config(args.config)
        apply_pipeline_config(args, pipeline_config)
    CONSOLE.start(args, args.config)
    with LocalProjectTransaction(MANAGED_DIRECTORIES):
        created_metric_view_identifiers: list[str] = []
        CONSOLE.stage("Limpiando assessment_outputs")
        clear_directory_contents(ASSESSMENT_OUTPUTS_DIRECTORY)
        CONSOLE.success("Carpeta assessment_outputs limpia")
        if args.existing_id:
            yaml_file, json_file = generar_genie_space_existente(
                args.existing_id, args.profile
            )
            if pipeline_config:
                CONSOLE.stage("Generando config.json desde configuracion declarativa")
                generate_config_from_import(
                    yaml_file,
                    json_file,
                    pipeline_config,
                )
                CONSOLE.success("Configuracion local creada con el warehouse importado")
            else:
                generate_config(yaml_file, json_file)
        else:
            yaml_file, json_file = create_manual_genie_space(
                args.title,
                args.warehouse_id,
                pipeline_config,
            )

        configured_benchmarks = get_config_benchmarks(pipeline_config)
        CONSOLE.stage("Preparando benchmarks del Genie")
        total_benchmarks, added_benchmarks = merge_benchmarks_into_genie_json(
            json_file,
            pipeline_config,
            configured_benchmarks,
            require_configured=not bool(args.existing_id),
        )
        CONSOLE.success(
            f"Benchmarks finales a desplegar: {total_benchmarks} "
            f"(agregados desde config: {added_benchmarks})"
        )

        should_validate = resolve_run_validation(
            pipeline_config,
            use_interactive_prompt=bool(args.existing_id),
        )
        should_refactor = resolve_refactor(
            pipeline_config,
            should_validate=should_validate,
            use_interactive_prompt=bool(args.existing_id),
        )
        revert_on_failed_benchmark = resolve_revert_on_failed_benchmark(pipeline_config)

        if should_validate:
            validate_and_run_job(args.target, args.profile)
            retrieve_assessment_outputs(args.profile, args.target)
            if should_refactor and METRIC_VIEW_OUTPUT_FILE.exists():
                created_metric_view_identifiers = refactor_genie(
                    json_file,
                    args.profile,
                    pipeline_config,
                )
            elif should_refactor:
                CONSOLE.skipped(
                    "Refactorización omitida: el assessment no propuso metric views nuevas."
                )
            else:
                CONSOLE.skipped(
                    "Refactorización omitida por configuración (refactor=false)."
                )

        else:
            CONSOLE.skipped("Validación, assessment, recuperación y refactorización.")

        resource_name = get_genie_resource_name(yaml_file)
        previous_snapshot: dict[str, Any] | None = None
        previous_deployed_space_id: str | None = None

        if args.existing_id:
            try:
                previous_deployed_space_id = args.existing_id
                previous_snapshot = snapshot_genie_space(
                    previous_deployed_space_id,
                    args.profile,
                )
            except RuntimeError:
                CONSOLE.skipped(
                    "No se encontró un despliegue previo para snapshot; no habrá rollback automático."
                )

        deploy_bundle(args.target, args.profile)
        deployed_space_id = resolve_deployed_genie_space_id(
            resource_name,
            args.target,
            args.profile,
            fallback_space_id=args.existing_id if args.existing_id else None,
        )
        benchmark_passed = run_benchmarks(
            deployed_space_id,
            json_file,
            args.profile,
            args.benchmark_threshold,
            revert_on_failed_benchmark,
        )
        if not benchmark_passed:
            if revert_on_failed_benchmark:
                if args.existing_id:
                    if (
                        previous_snapshot is not None
                        and previous_deployed_space_id
                        and deployed_space_id == previous_deployed_space_id
                    ):
                        restore_genie_space(previous_snapshot, args.profile)
                    elif deployed_space_id:
                        discard_deployed_genie_space(
                            deployed_space_id,
                            args.profile,
                            "El benchmark del Genie existente no superó el umbral; descartando el despliegue temporal.",
                        )
                    else:
                        CONSOLE.skipped(
                            "No fue posible identificar el despliegue temporal para descartarlo."
                        )
                else:
                    delete_genie_space(
                        deployed_space_id,
                        args.profile,
                        "El benchmark del Genie nuevo no superó el umbral; eliminando deploy remoto.",
                    )
                    CONSOLE.skipped(
                        "Deploy revertido para Genie nuevo por fallo de benchmark."
                    )
                cleanup_metric_views(created_metric_view_identifiers, args.profile)
            else:
                print(
                    "\n[WARNING] Benchmarks no superan el umbral, pero la configuración permite conservar los cambios."
                )

    CONSOLE.completed()


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, RuntimeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as error:
        print(f"\n[ERROR] Pipeline detenido: {error}", file=sys.stderr)
        raise SystemExit(1) from error
