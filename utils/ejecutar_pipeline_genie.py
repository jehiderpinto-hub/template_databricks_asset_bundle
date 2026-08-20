"""Orquesta el flujo completo de generación, validación y despliegue de un Genie Space."""

import argparse
import json
import sys
from pathlib import Path

import yaml
from comun import run_subprocess
from crear_genie_desde_entradas import build_config, create_files
from leer_estructura_genie import write_config
from transaccion_proyecto import LocalProjectTransaction


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESOURCES_DIRECTORY = PROJECT_ROOT / "resources" / "genie_spaces"
SOURCE_DIRECTORY = PROJECT_ROOT / "src" / "genie_spaces"
MANAGED_DIRECTORIES = [
    RESOURCES_DIRECTORY,
    SOURCE_DIRECTORY,
    PROJECT_ROOT / "genie_assessment" / "temp",
]


def run_command(command: list[str], description: str) -> None:
    """Ejecuta un comando desde la raíz del proyecto y falla si no termina bien."""
    print(f"\n[{description}] {' '.join(command)}")
    result = run_subprocess(command, PROJECT_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"El comando terminó con código {result.returncode}")


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
        help="Similitud SQL promedio mínima para permitir el deploy (0 a 1).",
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
    ]:
        if name in config and config[name] is not None:
            setattr(args, name, config[name])


def get_config_questions(config: dict) -> list[str]:
    """Valida y devuelve las preguntas declaradas en el YAML."""
    questions = config.get("business_questions", [])
    if not isinstance(questions, list) or not all(isinstance(item, str) for item in questions):
        raise ValueError("business_questions debe ser una lista de textos")
    return questions


def get_config_sources(config: dict) -> list[str]:
    """Valida y devuelve las fuentes declaradas en el YAML."""
    sources = config.get("sources", [])
    if not isinstance(sources, list) or not all(isinstance(item, str) for item in sources):
        raise ValueError("sources debe ser una lista de textos")
    return sources


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
        sources = get_config_sources(config)
        questions = get_config_questions(config)
        if not sources or not questions:
            raise ValueError("La configuración debe incluir sources y business_questions")
        yaml_file, json_file, _ = create_files(
            title,
            sources,
            questions,
            warehouse_id,
            PROJECT_ROOT,
        )
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
    """Valida, sincroniza archivos y ejecuta el Job de assessment ya desplegado."""
    run_command(
        ["databricks", "bundle", "validate", "--target", target, "--profile", profile],
        "Validando bundle",
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


def refactor_genie(json_file: Path, profile: str) -> None:
    """Crea la Metric View recuperada y actualiza el JSON del Genie."""
    config_file = PROJECT_ROOT / "genie_assessment" / "temp" / "config.json"
    output_directory = (
        PROJECT_ROOT / "genie_assessment" / "temp" / "assessment_outputs"
    )
    with config_file.open(encoding="utf-8") as file:
        config = json.load(file)
    run_command(
        [
            sys.executable,
            "utils/refactorizar_genie.py",
            "--metric-view-yaml",
            str(output_directory / "genie_proposed_metric_view_brz_dev.yml"),
            "--genie-json",
            str(json_file.relative_to(PROJECT_ROOT)),
            "--warehouse-id",
            config["warehouse_id"],
            "--metric-view-name",
            "mv_genie_assessment",
            "--profile",
            profile,
        ],
        "Refactorizando Genie Space",
    )


def retrieve_assessment_outputs(profile: str) -> None:
    """Descarga las salidas del Job a ``genie_assessment/temp/assessment_outputs``."""
    run_command(
        [
            sys.executable,
            "utils/recuperar_salidas_assessment.py",
            "--profile",
            profile,
        ],
        "Recuperando salidas",
    )


def run_benchmarks(
    genie_space_id: str,
    json_file: Path,
    profile: str,
    threshold: float,
) -> None:
    """Ejecuta benchmarks y detiene el pipeline si no alcanza el umbral."""
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
    print(f"\n[Ejecutando benchmarks] {' '.join(command)}")
    result = run_subprocess(command, PROJECT_ROOT, capture_output=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError("El umbral de benchmarks no fue alcanzado; deploy cancelado")


def deploy_bundle(target: str, profile: str) -> None:
    """Despliega el bundle después de superar la validación de benchmarks."""
    run_command(
        ["databricks", "bundle", "deploy", "--target", target, "--profile", profile],
        "Desplegando bundle después de benchmarks",
    )


def main() -> None:
    """Ejecuta el flujo y conserva las definiciones solo tras un deploy exitoso."""
    args = parse_arguments()
    pipeline_config = None
    if args.config:
        pipeline_config = load_pipeline_config(args.config)
        apply_pipeline_config(args, pipeline_config)
    with LocalProjectTransaction(PROJECT_ROOT, MANAGED_DIRECTORIES):
        if args.existing_id:
            yaml_file, json_file = generar_genie_space_existente(
                args.existing_id, args.profile
            )
            if pipeline_config:
                write_config(
                    build_config(
                        get_config_sources(pipeline_config),
                        get_config_questions(pipeline_config),
                        pipeline_config.get("warehouse_id", args.warehouse_id),
                    )
                )
            else:
                generate_config(yaml_file, json_file)
        else:
            yaml_file, json_file = create_manual_genie_space(
                args.title,
                args.warehouse_id,
                pipeline_config,
            )

        should_validate = (
            bool(pipeline_config.get("run_validation", True))
            if pipeline_config
            else (True if not args.existing_id else ask_yes_no("¿Deseas ejecutar la validación del Job?"))
        )
        if should_validate:
            validate_and_run_job(args.target, args.profile)
            retrieve_assessment_outputs(args.profile)

            should_refactor = (
                bool(pipeline_config.get("refactor", True))
                if pipeline_config
                else (True if not args.existing_id else ask_yes_no("¿Deseas refactorizar el Genie usando la propuesta recuperada?"))
            )
            if should_refactor:
                refactor_genie(json_file, args.profile)

            if args.existing_id:
                run_benchmarks(
                    args.existing_id,
                    json_file,
                    args.profile,
                    args.benchmark_threshold,
                )
            else:
                print(
                    "Genie nuevo: validación y refactorización obligatorias; "
                    "se omiten benchmarks API."
                )

        deploy_bundle(args.target, args.profile)

    print("\nPipeline completado correctamente.")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, RuntimeError) as error:
        raise SystemExit(f"Pipeline detenido: {error}") from error
