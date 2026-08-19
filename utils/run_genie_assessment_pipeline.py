import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESOURCES_DIRECTORY = PROJECT_ROOT / "resources" / "genie_spaces"
SOURCE_DIRECTORY = PROJECT_ROOT / "src" / "genie_spaces"


def run_command(command: list[str], description: str) -> None:
    print(f"\n[{description}] {' '.join(command)}")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def get_files(directory: Path, pattern: str) -> set[Path]:
    return set(directory.glob(pattern))


def find_generated_file(
    directory: Path,
    pattern: str,
    files_before_generation: set[Path],
) -> Path:
    files_after_generation = get_files(directory, pattern)
    new_files = files_after_generation - files_before_generation
    candidates = new_files or files_after_generation

    if not candidates:
        raise FileNotFoundError(
            f"No se encontró un archivo generado con el patrón '{pattern}' en {directory}"
        )

    return max(candidates, key=lambda file_path: file_path.stat().st_mtime)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Genera, evalúa y recupera los resultados de un Genie Space."
    )
    parser.add_argument(
        "--existing-id",
        required=True,
        help="ID del Genie Space existente en Databricks.",
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
    args = parser.parse_args()

    yaml_files_before = get_files(RESOURCES_DIRECTORY, "*.genie_space.yml")
    json_files_before = get_files(SOURCE_DIRECTORY, "*.geniespace.json")

    run_command(
        [
            sys.executable,
            "utils/generate_genie_space.py",
            "--existing-id",
            args.existing_id,
            "--profile",
            args.profile,
        ],
        "Generando Genie Space",
    )

    yaml_file = find_generated_file(
        RESOURCES_DIRECTORY,
        "*.genie_space.yml",
        yaml_files_before,
    )
    json_file = find_generated_file(
        SOURCE_DIRECTORY,
        "*.geniespace.json",
        json_files_before,
    )

    run_command(
        [
            sys.executable,
            "utils/read_genie_structure.py",
            "--yml",
            str(yaml_file.relative_to(PROJECT_ROOT)),
            "--json",
            str(json_file.relative_to(PROJECT_ROOT)),
        ],
        "Generando config.json",
    )

    run_command(
        [
            "databricks",
            "bundle",
            "validate",
            "--target",
            args.target,
            "--profile",
            args.profile,
        ],
        "Validando bundle",
    )
    run_command(
        [
            "databricks",
            "bundle",
            "run",
            "genie_assessment",
            "--target",
            args.target,
            "--profile",
            args.profile,
        ],
        "Ejecutando assessment",
    )
    run_command(
        [
            sys.executable,
            "utils/retrieve_genie_assessment_outputs.py",
            "--profile",
            args.profile,
        ],
        "Recuperando salidas",
    )

    print("\nPipeline completado correctamente.")
    print("Config: genie_assessment/temp/config.json")
    print("Salidas: temp/assessment_outputs")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Pipeline detenido: {error}") from error
