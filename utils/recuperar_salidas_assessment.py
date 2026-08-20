"""Recupera desde Workspace los resultados generados por Genie Assessment."""

import argparse
import fnmatch
import shutil
from pathlib import Path

from comun import run_subprocess


OUTPUT_FILE_PATTERNS = (
    "genie_evidence_payload_*.json",
    "genie_final_client_report_*.json",
    "genie_proposed_metric_view_*.yml",
    "genie_proposed_metric_view_*.yaml",
    "genie_scorecard_*.xlsx"
)


def is_assessment_output(file_name: str) -> bool:
    """Indica si un nombre corresponde a una salida recuperable del assessment."""
    return any(
        fnmatch.fnmatch(file_name, pattern)
        for pattern in OUTPUT_FILE_PATTERNS
    )


def export_workspace_directory(
    workspace_path: str,
    staging_directory: Path,
    profile: str | None,
) -> None:
    """Exporta una carpeta remota del Workspace a un staging local."""
    command = [
        "databricks",
        "workspace",
        "export-dir",
        workspace_path,
        str(staging_directory),
    ]
    if profile:
        command.extend(["--profile", profile])

    result = run_subprocess(command, Path.cwd(), capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())


def retrieve_outputs(
    workspace_path: str,
    output_directory: Path,
    profile: str | None,
) -> list[Path]:
    """Copia las salidas permitidas desde Workspace a la carpeta local."""
    output_directory.mkdir(parents=True, exist_ok=True)
    staging_directory = output_directory / ".workspace_export"
    if staging_directory.exists():
        shutil.rmtree(staging_directory)

    try:
        export_workspace_directory(workspace_path, staging_directory, profile)
        copied_files: list[Path] = []

        for source_file in staging_directory.iterdir():
            if not source_file.is_file() or not is_assessment_output(source_file.name):
                continue

            destination_file = output_directory / source_file.name
            shutil.copy2(source_file, destination_file)
            copied_files.append(destination_file)

        return copied_files
    finally:
        if staging_directory.exists():
            shutil.rmtree(staging_directory)


def main() -> None:
    """Configura la CLI y recupera los archivos del assessment."""
    parser = argparse.ArgumentParser(
        description="Recupera las salidas del Genie Assessment dentro del bundle local."
    )
    parser.add_argument(
        "--workspace-path",
        default=(
            "/Workspace/Users/josorioos@argos.com.co/"
            ".bundle/template_databricks_asset_bundle/dev/files/src/notebooks"
        ),
        help="Ruta remota de la carpeta de salidas en Databricks Workspace.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("genie_assessment") / "temp" / "assessment_outputs",
        help="Carpeta local del bundle donde se guardarán las salidas.",
    )
    parser.add_argument(
        "--profile",
        default="dev",
        help="Perfil de Databricks utilizado por la CLI.",
    )
    args = parser.parse_args()

    copied_files = retrieve_outputs(
        args.workspace_path,
        args.output_dir,
        args.profile,
    )

    if not copied_files:
        raise RuntimeError(
            "No se encontraron archivos de salida en la ruta remota especificada."
        )

    for file_path in copied_files:
        print(f"Recuperado: {file_path}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        raise SystemExit(f"Error recuperando salidas: {error}") from error
