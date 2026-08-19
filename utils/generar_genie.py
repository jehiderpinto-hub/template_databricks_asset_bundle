"""Genera un Genie Space existente y organiza sus archivos en el bundle."""

import argparse
import glob
import os
import shutil
import sys
from pathlib import Path

import yaml
from comun import run_subprocess


def normalize_genie_resource_file_path(resource_file: str) -> None:
    """Normaliza el ``file_path`` del YAML al layout del bundle."""
    with open(resource_file, encoding="utf-8") as file:
        resource = yaml.safe_load(file)

    genie_spaces = resource.get("resources", {}).get("genie_spaces", {})
    for genie_space in genie_spaces.values():
        file_path = genie_space.get("file_path")
        if file_path:
            genie_space["file_path"] = (
                "../../src/genie_spaces/" + os.path.basename(file_path)
            )

    with open(resource_file, "w", encoding="utf-8") as file:
        yaml.safe_dump(resource, file, allow_unicode=True, sort_keys=False)


def build_generation_command(genie_space_id: str, profile: str | None) -> list[str]:
    """Construye el comando de Databricks para generar un Genie Space."""
    command = [
        "databricks",
        "bundle",
        "generate",
        "genie-space",
        "--existing-id",
        genie_space_id,
    ]
    if profile:
        command.extend(["--profile", profile])
    return command


def move_generated_files(
    source_directory: str,
    target_directory: str,
    patterns: list[str],
    normalize_yaml: bool = False,
) -> list[str]:
    """Mueve archivos generados a una carpeta del bundle y devuelve sus nombres."""
    imported_files: list[str] = []
    for pattern in patterns:
        for filepath in glob.glob(os.path.join(source_directory, pattern)):
            if not os.path.isfile(filepath):
                continue
            filename = os.path.basename(filepath)
            destination = os.path.join(target_directory, filename)
            shutil.move(filepath, destination)
            if normalize_yaml:
                normalize_genie_resource_file_path(destination)
            imported_files.append(filename)
    return imported_files


def generate_and_organize_genie_space(
    genie_space_id: str, profile: str | None = None
) -> None:
    """Genera un Genie Space y organiza sus archivos en ``resources`` y ``src``."""
    root_dir = os.getcwd()
    resources_dir = os.path.join(root_dir, "resources")
    src_dir = os.path.join(root_dir, "src")

    # Carpetas de destino
    target_resources_dir = os.path.join(resources_dir, "genie_spaces")
    target_src_dir = os.path.join(src_dir, "genie_spaces")

    os.makedirs(target_resources_dir, exist_ok=True)
    os.makedirs(target_src_dir, exist_ok=True)

    cmd = build_generation_command(genie_space_id, profile)

    print(f"Ejecutando: {' '.join(cmd)}")

    result = run_subprocess(cmd, Path.cwd(), capture_output=True)

    if result.returncode != 0:
        print("Error al generar el Genie Space:", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    print(result.stdout)

    imported_resources = move_generated_files(
        resources_dir, target_resources_dir, ["*.yml"], normalize_yaml=True
    )
    imported_sources = move_generated_files(
        src_dir, target_src_dir, ["*.geniespace.json", "*.json"]
    )

    for filename in imported_resources:
        print(f"Importado: {filename} -> resources/genie_spaces/")
    for filename in imported_sources:
        print(f"Importado: {filename} -> src/genie_spaces/")

    print("\n¡Archivos de Genie Space generados y organizados exitosamente!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Genera y organiza Genie Spaces en subcarpetas dedicadas."
    )
    parser.add_argument(
        "--existing-id",
        required=True,
        help="El ID del Genie Space existente en Databricks",
    )
    parser.add_argument(
        "--profile",
        required=False,
        default="dev",
        help="Perfil de Databricks a utilizar (ej. dev, desa)",
    )

    args = parser.parse_args()
    generate_and_organize_genie_space(args.existing_id, args.profile)