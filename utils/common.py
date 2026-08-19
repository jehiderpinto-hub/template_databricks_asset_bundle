"""Funciones compartidas por los utilitarios del bundle."""

import json
import subprocess
from pathlib import Path
from typing import Any

import yaml


def read_yaml_file(file_path: Path) -> Any:
    """Lee un archivo YAML y devuelve su contenido deserializado."""
    with file_path.open(encoding="utf-8") as file:
        return yaml.safe_load(file)


def read_json_file(file_path: Path) -> Any:
    """Lee un archivo JSON y devuelve su contenido deserializado."""
    with file_path.open(encoding="utf-8") as file:
        return json.load(file)


def validate_file(file_path: Path, expected_suffix: str) -> None:
    """Valida que exista un archivo con la extensión solicitada."""
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")
    if file_path.suffix.lower() != expected_suffix:
        raise ValueError(
            f"El archivo {file_path} debe tener extensión {expected_suffix}"
        )


def run_subprocess(
    command: list[str],
    cwd: Path,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Ejecuta un comando con el directorio de trabajo del proyecto."""
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=capture_output,
        text=True,
        check=False,
    )
