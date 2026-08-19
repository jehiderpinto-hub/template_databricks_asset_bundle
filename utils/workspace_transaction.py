"""Transacción de archivos locales para el pipeline de Genie."""

import shutil
import tempfile
from pathlib import Path


class LocalProjectTransaction:
    """Guarda y restaura directorios locales modificados por el pipeline."""

    def __init__(self, project_root: Path, managed_directories: list[Path]) -> None:
        """Inicializa una transacción para los directorios administrados."""
        self.project_root = project_root
        self.managed_directories = managed_directories
        self._temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        self._snapshot_directory: Path | None = None

    def __enter__(self) -> "LocalProjectTransaction":
        """Crea una copia temporal del estado inicial del proyecto."""
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix="genie_pipeline_"
        )
        self._snapshot_directory = Path(self._temporary_directory.name)
        for index, directory in enumerate(self.managed_directories):
            if directory.exists():
                shutil.copytree(
                    directory,
                    self._snapshot_directory / str(index),
                )
        return self

    def restore(self) -> None:
        """Restaura el estado inicial y elimina archivos generados localmente."""
        if self._snapshot_directory is None:
            return

        for index, directory in enumerate(self.managed_directories):
            if directory.exists():
                shutil.rmtree(directory)
            snapshot = self._snapshot_directory / str(index)
            if snapshot.exists():
                shutil.copytree(snapshot, directory)

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """Restaura solo si falla el flujo y libera el snapshot temporal."""
        try:
            if exc_type is not None:
                self.restore()
        finally:
            if self._temporary_directory is not None:
                self._temporary_directory.cleanup()
        return False
