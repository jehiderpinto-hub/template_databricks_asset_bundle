import argparse
import glob
import os
import shutil
import subprocess
import sys


def generate_and_organize_genie_space(genie_space_id: str, profile: str = None):
    root_dir = os.getcwd()
    resources_dir = os.path.join(root_dir, "resources")
    src_dir = os.path.join(root_dir, "src")

    # Carpetas de destino
    target_resources_dir = os.path.join(resources_dir, "genie_spaces")
    target_src_dir = os.path.join(src_dir, "genie_spaces")

    os.makedirs(target_resources_dir, exist_ok=True)
    os.makedirs(target_src_dir, exist_ok=True)

    # Construir el comando de la CLI
    cmd = [
        "databricks",
        "bundle",
        "generate",
        "genie-space",
        "--existing-id",
        genie_space_id,
    ]

    # Si se pasa un perfil, agregarlo como flag al comando
    if profile:
        cmd.extend(["--profile", profile])

    print(f"Ejecutando: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("Error al generar el Genie Space:", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    print(result.stdout)

    # Mover archivos creados en 'resources/' a 'resources/genie_spaces/'
    for filepath in glob.glob(os.path.join(resources_dir, "*.yml")):
        if os.path.isfile(filepath):
            filename = os.path.basename(filepath)
            dest = os.path.join(target_resources_dir, filename)
            shutil.move(filepath, dest)
            print(f"Importado: {filename} -> resources/genie_spaces/")

    # Mover archivos creados en 'src/' a 'src/genie_spaces/'
    for filepath in glob.glob(
        os.path.join(src_dir, "*.geniespace.json")
    ) + glob.glob(os.path.join(src_dir, "*.json")):
        if os.path.isfile(filepath):
            filename = os.path.basename(filepath)
            dest = os.path.join(target_src_dir, filename)
            shutil.move(filepath, dest)
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