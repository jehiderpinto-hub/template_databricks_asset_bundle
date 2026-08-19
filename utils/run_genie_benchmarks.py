import argparse
import difflib
import json
import re
from pathlib import Path
from typing import Any

from common import read_json_file
from databricks.sdk import WorkspaceClient


def normalize_sql(sql: str) -> str:
    """Normaliza SQL para comparar consultas con diferencias de formato."""
    return re.sub(r"\s+", " ", sql.strip().lower().replace("`", ""))


def sql_similarity(expected_sql: str, actual_sql: str) -> float:
    """Calcula similitud entre SQL esperado y SQL generado, entre 0 y 1."""
    return difflib.SequenceMatcher(
        None,
        normalize_sql(expected_sql),
        normalize_sql(actual_sql),
    ).ratio()


def extract_benchmarks(genie_json: Path) -> list[dict[str, Any]]:
    """Extrae preguntas y SQL esperado del JSON exportado del Genie Space."""
    genie_space = read_json_file(genie_json)
    benchmarks = genie_space.get("benchmarks", {}).get("questions", [])
    extracted: list[dict[str, Any]] = []

    for benchmark in benchmarks:
        question_values = benchmark.get("question", [])
        answers = benchmark.get("answer", [])
        sql_answers = [
            "".join(answer.get("content", []))
            for answer in answers
            if answer.get("format") == "SQL"
        ]
        if question_values and sql_answers:
            extracted.append(
                {
                    "id": benchmark.get("id", ""),
                    "question": question_values[0],
                    "expected_sql": sql_answers[0],
                }
            )

    return extracted


def extract_generated_sql(message: Any) -> str:
    """Obtiene el primer SQL generado desde los attachments de una respuesta."""
    for attachment in message.attachments or []:
        if attachment.query:
            return attachment.query
    return ""


def evaluate_benchmarks(
    genie_space_id: str,
    benchmarks: list[dict[str, Any]],
    profile: str,
) -> list[dict[str, Any]]:
    """Ejecuta las preguntas contra Genie y devuelve sus resultados comparados."""
    client = WorkspaceClient(profile=profile)
    results: list[dict[str, Any]] = []

    for benchmark in benchmarks:
        message = client.genie.start_conversation_and_wait(
            space_id=genie_space_id,
            content=benchmark["question"],
        )
        actual_sql = extract_generated_sql(message)
        score = sql_similarity(benchmark["expected_sql"], actual_sql)
        results.append(
            {
                **benchmark,
                "actual_sql": actual_sql,
                "score": round(score, 4),
                "passed": bool(actual_sql),
            }
        )

    return results


def write_report(results: list[dict[str, Any]], output_file: Path) -> float:
    """Guarda el reporte de benchmarks y devuelve el promedio de similitud."""
    average_score = sum(result["score"] for result in results) / len(results)
    report = {
        "average_score": round(average_score, 4),
        "benchmarks_total": len(results),
        "results": results,
    }
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)
        file.write("\n")
    return average_score


def main() -> None:
    """Ejecuta benchmarks y falla si el promedio no alcanza el umbral."""
    parser = argparse.ArgumentParser(
        description="Ejecuta benchmarks de Genie y valida un umbral de calidad."
    )
    parser.add_argument("--genie-space-id", required=True)
    parser.add_argument("--genie-json", type=Path, required=True)
    parser.add_argument("--threshold", type=float, default=0.8)
    parser.add_argument("--profile", default="dev")
    parser.add_argument(
        "--report",
        type=Path,
        default=(
            Path("genie_assessment")
            / "temp"
            / "assessment_outputs"
            / "genie_benchmark_results.json"
        ),
    )
    args = parser.parse_args()

    if not 0 <= args.threshold <= 1:
        raise ValueError("El umbral debe estar entre 0 y 1")

    benchmarks = extract_benchmarks(args.genie_json)
    if not benchmarks:
        raise ValueError("El JSON no contiene benchmarks SQL ejecutables")

    results = evaluate_benchmarks(args.genie_space_id, benchmarks, args.profile)
    average_score = write_report(results, args.report)
    print(f"Benchmark score: {average_score:.4f} / threshold: {args.threshold:.4f}")

    if average_score < args.threshold:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
