"""Ejecuta benchmarks de Genie vía API y evalúa su umbral de calidad."""

import argparse
import json
from pathlib import Path
from typing import Any

from comun import read_json_file
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieEvalAssessment


def _extract_sql_from_eval_response(responses: list[Any] | None) -> str:
    """Obtiene el SQL ejecutado desde la respuesta de evaluación del Genie."""
    if not responses:
        return ""
    for response in responses:
        if getattr(response, "response_type", None) and str(response.response_type).upper() == "SQL":
            sql = getattr(response, "response", "")
            if sql:
                return sql
    return ""


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


def evaluate_benchmarks(
    genie_space_id: str,
    benchmarks: list[dict[str, Any]],
    profile: str,
    pass_threshold: float = 0.8,
) -> list[dict[str, Any]]:
    """Usa la evaluación oficial del Genie para decidir si cada benchmark fue bueno o malo."""
    client = WorkspaceClient(profile=profile)
    benchmark_ids = [benchmark["id"] for benchmark in benchmarks if benchmark.get("id")]
    if not benchmark_ids:
        raise ValueError("El JSON no contiene identificadores válidos para benchmarks")

    runs = client.genie.genie_list_eval_runs(space_id=genie_space_id, page_size=20).eval_runs or []
    if runs:
        latest_run = max(runs, key=lambda item: getattr(item, "created_timestamp", 0) or 0)
        eval_run_id = latest_run.eval_run_id
    else:
        run = client.genie.genie_create_eval_run(
            space_id=genie_space_id,
            benchmark_question_ids=benchmark_ids,
        )
        eval_run_id = run.eval_run_id

    eval_results = client.genie.genie_list_eval_results(
        space_id=genie_space_id,
        eval_run_id=eval_run_id,
    ).eval_results or []
    if not eval_results:
        raise ValueError("No se obtuvieron resultados de evaluación del Genie")

    benchmark_lookup = {benchmark["id"]: benchmark for benchmark in benchmarks if benchmark.get("id")}
    results: list[dict[str, Any]] = []

    for eval_result in eval_results:
        benchmark_id = eval_result.benchmark_question_id
        benchmark = benchmark_lookup.get(benchmark_id, {})
        detail = client.genie.genie_get_eval_result_details(
            space_id=genie_space_id,
            eval_run_id=eval_run_id,
            result_id=eval_result.result_id,
        )
        assessment = getattr(detail, "assessment", None)
        passed = assessment == GenieEvalAssessment.GOOD
        actual_sql = _extract_sql_from_eval_response(getattr(detail, "actual_response", None))
        score = 1.0 if passed else 0.0
        results.append(
            {
                "id": benchmark_id,
                "question": benchmark.get("question") or getattr(eval_result, "question", ""),
                "expected_sql": benchmark.get("expected_sql") or getattr(eval_result, "benchmark_answer", ""),
                "actual_sql": actual_sql,
                "score": round(score, 4),
                "passed": passed,
            }
        )

    return results


def write_report(results: list[dict[str, Any]], output_file: Path) -> float:
    """Guarda el reporte de benchmarks y devuelve el porcentaje de éxitos."""
    if not results:
        raise ValueError("No hay resultados de benchmark para escribir")

    passed_count = sum(1 for result in results if result["passed"])
    pass_rate = passed_count / len(results)
    report = {
        "average_score": round(pass_rate, 4),
        "benchmarks_total": len(results),
        "passed_benchmarks": passed_count,
        "results": results,
    }
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)
        file.write("\n")
    return pass_rate


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

    results = evaluate_benchmarks(
        args.genie_space_id,
        benchmarks,
        args.profile,
        pass_threshold=args.threshold,
    )
    average_score = write_report(results, args.report)
    print(f"Benchmark score: {average_score:.4f} / threshold: {args.threshold:.4f}")

    if average_score < args.threshold:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
