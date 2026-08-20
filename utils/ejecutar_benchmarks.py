"""Ejecuta benchmarks de Genie y evalúa su umbral de calidad."""

import argparse
import json
import time
from pathlib import Path
from typing import Any

from comun import read_json_file
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import EvaluationStatusType, GenieEvalAssessment


def _extract_sql_from_eval_response(responses: list[Any] | None) -> str:
    """Obtiene el SQL ejecutado desde una respuesta de evaluación del Genie."""
    if not responses:
        return ""
    for response in responses:
        response_type = getattr(response, "response_type", None)
        if response_type and str(response_type).upper().endswith("SQL"):
            sql = getattr(response, "response", "")
            if sql:
                return sql
    return ""


def extract_benchmarks(genie_json: Path) -> list[dict[str, str]]:
    """Extrae benchmarks del JSON del Genie."""
    genie_space = read_json_file(genie_json)
    questions = genie_space.get("benchmarks", {}).get("questions", [])
    if not isinstance(questions, list):
        raise ValueError("benchmarks.questions debe ser una lista")

    extracted: list[dict[str, str]] = []
    for benchmark in questions:
        if not isinstance(benchmark, dict):
            continue
        question_values = benchmark.get("question", [])
        answers = benchmark.get("answer", [])
        question = question_values[0].strip() if question_values else ""
        expected_sql = ""
        for answer in answers:
            if answer.get("format") != "SQL":
                continue
            expected_sql = "".join(answer.get("content", [])).strip()
            if expected_sql:
                break
        if question and expected_sql:
            extracted.append(
                {
                    "id": str(benchmark.get("id", "")).strip(),
                    "question": question,
                    "expected_sql": expected_sql,
                }
            )
    return extracted


def _wait_for_eval_run(client: WorkspaceClient, space_id: str, eval_run_id: str) -> None:
    """Espera hasta que termine un eval run de Genie."""
    timeout_seconds = 600
    polling_interval_seconds = 5
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        run_status = client.genie.genie_get_eval_run(
            space_id=space_id,
            eval_run_id=eval_run_id,
        )
        status = run_status.eval_run_status
        if status == EvaluationStatusType.DONE:
            return
        if status in {
            EvaluationStatusType.EVALUATION_CANCELLED,
            EvaluationStatusType.EVALUATION_FAILED,
            EvaluationStatusType.EVALUATION_TIMEOUT,
        }:
            raise RuntimeError(f"La evaluación del Genie terminó con estado {status.value}")
        time.sleep(polling_interval_seconds)

    raise RuntimeError("La evaluación del Genie no terminó a tiempo")


def _get_remote_benchmark_ids(client: WorkspaceClient, genie_space_id: str) -> set[str]:
    """Devuelve los IDs de benchmark que existen actualmente en el espacio remoto."""
    space = client.genie.get_space(genie_space_id, include_serialized_space=True)
    serialized_space = getattr(space, "serialized_space", None)
    if not serialized_space:
        return set()
    try:
        parsed = json.loads(serialized_space)
    except json.JSONDecodeError:
        return set()

    questions = parsed.get("benchmarks", {}).get("questions", [])
    if not isinstance(questions, list):
        return set()
    ids: set[str] = set()
    for question in questions:
        if not isinstance(question, dict):
            continue
        benchmark_id = str(question.get("id", "")).strip()
        if benchmark_id:
            ids.add(benchmark_id)
    return ids


def _resolve_eval_space_id(
    client: WorkspaceClient,
    base_space_id: str,
    benchmark_ids: list[str],
    genie_json_path: Path,
) -> tuple[str, bool]:
    """Usa el espacio remoto o crea uno temporal si faltan IDs de benchmark."""
    remote_ids = _get_remote_benchmark_ids(client, base_space_id)
    if set(benchmark_ids).issubset(remote_ids):
        return base_space_id, False

    base_space = client.genie.get_space(base_space_id)
    warehouse_id = base_space.warehouse_id
    if not warehouse_id:
        raise ValueError("El Genie no tiene warehouse_id para crear un espacio temporal")

    serialized_space = genie_json_path.read_text(encoding="utf-8")
    temp_space = client.genie.create_space(
        warehouse_id=warehouse_id,
        serialized_space=serialized_space,
        title=f"{(base_space.title or 'genie')}_bench_eval_{int(time.time())}",
        parent_path=base_space.parent_path,
        description="Espacio temporal para evaluación de benchmarks del pipeline",
    )
    return temp_space.space_id, True


def _evaluate_registered_benchmarks(
    client: WorkspaceClient,
    genie_space_id: str,
    benchmarks_with_id: list[dict[str, str]],
    genie_json_path: Path,
) -> list[dict[str, Any]]:
    """Evalúa benchmarks con la evaluación oficial de Genie."""
    benchmark_ids = [benchmark["id"] for benchmark in benchmarks_with_id if benchmark["id"]]
    if not benchmark_ids:
        return []

    benchmark_lookup = {benchmark["id"]: benchmark for benchmark in benchmarks_with_id}
    eval_space_id, created_temp_space = _resolve_eval_space_id(
        client,
        genie_space_id,
        benchmark_ids,
        genie_json_path,
    )
    run = client.genie.genie_create_eval_run(
        space_id=eval_space_id,
        benchmark_question_ids=benchmark_ids,
    )
    eval_run_id = run.eval_run_id
    try:
        _wait_for_eval_run(client, eval_space_id, eval_run_id)
        eval_results = client.genie.genie_list_eval_results(
            space_id=eval_space_id,
            eval_run_id=eval_run_id,
        ).eval_results or []

        results: list[dict[str, Any]] = []
        for eval_result in eval_results:
            benchmark_id = eval_result.benchmark_question_id
            benchmark = benchmark_lookup.get(benchmark_id)
            if not benchmark:
                continue
            detail = client.genie.genie_get_eval_result_details(
                space_id=eval_space_id,
                eval_run_id=eval_run_id,
                result_id=eval_result.result_id,
            )
            assessment = getattr(detail, "assessment", None)
            passed = assessment == GenieEvalAssessment.GOOD
            results.append(
                {
                    "id": benchmark_id,
                    "question": benchmark["question"],
                    "expected_sql": benchmark["expected_sql"],
                    "actual_sql": _extract_sql_from_eval_response(
                        getattr(detail, "actual_response", None)
                    ),
                    "score": 1.0 if passed else 0.0,
                    "passed": passed,
                    "evaluation_mode": "official_genie",
                }
            )
        return results
    finally:
        if created_temp_space:
            client.genie.trash_space(eval_space_id)


def evaluate_benchmarks(
    genie_space_id: str,
    benchmarks: list[dict[str, str]],
    profile: str,
    genie_json_path: Path,
) -> list[dict[str, Any]]:
    """Evalúa benchmarks con evaluación oficial de Genie."""
    if not benchmarks:
        raise ValueError("No hay benchmarks configurados para evaluar")

    client = WorkspaceClient(profile=profile)
    benchmarks_with_id = [benchmark for benchmark in benchmarks if benchmark.get("id")]
    if len(benchmarks_with_id) != len(benchmarks):
        raise ValueError("Todos los benchmarks deben tener ID para evaluación oficial")

    results = _evaluate_registered_benchmarks(
        client,
        genie_space_id,
        benchmarks_with_id,
        genie_json_path,
    )
    if not results:
        raise ValueError("No se pudieron evaluar benchmarks")
    return results


def write_report(results: list[dict[str, Any]], output_file: Path) -> float:
    """Guarda el reporte de benchmarks y devuelve el porcentaje de éxitos."""
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
        args.genie_json,
    )
    average_score = write_report(results, args.report)
    print(f"Benchmark score: {average_score:.4f} / threshold: {args.threshold:.4f}")

    if average_score < args.threshold:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
