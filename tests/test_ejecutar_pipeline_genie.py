"""Pruebas unitarias para la lógica pura de utils/ejecutar_pipeline_genie.py.

No requieren Spark ni credenciales de Databricks.
"""

import json

import ejecutar_pipeline_genie as pipeline
import pytest


def test_get_config_questions_returns_list():
    config = {"business_questions": ["¿Pregunta uno?", "¿Pregunta dos?"]}
    assert pipeline.get_config_questions(config) == ["¿Pregunta uno?", "¿Pregunta dos?"]


def test_get_config_questions_require_non_empty_raises():
    with pytest.raises(ValueError):
        pipeline.get_config_questions({}, require_non_empty=True)


def test_get_config_questions_rejects_non_string_items():
    with pytest.raises(ValueError):
        pipeline.get_config_questions({"business_questions": ["ok", 123]})


def test_get_config_sources_defaults_to_empty_list():
    assert pipeline.get_config_sources({}) == []


def test_get_config_benchmarks_from_dict_format():
    config = {
        "benchmarks": {
            "questions": [
                {
                    "id": "",
                    "question": ["¿Cuántos clientes activos hay?"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                }
            ]
        }
    }
    benchmarks = pipeline.get_config_benchmarks(config)
    assert len(benchmarks) == 1
    assert benchmarks[0]["question"] == ["¿Cuántos clientes activos hay?"]
    assert benchmarks[0]["answer"][0]["content"] == ["SELECT 1"]
    assert "id" not in benchmarks[0]  # id vacío no es válido: se autogenera después


def test_get_config_benchmarks_accepts_expected_sql_shortcut():
    config = {
        "benchmarks": [
            {"question": "¿Total de ventas?", "expected_sql": "SELECT SUM(1)"},
        ]
    }
    benchmarks = pipeline.get_config_benchmarks(config)
    assert benchmarks[0]["answer"] == [{"format": "SQL", "content": ["SELECT SUM(1)"]}]


def test_get_config_benchmarks_requires_question():
    with pytest.raises(ValueError):
        pipeline.get_config_benchmarks({"benchmarks": [{"expected_sql": "SELECT 1"}]})


def test_get_config_benchmarks_requires_sql():
    with pytest.raises(ValueError):
        pipeline.get_config_benchmarks({"benchmarks": [{"question": "¿Algo?"}]})


def test_get_config_benchmarks_rejects_invalid_container():
    with pytest.raises(ValueError):
        pipeline.get_config_benchmarks({"benchmarks": "no-valido"})


def test_build_metric_view_name_strips_geniespace_suffix(tmp_path):
    path = tmp_path / "Mi Genie Raro!.geniespace.json"
    assert pipeline.build_metric_view_name(path) == "mv_mi_genie_raro"


def test_build_metric_view_name_keeps_mv_prefix(tmp_path):
    path = tmp_path / "mv_ya_prefijado.geniespace.json"
    assert pipeline.build_metric_view_name(path) == "mv_ya_prefijado"


def test_resolve_run_validation_prefers_run_validate_key():
    assert pipeline.resolve_run_validation({"run_validate": False, "run_validation": True}, False) is False


def test_resolve_run_validation_falls_back_to_alias():
    assert pipeline.resolve_run_validation({"run_validation": False}, False) is False


def test_resolve_run_validation_defaults_true_without_config():
    assert pipeline.resolve_run_validation(None, use_interactive_prompt=False) is True


def test_resolve_refactor_forces_false_without_validation():
    assert pipeline.resolve_refactor({"refactor": True}, should_validate=False, use_interactive_prompt=False) is False


def test_resolve_revert_on_failed_benchmark_defaults_true():
    assert pipeline.resolve_revert_on_failed_benchmark(None) is True
    assert pipeline.resolve_revert_on_failed_benchmark({"revert_on_failed_benchmark": False}) is False


def test_resolve_metric_view_destination_valid():
    assert pipeline.resolve_metric_view_destination({"metric_view_destination": " brz_dev . default "}) == "brz_dev.default"


@pytest.mark.parametrize("destination", ["solo_catalogo", "a.b.c", ""])
def test_resolve_metric_view_destination_invalid(destination):
    with pytest.raises(ValueError):
        pipeline.resolve_metric_view_destination({"metric_view_destination": destination})


def test_resolve_metric_view_destination_requires_config():
    with pytest.raises(ValueError):
        pipeline.resolve_metric_view_destination(None)


def test_is_valid_benchmark_id():
    assert pipeline._is_valid_benchmark_id("a" * 32) is True
    assert pipeline._is_valid_benchmark_id("no-es-un-id") is False


def test_autogenerate_empty_ids_fills_missing_and_blank_ids():
    payload = {
        "id": "",
        "children": [{"id": "already-set"}, {"id": None}, {"no_id_field": True}],
    }
    pipeline._autogenerate_empty_ids(payload)
    assert pipeline._is_valid_benchmark_id(payload["id"])
    assert payload["children"][0]["id"] == "already-set"
    assert pipeline._is_valid_benchmark_id(payload["children"][1]["id"])
    assert "id" not in payload["children"][2]


def _write_genie_json(path, benchmarks=None):
    payload = {
        "version": 2,
        "data_sources": {"tables": []},
        "benchmarks": {"questions": benchmarks or []},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_merge_benchmarks_into_genie_json_adds_new_and_dedupes(tmp_path):
    json_file = _write_genie_json(
        tmp_path / "genie.json",
        benchmarks=[
            {
                "id": "a" * 32,
                "question": ["¿Cuántos clientes hay?"],
                "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
            }
        ],
    )
    configured = [
        {
            "question": ["¿Cuántos clientes hay?"],
            "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
        },
        {
            "question": ["¿Ventas totales?"],
            "answer": [{"format": "SQL", "content": ["SELECT SUM(1)"]}],
        },
    ]

    total, added = pipeline.merge_benchmarks_into_genie_json(
        json_file, pipeline_config=None, configured_benchmarks=configured, require_configured=False
    )

    assert total == 2  # el duplicado no se agrega de nuevo
    assert added == 1
    persisted = json.loads(json_file.read_text(encoding="utf-8"))
    assert len(persisted["benchmarks"]["questions"]) == 2
    for question in persisted["benchmarks"]["questions"]:
        assert pipeline._is_valid_benchmark_id(question["id"])


def test_merge_benchmarks_into_genie_json_requires_configured_for_new_genie(tmp_path):
    json_file = _write_genie_json(tmp_path / "genie.json")
    with pytest.raises(ValueError):
        pipeline.merge_benchmarks_into_genie_json(
            json_file, pipeline_config=None, configured_benchmarks=[], require_configured=True
        )


def test_merge_benchmarks_into_genie_json_rejects_empty_result(tmp_path):
    json_file = _write_genie_json(tmp_path / "genie.json")
    with pytest.raises(ValueError):
        pipeline.merge_benchmarks_into_genie_json(
            json_file, pipeline_config=None, configured_benchmarks=[], require_configured=False
        )
