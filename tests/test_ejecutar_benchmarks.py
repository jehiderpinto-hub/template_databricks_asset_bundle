"""Pruebas unitarias para la lógica pura de utils/ejecutar_benchmarks.py."""

import json

import ejecutar_benchmarks as benchmarks_module
import pytest


def _write_genie_json(path, questions):
    path.write_text(
        json.dumps({"benchmarks": {"questions": questions}}), encoding="utf-8"
    )
    return path


def test_extract_benchmarks_dedupes_by_question_and_sql(tmp_path):
    json_file = _write_genie_json(
        tmp_path / "genie.json",
        questions=[
            {
                "id": "a" * 32,
                "question": ["¿Total de clientes?"],
                "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
            },
            {
                "id": "b" * 32,
                "question": ["  ¿TOTAL de   clientes?  "],
                "answer": [{"format": "SQL", "content": ["select   1"]}],
            },
            {
                "id": "c" * 32,
                "question": ["¿Ventas totales?"],
                "answer": [{"format": "SQL", "content": ["SELECT SUM(1)"]}],
            },
        ],
    )

    extracted = benchmarks_module.extract_benchmarks(json_file)

    assert len(extracted) == 2
    assert extracted[0]["question"] == "¿Total de clientes?"
    assert extracted[1]["question"] == "¿Ventas totales?"


def test_extract_benchmarks_ignores_entries_without_sql(tmp_path):
    json_file = _write_genie_json(
        tmp_path / "genie.json",
        questions=[{"id": "a" * 32, "question": ["¿Sin SQL?"], "answer": []}],
    )
    assert benchmarks_module.extract_benchmarks(json_file) == []


def test_write_report_computes_pass_rate(tmp_path):
    output_file = tmp_path / "report.json"
    results = [
        {"passed": True},
        {"passed": True},
        {"passed": False},
    ]

    pass_rate = benchmarks_module.write_report(results, output_file)

    assert pass_rate == pytest.approx(2 / 3)
    persisted = json.loads(output_file.read_text(encoding="utf-8"))
    assert persisted["passed_benchmarks"] == 2
    assert persisted["benchmarks_total"] == 3


def test_benchmark_signature_normalizes_whitespace_and_case():
    signature_a = benchmarks_module._benchmark_signature("¿Cuánto?", "SELECT   1")
    signature_b = benchmarks_module._benchmark_signature("  ¿cuánto?  ", "select 1")
    assert signature_a == signature_b
