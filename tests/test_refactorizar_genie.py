"""Pruebas unitarias para la lógica pura de utils/refactorizar_genie.py."""

import pytest
import refactorizar_genie as refactor_module


def test_sanitize_metric_view_name_adds_mv_prefix():
    assert refactor_module._sanitize_metric_view_name("Ventas Netas!") == "mv_ventas_netas"


def test_sanitize_metric_view_name_keeps_existing_prefix():
    assert refactor_module._sanitize_metric_view_name("mv_ya_listo") == "mv_ya_listo"


def test_sanitize_metric_view_name_falls_back_when_empty():
    assert refactor_module._sanitize_metric_view_name("!!!") == "mv_genie_assessment"


def test_sanitize_identifier_strips_accents_and_symbols():
    assert refactor_module._sanitize_identifier("Precio Promedio (USD)") == "precio_promedio_usd"


def test_sanitize_identifier_prefixes_leading_digit():
    assert refactor_module._sanitize_identifier("2024_meta") == "_2024_meta"


def test_build_metric_view_name_prefers_explicit_name():
    proposal = {"name": "Ventas por zona", "source": "cat.schema.tabla"}
    assert refactor_module.build_metric_view_name("base", proposal) == "mv_ventas_por_zona"


def test_build_metric_view_name_falls_back_to_source_table():
    proposal = {"source": "cat.schema.facturacion_hnd"}
    assert refactor_module.build_metric_view_name("base", proposal) == "mv_facturacion_hnd"


def test_build_metric_view_name_falls_back_to_base_name():
    proposal = {}
    assert refactor_module.build_metric_view_name("mi_genie.geniespace.json", proposal) == "mv_mi_genie_geniespace"


def test_parse_destination_requires_two_parts():
    assert refactor_module._parse_destination(" brz_dev . default ") == ("brz_dev", "default")
    with pytest.raises(ValueError):
        refactor_module._parse_destination("solo_catalogo")
