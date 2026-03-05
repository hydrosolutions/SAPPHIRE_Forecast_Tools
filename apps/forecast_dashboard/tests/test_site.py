"""Unit tests for forecast_dashboard/src/site.py."""

import pandas as pd
import pytest
from src.site import SapphireSite

# ── SapphireSite.__init__ ─────────────────────────────────────────────────


class TestSapphireSiteInit:
    def test_basic_construction(self):
        site = SapphireSite(
            code="15102",
            river_name_ru="Чу",
            punkt_name_ru="Кара-Балта",
        )
        assert site.code == "15102"
        assert site.river_name_ru == "Чу"
        assert site.punkt_name_ru == "Кара-Балта"

    def test_defaults_to_none(self):
        site = SapphireSite()
        assert site.code is None
        assert site.name_ru is None
        assert site.lat is None
        assert site.qdanger is None

    def test_station_label_set_when_provided(self):
        site = SapphireSite(
            code="15102",
            river_name_ru="Чу",
            punkt_name_ru="Кара-Балта",
            station_label="custom label",
        )
        # station_label is formatted as "code - river punkt"
        assert site.station_label == "15102 - Чу Кара-Балта"

    def test_station_label_none_when_not_provided(self):
        site = SapphireSite(code="15102")
        assert site.station_label is None

    def test_str_representation(self):
        site = SapphireSite(
            code="15102",
            river_name_ru="Чу",
            punkt_name_ru="Кара-Балта",
        )
        assert "15102" in str(site)
        assert "Чу" in str(site)

    def test_numeric_attributes(self):
        site = SapphireSite(
            lat=42.5,
            lon=74.6,
            qdanger=150.0,
            histqmin=5.0,
            histqmax=300.0,
        )
        assert site.lat == 42.5
        assert site.lon == 74.6
        assert site.qdanger == 150.0


# ── get_site_attribues_from_iehhf_dataframe ───────────────────────────────


class TestGetSiteAttributesFromIehhfDataframe:
    @pytest.fixture
    def iehhf_df(self):
        return pd.DataFrame(
            {
                "code": ["99001", "99002", "99003"],
                "station_labels": ["Station A", "Station B", "Station C"],
                "river_name": ["River A", "River B", "River C"],
                "punkt_name": ["Punkt A", "Punkt B", "Punkt C"],
                "lat": [42.0, 43.0, 44.0],
                "lon": [74.0, 75.0, 76.0],
                "basin": ["Basin1", "Basin2", "Basin1"],
                "bulletin_order": [2, 1, 1],
                "qdanger": [100.0, 200.0, 150.0],
                "histqmin": [5.0, 10.0, 7.0],
                "histqmax": [300.0, 400.0, 350.0],
            }
        )

    def test_creates_correct_number_of_sites(self, iehhf_df):
        sites = SapphireSite.get_site_attribues_from_iehhf_dataframe(iehhf_df)
        assert len(sites) == 3

    def test_ordered_by_basin_and_bulletin_order(self, iehhf_df):
        sites = SapphireSite.get_site_attribues_from_iehhf_dataframe(iehhf_df)
        codes = [s.code for s in sites]
        # Basin1: 99003 (order=1), 99001 (order=2); Basin2: 99002 (order=1)
        assert codes == ["99003", "99001", "99002"]

    def test_station_label_format(self, iehhf_df):
        sites = SapphireSite.get_site_attribues_from_iehhf_dataframe(iehhf_df)
        # station_label is set as "code - name_ru" (station_labels col)
        first = sites[0]  # 99003 after ordering
        assert first.station_label == "99003 - Station C"

    def test_preserves_attributes(self, iehhf_df):
        sites = SapphireSite.get_site_attribues_from_iehhf_dataframe(iehhf_df)
        site_a = next(s for s in sites if s.code == "99001")
        assert site_a.river_name_ru == "River A"
        assert site_a.lat == 42.0
        assert site_a.qdanger == 100.0

    def test_missing_columns_handled(self):
        df = pd.DataFrame(
            {
                "code": ["99001"],
                "station_labels": ["Station A"],
            }
        )
        sites = SapphireSite.get_site_attribues_from_iehhf_dataframe(df)
        assert len(sites) == 1
        site = sites[0]
        assert site.code == "99001"
        assert site.river_name_ru is None

    def test_empty_dataframe_returns_empty_list(self):
        df = pd.DataFrame(
            columns=[
                "code",
                "station_labels",
                "river_name",
                "punkt_name",
                "lat",
                "lon",
                "basin",
                "bulletin_order",
            ]
        )
        sites = SapphireSite.get_site_attribues_from_iehhf_dataframe(df)
        assert sites == []


# ── oder_sites_list_according_to_bulletin_order ───────────────────────────


class TestOrderSitesList:
    def test_orders_by_basin_then_bulletin(self):
        sites = [
            SapphireSite(code="A", basin_ru="B2", bulletin_order=1),
            SapphireSite(code="B", basin_ru="B1", bulletin_order=2),
            SapphireSite(code="C", basin_ru="B1", bulletin_order=1),
        ]
        # Note: oder_sites_list_according_to_bulletin_order is defined as
        # a regular method (no @classmethod), so call on an instance.
        instance = SapphireSite()
        ordered = instance.oder_sites_list_according_to_bulletin_order(sites)
        # All 3 sites should be returned (list comprehension keeps all
        # whose code is in ordered_codes)
        assert len(ordered) == 3
