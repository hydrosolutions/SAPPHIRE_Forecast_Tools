"""LOCKED acceptance test for milestone M2 — legacy pentad/decad writers retire.

The pentad/decad hydrograph rows move to ``preprocessing_runoff`` in M2, so the
forecast_library writers must stop writing rows: their bodies must no longer
reach the shared API sink ``_write_hydrograph_to_api``. The shared sink itself
stays live because the MONTH path (write_month_hydrograph_data) still uses it —
that path is only retired in M3.

This is a behavioural contract on "does the writer still write pentad/decad
rows", asserted at the function-body level so it is durable after removal (a
golden/live-write comparison cannot survive the old writer being deleted).

Fake station code '19999'; no real discharge values.
"""

import inspect

from iEasyHydroForecast import forecast_library as fl

SINK = "_write_hydrograph_to_api"


def test_pentad_writer_no_longer_reaches_the_api_sink():
    src = inspect.getsource(fl.write_pentad_hydrograph_data)
    assert f"{SINK}(" not in src, (
        "write_pentad_hydrograph_data still writes hydrograph rows to the API "
        "sink; M2 must retire the pentad write path (owner is preprocessing_runoff)."
    )


def test_decad_writer_no_longer_reaches_the_api_sink():
    src = inspect.getsource(fl.write_decad_hydrograph_data)
    assert f"{SINK}(" not in src, (
        "write_decad_hydrograph_data still writes hydrograph rows to the API "
        "sink; M2 must retire the decad write path (owner is preprocessing_runoff)."
    )


def test_month_path_and_shared_sink_stay_live_until_m3():
    # The shared sink must NOT be deleted in M2 — the month writer still uses it.
    assert callable(fl._write_hydrograph_to_api)
    month_src = inspect.getsource(fl.write_month_hydrograph_data)
    assert f"{SINK}(" in month_src, (
        "M2 must only silence the pentad/decad call paths; the month path stays live until M3."
    )
