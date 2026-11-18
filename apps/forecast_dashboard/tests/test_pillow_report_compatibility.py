"""Exercise the Pillow-backed PNG export used by skill inspection reports."""

from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image


def test_skill_report_png_encoding_and_chart_labels(monkeypatch):
    """Check PNG decoding, dimensions, opacity, nonblank pixels, and labels."""
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "dev_code"))
    from inspect_skill import plot_skill_metric

    figure, axis = plt.subplots(figsize=(4, 3), dpi=100)
    metrics = pd.DataFrame(
        {"model_short": ["LR", "LR"], "pentad": [1, 2], "accuracy": [65.0, 90.0]}
    )

    plot_skill_metric(metrics, "accuracy", "synthetic", "pentad", ax=axis)
    output = BytesIO()
    try:
        figure.savefig(output, format="png", dpi=150)
    finally:
        plt.close(figure)
    output.seek(0)

    with Image.open(output) as image:
        image.load()
        assert image.format == "PNG"
        assert image.size == (600, 450)
        assert image.mode == "RGBA"
        assert image.getextrema()[3] == (255, 255)
        assert any(low < high for low, high in image.getextrema()[:3])
    assert axis.get_ylabel() == "Accuracy (%)"
    assert axis.get_legend_handles_labels()[1] == ["LR", "Accuracy = 80%"]
