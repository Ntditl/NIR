import pytest
import os
import tempfile
from lib.visualization.plots import PlotBuilder

def test_plot_builder_creates_directory():
    temp_dir = tempfile.mkdtemp()
    builder = PlotBuilder(temp_dir)
    assert os.path.exists(temp_dir)

def test_plot_builder_creates_chart():
    temp_dir = tempfile.mkdtemp()
    builder = PlotBuilder(temp_dir)

    series = {
        'Series 1': ([1, 2, 3, 4, 5], [1.0, 2.5, 3.2, 4.1, 5.0]),
        'Series 2': ([1, 2, 3, 4, 5], [0.5, 1.2, 2.0, 3.5, 4.8])
    }

    file_path = builder.buildChart(
        series,
        'Test Chart',
        'X Axis',
        'Y Axis',
        'test_chart',
        isRaster=True
    )

    assert os.path.exists(file_path)
    assert file_path.endswith('.png')

def test_plot_builder_vector_format():
    temp_dir = tempfile.mkdtemp()
    builder = PlotBuilder(temp_dir)

    series = {
        'Data': ([1, 2, 3], [1.0, 2.0, 3.0])
    }

    file_path = builder.buildChart(
        series,
        'Vector Test',
        'X',
        'Y',
        'vector_chart',
        isRaster=False
    )

    assert os.path.exists(file_path)
    assert file_path.endswith('.svg')

def test_plot_builder_with_markers():
    temp_dir = tempfile.mkdtemp()
    builder = PlotBuilder(temp_dir)

    series = {
        'Small Series': ([1, 2, 3], [1.0, 2.0, 3.0])
    }

    file_path = builder.buildChart(
        series,
        'Markers Test',
        'X',
        'Y',
        'markers_chart',
        isRaster=True
    )

    assert os.path.exists(file_path)

