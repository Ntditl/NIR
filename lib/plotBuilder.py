import os
import matplotlib.pyplot as plt

class PlotBuilder:
    @staticmethod
    def buildLineChart(
        xValues,
        yValues,
        seriesLabel: str,
        chartTitle: str,
        xAxisLabel: str,
        yAxisLabel: str,
        saveDirectory: str = 'plots',
        fileName: str = 'chart',
        rasterFormat: bool = False
    ):
        os.makedirs(saveDirectory, exist_ok=True)
        fig, ax = plt.subplots()
        markerStyle = 'o' if len(xValues) < 10 else None
        ax.plot(xValues, yValues, label=seriesLabel, linestyle='-', marker=markerStyle)
        ax.set_title(chartTitle)
        ax.set_xlabel(xAxisLabel)
        ax.set_ylabel(yAxisLabel)
        ax.legend()
        extension = 'png' if rasterFormat else 'svg'
        filePath = os.path.join(saveDirectory, f"{fileName}.{extension}")
        fig.savefig(filePath, format=extension)
        plt.close(fig)
        return filePath

    @staticmethod
    def buildMultiLineChart(
        dataSeries: dict,
        chartTitle: str,
        xAxisLabel: str,
        yAxisLabel: str,
        saveDirectory: str = 'plots',
        fileName: str = 'multiline_chart',
        rasterFormat: bool = False
    ):
        os.makedirs(saveDirectory, exist_ok=True)
        fig, ax = plt.subplots()
        for seriesLabel, pair in dataSeries.items():
            xValues, yValues = pair
            markerStyle = 'o' if len(xValues) < 10 else None
            ax.plot(xValues, yValues, label=seriesLabel, linestyle='-', marker=markerStyle)
        ax.set_title(chartTitle)
        ax.set_xlabel(xAxisLabel)
        ax.set_ylabel(yAxisLabel)
        ax.legend()
        extension = 'png' if rasterFormat else 'svg'
        filePath = os.path.join(saveDirectory, f"{fileName}.{extension}")
        fig.savefig(filePath, format=extension)
        plt.close(fig)
        return filePath
