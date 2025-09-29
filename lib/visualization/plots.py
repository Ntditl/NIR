import os
import matplotlib.pyplot as plt

TITLE_FONT_SCALE = 2.0
AXIS_LABEL_FONT_SCALE = 1.4

class PlotBuilder:
    def __init__(self, saveDirectory: str = 'plots'):
        self.saveDirectory = saveDirectory
        os.makedirs(self.saveDirectory, exist_ok=True)
        # новые цвета в фиксированном порядке
        self.colors = ["red", "green", "orange", "blue", "brown", "pink"]
        self.linestyles = ['-']  # один стиль чтобы не дублировать цвет
        self.markers = ['o', 's', 'v', '^', '<', '>', 'p', '*', 'D']

    def buildChart(
        self,
        series: dict,
        chartTitle: str,
        xAxisLabel: str,
        yAxisLabel: str,
        fileName: str = 'chart',
        isRaster: bool = False
    ):
        baseFontSize = plt.rcParams.get('font.size', 10)
        titleFontSize = int(baseFontSize * TITLE_FONT_SCALE)
        axisFontSize = int(baseFontSize * AXIS_LABEL_FONT_SCALE)
        fig, ax = plt.subplots(figsize=(12, 8))
        seriesItems = list(series.items())
        for i in range(len(seriesItems)):
            label, data = seriesItems[i]
            xValues, yValues = data
            color = self.colors[i % len(self.colors)]
            linestyle = '-'
            marker = None
            if len(xValues) < 10:
                marker = self.markers[i % len(self.markers)]
            ax.plot(xValues, yValues, label=label, color=color, linestyle=linestyle, marker=marker)
        ax.set_title(chartTitle, fontsize=titleFontSize)
        ax.set_xlabel(xAxisLabel, fontsize=axisFontSize)
        ax.set_ylabel(yAxisLabel, fontsize=axisFontSize)
        ax.tick_params(labelsize=axisFontSize)
        ax.legend(fontsize=axisFontSize)
        ax.grid(True)
        extension = 'png' if isRaster else 'svg'
        filePath = os.path.join(self.saveDirectory, f"{fileName}.{extension}")
        fig.savefig(filePath)
        plt.close(fig)
        return filePath
