import os
import matplotlib.pyplot as plt

class PlotBuilder:
    def __init__(self, saveDirectory: str = 'plots'):
        self.saveDirectory = saveDirectory
        os.makedirs(self.saveDirectory, exist_ok=True)
        self.colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
        self.linestyles = ['-', '--', '-.', ':']
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
        fig, ax = plt.subplots(figsize=(12, 8))
        seriesItems = list(series.items())
        usedCombos = []
        for i in range(len(seriesItems)):
            label, data = seriesItems[i]
            xValues, yValues = data
            comboFound = False
            for c in self.colors:
                if comboFound:
                    break
                for ls in self.linestyles:
                    if (c, ls) not in usedCombos:
                        color = c
                        linestyle = ls
                        usedCombos.append((c, ls))
                        comboFound = True
                        break
            if not comboFound:
                color = self.colors[i % len(self.colors)]
                linestyle = self.linestyles[i % len(self.linestyles)]
            marker = None
            if len(xValues) < 10:
                marker = self.markers[i % len(self.markers)]
            ax.plot(xValues, yValues, label=label, color=color, linestyle=linestyle, marker=marker)
        ax.set_title(chartTitle)
        ax.set_xlabel(xAxisLabel)
        ax.set_ylabel(yAxisLabel)
        ax.legend()
        ax.grid(True)
        extension = 'png' if isRaster else 'svg'
        filePath = os.path.join(self.saveDirectory, f"{fileName}.{extension}")
        fig.savefig(filePath)
        plt.close(fig)
        return filePath
