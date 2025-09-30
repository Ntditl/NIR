import os
from lib.visualization.plots import PlotBuilder

def testPlotBuilder():
    outDir = 'test_plots'
    builder = PlotBuilder(outDir)
    series = {
        'A': ([1,2,3],[0.001,0.002,0.003]),
        'B': ([1,2,3],[0.002,0.004,0.008])
    }
    p1 = builder.buildChart(series, 'T', 'X', 'Y', 'test_linear', True, logY=False)
    p2 = builder.buildChart(series, 'T log', 'X', 'Y', 'test_log', True, logY=True)
    if not os.path.isfile(p1):
        raise RuntimeError('linear plot missing')
    if not os.path.isfile(p2):
        raise RuntimeError('log plot missing')


def main():
    testPlotBuilder()
    print('Проверка PlotBuilder пройдена')

if __name__ == '__main__':
    main()

