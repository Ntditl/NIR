import os
import shutil
from lib.visualization.plots import PlotBuilder

def fail(msg):
    raise RuntimeError(msg)

def check(cond, msg):
    if not cond:
        fail(msg)

def run():
    print('plots_manual start')
    outDir = 'tmp_plots_manual'
    if os.path.isdir(outDir):
        shutil.rmtree(outDir)
    builder = PlotBuilder(outDir)
    series = {
        's1': ([1,2,3,4],[10,20,15,25]),
        's2': ([1,2,3,4],[5,0,7,9])
    }
    pathVector = builder.buildChart(series, 'Title', 'X', 'Y', fileName='chart_vec', isRaster=False, logY=False)
    check(os.path.isfile(pathVector), 'vector file exists')
    pathRaster = builder.buildChart(series, 'TitleLog', 'X', 'Y', fileName='chart_rast', isRaster=True, logY=True)
    check(os.path.isfile(pathRaster), 'raster file exists')
    print('plots_manual ok')

if __name__ == '__main__':
    run()

