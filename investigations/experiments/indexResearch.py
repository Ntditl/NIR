import os
import shutil
from benchmarks import indexCinema


def runIndexResearch(researchConfig: dict, outputDir: str):
    if researchConfig is None:
        researchConfig = {}
    sizes = researchConfig.get('sizes')
    repeats = researchConfig.get('repeats')
    if isinstance(sizes, list) and len(sizes) > 0:
        try:
            indexCinema.SIZES = [int(x) for x in sizes]
        except Exception:
            pass
    if isinstance(repeats, int) and repeats > 0:
        try:
            indexCinema.REPEATS = repeats
        except Exception:
            pass
    if not os.path.isdir(outputDir):
        os.makedirs(outputDir, exist_ok=True)
    indexCinema.runIndexBenchmarks()
    sourceDir = os.path.join(os.path.dirname(__file__), '..', 'results', 'index_bench')
    if os.path.isdir(sourceDir):
        names = os.listdir(sourceDir)
        for name in names:
            srcPath = os.path.join(sourceDir, name)
            dstPath = os.path.join(outputDir, name)
            try:
                shutil.copy2(srcPath, dstPath)
            except Exception:
                pass
    return outputDir
