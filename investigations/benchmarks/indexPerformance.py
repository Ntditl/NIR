from lib.db.connection import getDbConnection
import csv
import timeit
from typing import List, Dict, Any
import os
import matplotlib.pyplot as plt


def measureIndexPerformance(
    indexConfig: List[Dict[str, Any]],
    outputCsvPath: str
) -> None:
    # deprecated: merged into indexCinema.measureGenericIndexSet
    pass
