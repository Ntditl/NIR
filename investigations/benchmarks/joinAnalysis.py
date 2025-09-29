import csv
import os
import timeit
from typing import List, Dict, Any
from lib.db.connection import getDbConnection


def analyzeJoinPerformance(joinConfig: List[Dict[str, Any]], outputCsvPath: str) -> None:
    # deprecated merged into queryPerformance
    pass
