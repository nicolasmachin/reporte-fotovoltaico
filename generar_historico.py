import os
import runpy


os.environ["PROCESS_ALL_MONTHS"] = "1"
os.environ["REBUILD_HISTORICO"] = "1"

runpy.run_path("main.py", run_name="__main__")
