import subprocess
import os

def transform():
    # Specify the path to your shell script
    script_path = "/home/chinhnv/2023-hn-NguyenVanChinh-GoogleadsApiDataCrawl/dags/python/transform.sh"

    # Execute the shell script
    subprocess.run(["sh", script_path])