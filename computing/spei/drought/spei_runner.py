from pathlib import Path
import subprocess


BASE_DIR = Path(__file__).resolve().parent.parent

R_SCRIPT = BASE_DIR / "drought" / "drought_spei.R"


def run_spei_pipeline(aez=None):
    print(aez)
    command = [
        "Rscript",
        str(R_SCRIPT),
        aez,
    ]

    print("COMMAND:", command)

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    print("RETURN CODE:", result.returncode)
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise Exception(
            f" R Script Failed COMMAND:{command} RETURN CODE:{result.returncode} STDOUT:{result.stdout} STDERR:{result.stderr}"
        )

    return result.stdout
