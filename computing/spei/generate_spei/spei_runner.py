from pathlib import Path
import subprocess


BASE_DIR = Path(__file__).resolve().parent.parent

R_SCRIPT = BASE_DIR / "generate_spei" / "compute_spei.R"


def run_spei(aez=None, start_year=None, end_year=None):
    print(aez)
    command = ["Rscript", str(R_SCRIPT), str(aez), str(start_year), str(end_year)]

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
