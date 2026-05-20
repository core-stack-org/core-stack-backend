from pathlib import Path
import subprocess


BASE_DIR = Path(__file__).resolve().parent.parent

R_SCRIPT = BASE_DIR / "drought" / "drought_spei.R"


def run_spei_pipeline(state_safe=None, input_file=None, output_dir=None):

    command = [
        "Rscript",
        str(R_SCRIPT),
        state_safe,
        input_file,
        output_dir,
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
