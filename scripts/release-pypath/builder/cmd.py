import subprocess
import sys


def stream_output(cmd, cwd=None) -> None:
    """Stream the output to stdout as the command is running"""
    try:
        subprocess.run(cmd, cwd=cwd, check=True, stdout=None, stderr=None)
    except subprocess.CalledProcessError as exc:
        print(f"Command {exc.cmd} failed")
        if exc.output:
            print(exc.output.decode("utf-8"))
        if exc.stderr:
            print(exc.stderr.decode("utf-8"), file=sys.stderr)
        raise


def collect_output(cmd, cwd=None, stderr=subprocess.PIPE, check=True) -> str:
    """Collect stdout and return it as a str"""
    try:
        result = subprocess.run(
            cmd, cwd=cwd, check=check, stdout=subprocess.PIPE, stderr=stderr
        )
    except subprocess.CalledProcessError as exc:
        print(f"Command {exc.cmd} failed")
        if exc.output:
            print(exc.output.decode("utf-8"))
        if exc.stderr:
            print(exc.stderr.decode("utf-8"), file=sys.stderr)
        raise
    return result.stdout.decode("utf-8")


# def run_command(cmd, cwd=None) -> None:
#     """Run a command, and print the stdout/stderr after it finishes."""
#     result = collect_output(cmd, stderr=subprocess.STDOUT, cwd=cwd)
#     print(result)
