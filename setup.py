import os
import platform
import subprocess
import sys


def run_command(command, cwd=None):
    """Run a shell command and return the exit code."""
    try:
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
        for line in process.stdout:
            print(line, end="")
        process.wait()
        return process.returncode
    except Exception as e:
        print(f"[ERROR] Exception occurred: {e}")
        return 1


def setup():
    print("[INFO] Starting AlphaMint universal installation...")

    # 1. Determine OS and Paths
    is_windows = platform.system() == "Windows"
    venv_dir = ".venv"

    if is_windows:
        python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
        pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
        activate_cmd = f"{venv_dir}\\Scripts\\activate"
    else:
        python_exe = os.path.join(venv_dir, "bin", "python")
        pip_exe = os.path.join(venv_dir, "bin", "pip")
        activate_cmd = f"source {venv_dir}/bin/activate"

    # 2. Create Virtual Environment
    if not os.path.exists(venv_dir):
        print(f"[INFO] Creating virtual environment in {venv_dir}...")
        # Use sys.executable to ensure we use the same python version
        if run_command(f'"{sys.executable}" -m venv {venv_dir}') != 0:
            print("[ERROR] Failed to create virtual environment.")
            return

    # 3. Configure Pip Mirror (Tencent with fallbacks)
    print("[INFO] Configuring pip with Tencent mirror and fallbacks...")
    primary_mirror = "https://mirrors.cloud.tencent.com/pypi/simple"
    fallback_mirror = "https://pypi.tuna.tsinghua.edu.cn/simple"

    # Set primary mirror
    run_command(f'"{python_exe}" -m pip config set global.index-url {primary_mirror}')
    # Add extra index for robustness
    run_command(f'"{python_exe}" -m pip config set global.extra-index-url {fallback_mirror}')

    # 4. Upgrade Pip and install build tools
    print("[INFO] Upgrading pip and installing build tools...")
    run_command(f'"{python_exe}" -m pip install --upgrade pip setuptools wheel')
    # Pre-install numpy to avoid build issues with pandas
    print("[INFO] Pre-installing numpy...")
    run_command(f'"{python_exe}" -m pip install numpy')

    # 5. Install Dependencies
    req_file = "requirements.txt"
    if os.path.exists(req_file):
        print(f"[INFO] Installing dependencies from {req_file}...")
        if run_command(f'"{python_exe}" -m pip install -r {req_file}') == 0:
            print("[SUCCESS] Dependencies installed successfully!")
        else:
            print("[ERROR] Failed to install dependencies.")
            return
    else:
        print(f"[WARNING] {req_file} not found. Skipping dependency installation.")

    print("\n" + "=" * 50)
    print("[SUCCESS] AlphaMint environment setup complete!")
    print(f"[INFO] To activate the environment, run:")
    if is_windows:
        print(f"    {activate_cmd}")
    else:
        print(f"    {activate_cmd}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    setup()
