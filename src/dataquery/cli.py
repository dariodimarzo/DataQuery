import sys
import subprocess
from pathlib import Path

def main():
    """CLI entry point for dataquery."""
    app_path = Path(__file__).parent / "app.py"
    # Launch Streamlit as a subprocess to avoid ScriptRunContext warnings
    sys.exit(subprocess.call([sys.executable, "-m", "streamlit", "run", str(app_path)] + sys.argv[1:]))

if __name__ == "__main__":
    main()
