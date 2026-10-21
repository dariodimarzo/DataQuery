"""Run the tool as a package, also without installing it: ``python -m dataquery``,
with ``src`` on the Python path (see the README, Installation)."""

from .cli import main

if __name__ == "__main__":
    main()
