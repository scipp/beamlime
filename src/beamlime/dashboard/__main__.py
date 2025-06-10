"""Dashboard package entry point."""

import sys

from .dev import main

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'dev':
        main()
    else:
        print("Usage: python -m beamlime.dashboard dev")
        sys.exit(1)
