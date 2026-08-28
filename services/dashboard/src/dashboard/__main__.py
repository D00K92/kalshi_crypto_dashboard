"""Module entrypoint for ``python -m dashboard``."""

from dashboard.app import app


def main() -> None:
    import os

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")), debug=False)


if __name__ == "__main__":
    main()
