from .app import create_app


def main() -> None:
    app = create_app()
    app.run(host=app.config["HOST"], port=app.config["PORT"])


if __name__ == "__main__":
    main()
