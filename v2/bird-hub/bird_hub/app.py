from __future__ import annotations

import os

from flask import Flask, jsonify


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["APP_VARIANT"] = os.getenv("BIRD_MONITOR_APP_VARIANT", "v2-bird-hub")
    app.config["APP_COMMIT"] = os.getenv("BIRD_MONITOR_APP_COMMIT", "unknown")

    @app.get("/")
    def index() -> str:
        return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>V2 Bird Hub</title>
    <style>
      body {{ font-family: sans-serif; margin: 2rem; line-height: 1.5; }}
      main {{ max-width: 48rem; }}
      code {{ background: #f4f4f4; padding: 0.15rem 0.35rem; border-radius: 0.25rem; }}
    </style>
  </head>
  <body>
    <main>
      <h1>V2 Bird Hub</h1>
      <p>This is the new hub scaffold.</p>
      <p>Variant: <code>{app.config["APP_VARIANT"]}</code></p>
      <p>Commit: <code>{app.config["APP_COMMIT"]}</code></p>
    </main>
  </body>
</html>
"""

    @app.get("/api/status")
    def status():
        return jsonify(
            {
                "app": {
                    "commit": app.config["APP_COMMIT"],
                    "variant": app.config["APP_VARIANT"],
                },
                "service": {
                    "started": True,
                    "message": "V2 bird-hub scaffold is installed.",
                },
            }
        )

    return app
