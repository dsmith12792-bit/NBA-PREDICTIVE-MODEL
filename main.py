import os
import subprocess
from flask import Flask, jsonify

app = Flask(__name__)

@app.get("/")
def health():
    return jsonify({"ok": True, "service": "nba-predictive-model"}), 200

@app.post("/run")
def run_job():
    try:
        result = subprocess.run(
            ["python", "adv_update.py"],
            capture_output=True,
            text=True,
            check=False,
            env=os.environ.copy(),
        )
        return jsonify({
            "ok": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout[-4000:],
            "stderr": result.stderr[-4000:],
        }), (200 if result.returncode == 0 else 500)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

