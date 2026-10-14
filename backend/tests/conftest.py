import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def stub_ollama():
    """Faux serveur Ollama local (model 'stub-model', répond 'B' ; 'LENT' dans le prompt => 1,5 s)."""
    class Stub(BaseHTTPRequestHandler):
        def do_GET(self):
            self._reply({"models": [{"name": "stub-model"}]})

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            Stub.prompts.append(body["prompt"])
            time.sleep(Stub.delay if "LENT" in body["prompt"] or Stub.always_slow else 0)
            self._reply({"response": "B", "prompt_eval_count": 300, "eval_count": 2})

        def _reply(self, data):
            raw = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    Stub.prompts, Stub.delay, Stub.always_slow = [], 1.5, False
    server = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", Stub
    server.shutdown()
