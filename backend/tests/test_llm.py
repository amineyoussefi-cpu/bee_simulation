import asyncio

from essaim.llm import OllamaClient, build_prompt, parse_letter
from essaim.world import World


def test_parse_letter():
    allowed = ["A", "B", "C"]
    assert parse_letter("B", allowed) == "B"
    assert parse_letter("[C]", allowed) == "C"
    assert parse_letter(" A. Je reste.", allowed) == "A"
    assert parse_letter("<think>hmm B?</think>\nC", allowed) == "C"
    assert parse_letter("D", allowed) is None
    assert parse_letter("Je choisis B", allowed) is None  # on exige la lettre seule


def llm_world() -> tuple[World, dict]:
    w = World(5, llm_bees={0})
    while not w.llm_requests:
        w.step(0.1)
    _, requests, _ = w.drain()
    return w, requests[0]


def test_llm_bee_waits_without_blocking_others():
    w, req = llm_world()
    bee = w.bees[0]
    assert bee.pending and req["bee"] == 0
    for _ in range(50):
        w.step(0.1)
    assert bee.pending  # toujours en attente, la simulation des autres continue
    assert w.time > req["t"]


def test_prompt_contains_only_bee_knowledge_and_options():
    w, req = llm_world()
    prompt = build_prompt(w.bees[0].to_view(), req)
    assert "[A]" in prompt and "uniquement par la lettre" in prompt
    assert "Creux du chêne" not in prompt and "0.957" not in prompt  # aucune propriété cachée


def test_valid_letter_applied_with_llm_source():
    w, req = llm_world()
    letter = req["options"][0]["letter"]
    assert w.resolve_llm(req["decision_id"], req["run_id"], letter, "llm", {"latency_ms": 12}) == "appliquée"
    assert w.bees[0].last_decision["source"] == "llm"
    assert w.drain()[2][0]["latency_ms"] == 12


def test_invalid_letter_falls_back_to_rules():
    w, req = llm_world()
    assert w.resolve_llm(req["decision_id"], req["run_id"], "Z", "llm", {}) == "appliquée"
    assert w.bees[0].last_decision["source"] == "fallback"
    assert "Z" in w.bees[0].last_decision["info"]["fallback_reason"]


def test_late_decision_from_old_run_is_ignored():
    w, req = llm_world()
    assert w.resolve_llm(req["decision_id"], "ancien-run", "A", "llm", {}) == "ignorée: ancien run"
    assert w.resolve_llm("inconnue", req["run_id"], "A", "llm", {}) == "ignorée: décision périmée"
    assert w.bees[0].pending  # toujours en attente de la vraie réponse


def test_unreachable_ollama_reports_error_quickly():
    client = OllamaClient("http://127.0.0.1:9", "modele", timeout_s=2, num_ctx=2048)
    assert asyncio.run(client.check()).startswith("hors ligne")
    info = asyncio.run(client.decide("x", ["A"]))
    assert info["letter"] is None and info["error"] and info["latency_ms"] < 2500


def test_client_against_stub_ollama(stub_ollama):
    url, _ = stub_ollama
    client = OllamaClient(url, "stub-model", timeout_s=1, num_ctx=2048)
    assert asyncio.run(client.check()) == "prêt"
    info = asyncio.run(client.decide("question", ["A", "B"]))
    assert info["letter"] == "B" and info["tokens_available"] == 1748 and info["latency_ms"] > 0
    slow = asyncio.run(client.decide("LENT", ["A", "B"]))
    assert slow["letter"] is None and slow["error"].startswith("timeout")
    assert asyncio.run(OllamaClient(url, "autre", 1, 2048).check()) == "modèle absent"
