import uuid

from essaim.metrics import RunMetrics
from essaim.world import World


def ev(run_id, seq, t, etype, agent=None, **payload):
    return {"schema_version": 1, "event_id": uuid.uuid4().hex, "run_id": run_id, "sequence": seq,
            "sim_time": t, "agent_id": agent, "event_type": etype, "payload": payload}


def start(run_id="r"):
    return ev(run_id, 1, 0.0, "run_started", seed=1, n_bees=10, llm_bees=[], sites=["A", "B", "C"])


def test_duplicate_events_do_not_double_metrics():
    w = World(2)
    events = []
    for _ in range(3000):
        w.step(0.1)
    events = w.drain()[0]
    once, twice = RunMetrics(w.run_id), RunMetrics(w.run_id)
    for e in events:
        once.apply(e)
    for e in events:
        assert twice.apply(e) is True
        assert twice.apply(dict(e)) is False  # même event_id reçu deux fois
    assert once.to_dict() == twice.to_dict()
    assert once.communications > 0
    # les soutiens calculés depuis les événements correspondent à l'état du monde
    expected = {s: sum(b.preference == s for b in w.bees) for s in "ABC"}
    assert once.supports() == expected


def test_consensus_requires_hold_time():
    m = RunMetrics("r", min_bees=8, hold_s=10)
    m.apply(start())
    seq = 1
    for bee in range(8):
        seq += 1
        m.apply(ev("r", seq, 5.0, "preference_changed", bee, old=None, new="A", score=0.9, source="règles"))
    for t in range(6, 15):
        seq += 1
        m.apply(ev("r", seq, float(t), "heartbeat"))
    assert m.to_dict()["consensus_time"] is None  # seulement 9 s de maintien
    seq += 1
    m.apply(ev("r", seq, 15.0, "heartbeat"))
    d = m.to_dict()
    assert d["consensus_time"] == 15.0 and d["consensus_site"] == "A" and d["consensus_now"]
    assert d["top_fraction"] == 0.8 and d["undecided"] == 2
    # rupture : une abeille change d'avis à t=20, consensus perdu
    for t in range(16, 21):
        seq += 1
        m.apply(ev("r", seq, float(t), "heartbeat"))
    seq += 1
    m.apply(ev("r", seq, 20.0, "preference_changed", 0, old="A", new="B", score=0.95, source="règles"))
    for t in range(21, 26):
        seq += 1
        m.apply(ev("r", seq, float(t), "heartbeat"))
    d = m.to_dict()
    assert d["consensus_breaks"] == 1 and not d["consensus_now"]
    assert d["consensus_stability"] == 0.5  # tenu 5 s sur 10 s depuis le consensus


def test_run_can_end_without_consensus():
    m = RunMetrics("r")
    m.apply(start())
    m.apply(ev("r", 2, 100.0, "run_ended", reason="durée maximale atteinte"))
    d = m.to_dict()
    assert d["consensus_time"] is None and d["ended"] == "durée maximale atteinte" and d["undecided"] == 10


def test_events_of_other_runs_are_ignored():
    m = RunMetrics("r")
    m.apply(start())
    assert m.apply(ev("autre", 2, 1.0, "preference_changed", 0, old=None, new="A")) is False
    assert m.supports()["A"] == 0
