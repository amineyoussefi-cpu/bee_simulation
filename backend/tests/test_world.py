import pytest

from essaim.world import World, evaluate_site


def run(world: World, seconds: float) -> list[dict]:
    events = []
    for _ in range(int(seconds / 0.1)):
        world.step(0.1)
        events += world.drain()[0]
    return events


def test_evaluation_rule():
    assert evaluate_site(40, 1.0, 15) == 1.0
    assert evaluate_site(40, 0.9, 14) > evaluate_site(32, 0.8, 18) > evaluate_site(55, 0.6, 28)


def test_same_seed_same_outcome():
    a, b = World(7), World(7)
    run(a, 200), run(b, 200)
    assert [x.preference for x in a.bees] == [x.preference for x in b.bees]
    assert [(round(x.x, 3), x.state) for x in a.bees] == [(round(x.x, 3), x.state) for x in b.bees]


def test_no_initial_knowledge():
    w = World(1)
    assert all(not b.memory and b.preference is None for b in w.bees)


@pytest.mark.parametrize("seed", [1, 2, 3, 42])
def test_bee_only_knows_what_it_observed_or_received(seed):
    w = World(seed)
    events = run(w, 400)
    inspected = {(e["agent_id"], e["payload"]["site"]) for e in events if e["event_type"] == "site_inspected"}
    received = {(r, e["payload"]["site"]) for e in events if e["event_type"] == "communication"
                for r in e["payload"]["recipients"]}
    discovered = {(e["agent_id"], e["payload"]["site"]) for e in events if e["event_type"] == "site_discovered"}
    for bee in w.bees:
        for m in bee.memory:
            key = (bee.id, m["site"])
            if m["kind"] == "observation":
                assert key in inspected, f"abeille {bee.id} a une observation de {m['site']} sans l'avoir inspecté"
            else:
                assert key in received
        for site in w.sites:
            if (bee.id, site) not in inspected | received | discovered:
                assert site not in bee.known_site_ids(), f"abeille {bee.id} connaît {site} sans source"
        # une préférence ne vient que d'une inspection personnelle
        if bee.preference:
            assert (bee.id, bee.preference) in inspected


def test_inspection_requires_presence():
    w = World(1)
    with pytest.raises(PermissionError):
        w.inspect(w.bees[0], "A")  # l'abeille est sur la branche, loin de A


def test_blocked_site_is_learned_only_by_observation_or_communication():
    w = World(2)
    w.set_site_accessible("A", False)
    events = run(w, 400)
    blocked_inspections = {e["agent_id"] for e in events
                           if e["event_type"] == "site_inspected" and e["payload"]["site"] == "A"}
    for bee in w.bees:
        assert bee.preference != "A"
        for m in bee.memory:
            if m["site"] == "A" and m["kind"] == "observation":
                assert m["accessible"] is False and "score" not in m
                assert bee.id in blocked_inspections


def test_operator_toggle_emits_event():
    w = World(1)
    w.drain()
    w.set_site_accessible("B", False)
    w.set_site_accessible("B", False)  # sans effet
    events = w.drain()[0]
    assert [e["event_type"] for e in events] == ["site_access_changed"]


def test_event_envelope():
    w = World(3)
    events = run(w, 30)
    keys = {"schema_version", "event_id", "run_id", "sequence", "sim_time", "agent_id", "event_type", "payload"}
    assert all(set(e) == keys for e in events)
    seqs = [e["sequence"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    assert events[0]["event_type"] == "run_started"
