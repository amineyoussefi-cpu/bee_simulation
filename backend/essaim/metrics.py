"""Indicateurs d'un run, calculés uniquement à partir des événements Kafka (jamais depuis le monde).

Consensus : au moins `min_bees` abeilles soutiennent le même site pendant `hold_s` secondes simulées.
Stabilité : part du temps écoulé depuis le premier consensus pendant laquelle ce même site garde le seuil.
"""


class RunMetrics:
    def __init__(self, run_id: str, min_bees: int = 8, hold_s: float = 10.0):
        self.run_id = run_id
        self.min_bees, self.hold_s = min_bees, hold_s
        self.n_bees = 0
        self.sites: list[str] = []
        self.preferences: dict[int, str | None] = {}
        self.seen: set[str] = set()
        self.last_sequence = 0
        self.sim_time = 0.0
        self.events_applied = 0
        self.communications = 0
        self.candidate_site: str | None = None
        self.candidate_since: float | None = None
        self.consensus_site: str | None = None
        self.consensus_time: float | None = None
        self.time_after_consensus = 0.0
        self.time_held_after_consensus = 0.0
        self.breaks = 0
        self.ended: str | None = None

    def apply(self, event: dict) -> bool:
        """Applique un événement ; retourne False s'il a déjà été vu (doublon ignoré)."""
        if event["run_id"] != self.run_id or event["event_id"] in self.seen:
            return False
        self.seen.add(event["event_id"])
        self.events_applied += 1
        self.last_sequence = max(self.last_sequence, event["sequence"])
        self._advance(event["sim_time"])
        etype, payload = event["event_type"], event["payload"]
        if etype == "run_started":
            self.n_bees = payload["n_bees"]
            self.sites = payload["sites"]
            self.preferences = {i: None for i in range(self.n_bees)}
        elif etype == "preference_changed":
            self.preferences[event["agent_id"]] = payload["new"]
            self._update_candidate()
        elif etype == "communication":
            self.communications += 1
        elif etype == "run_ended":
            self.ended = payload.get("reason")
        return True

    def supports(self) -> dict[str, int]:
        counts = {s: 0 for s in self.sites}
        for site in self.preferences.values():
            if site is not None:
                counts[site] = counts.get(site, 0) + 1
        return counts

    def _top(self) -> tuple[str | None, int]:
        counts = self.supports()
        if not counts:
            return None, 0
        site = max(counts, key=counts.get)
        return site, counts[site]

    def _advance(self, t: float) -> None:
        if t <= self.sim_time:
            return
        site, n = self._top()
        holding = n >= self.min_bees
        if (holding and self.consensus_time is None and self.candidate_since is not None
                and t - self.candidate_since >= self.hold_s):
            self.consensus_site = self.candidate_site
            self.consensus_time = round(self.candidate_since + self.hold_s, 2)
        if self.consensus_time is not None:
            start = max(self.sim_time, self.consensus_time)
            if t > start:
                self.time_after_consensus += t - start
                if holding and site == self.consensus_site:
                    self.time_held_after_consensus += t - start
        self.sim_time = t

    def _update_candidate(self) -> None:
        site, n = self._top()
        if n >= self.min_bees:
            if site != self.candidate_site:
                self.candidate_site, self.candidate_since = site, self.sim_time
        else:
            if self.consensus_time is not None and self.candidate_site == self.consensus_site:
                self.breaks += 1
            self.candidate_site = self.candidate_since = None

    def to_dict(self) -> dict:
        counts = self.supports()
        site, n = self._top()
        decided = sum(counts.values())
        stability = (round(self.time_held_after_consensus / self.time_after_consensus, 3)
                     if self.time_after_consensus > 0 else None)
        return {
            "run_id": self.run_id,
            "sim_time": round(self.sim_time, 2),
            "last_sequence": self.last_sequence,
            "events_applied": self.events_applied,
            "supports": counts,
            "undecided": self.n_bees - decided,
            "top_site": site if n > 0 else None,
            "top_fraction": round(n / self.n_bees, 3) if self.n_bees else 0.0,
            "consensus_rule": f">= {self.min_bees}/{self.n_bees} pendant {self.hold_s:g} s simulées",
            "consensus_now": n >= self.min_bees and self.candidate_since is not None
                             and self.sim_time - self.candidate_since >= self.hold_s,
            "consensus_site": self.consensus_site,
            "consensus_time": self.consensus_time,
            "consensus_stability": stability,
            "consensus_breaks": self.breaks,
            "communications": self.communications,
            "ended": self.ended,
        }
