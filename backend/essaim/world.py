"""Monde simulé : branche, abris, abeilles. Pas de temps fixe, déterministe pour une graine donnée.

Règle d'isolement : une abeille ne lit que ses propres champs (mémoire, préférence, profil).
Les propriétés réelles d'un abri ne sortent du monde que par `World.inspect()`, appelé quand
l'abeille est physiquement sur le site.
"""
import math
import random
import uuid
from collections import deque
from dataclasses import dataclass, field

SCHEMA_VERSION = 1
STATES = ("REPOS", "EXPLORATION", "INSPECTION", "RETOUR", "DANSE")

WIDTH, HEIGHT = 1000.0, 600.0
BRANCH = (500.0, 300.0)
BRANCH_RADIUS = 30.0
DISCOVERY_RADIUS = 50.0
INSPECTION_TIME = 3.0
EXPLORATION_TIME = 15.0
REST_DECISION_INTERVAL = 2.0
MEMORY_SIZE = 8
DANCE_BASE_S = 15.0
DANCE_DECAY = 0.6
DANCE_MIN_S = 2.0
DANCE_ATTEND_PROB = 0.35
SWITCH_MARGIN = 0.05


def evaluate_site(volume_l: float, dryness: float, entrance_cm2: float) -> float:
    """Règle d'évaluation (0..1) : 50 % volume (idéal 40 L), 30 % sécheresse, 20 % entrée (idéale 15 cm²)."""
    volume_score = max(0.0, 1.0 - abs(volume_l - 40.0) / 40.0)
    entrance_score = max(0.0, 1.0 - abs(entrance_cm2 - 15.0) / 15.0)
    return round(0.5 * volume_score + 0.3 * dryness + 0.2 * entrance_score, 3)


@dataclass
class Site:
    id: str
    name: str
    x: float
    y: float
    volume_l: float
    dryness: float
    entrance_cm2: float
    accessible: bool = True


def default_sites() -> list[Site]:
    return [
        Site("A", "Creux du chêne", 840, 120, 40, 0.9, 14),   # score 0.957
        Site("B", "Nichoir", 160, 160, 32, 0.8, 18),           # score 0.800
        Site("C", "Fissure du rocher", 720, 500, 55, 0.6, 28), # score 0.520, le plus proche
    ]


@dataclass(frozen=True)
class Profile:
    name: str
    explore_rate: float    # probabilité/s de partir explorer quand elle est indécise
    follow_prob: float     # probabilité d'aller vérifier une recommandation reçue
    accept_threshold: float
    dance_vigor: float
    speed: float


PROFILES = {
    "eclaireuse": Profile("eclaireuse", 0.25, 0.3, 0.5, 1.2, 80),
    "prudente": Profile("prudente", 0.05, 0.5, 0.7, 0.8, 60),
    "suiveuse": Profile("suiveuse", 0.02, 0.8, 0.5, 1.0, 65),
}
PROFILE_PLAN = ["eclaireuse"] * 3 + ["prudente"] * 3 + ["suiveuse"] * 4


@dataclass
class Bee:
    id: int
    profile: Profile
    x: float
    y: float
    state: str = "REPOS"
    memory: deque = field(default_factory=lambda: deque(maxlen=MEMORY_SIZE))
    preference: str | None = None
    preference_score: float | None = None
    target: tuple[float, float] | None = None
    target_site: str | None = None
    timer: float = 0.0
    heading: float = 0.0
    dances_done: int = 0
    dance_site: str | None = None
    pending: dict | None = None
    last_decision: dict | None = None

    # --- connaissances propres (aucun accès au monde) ---
    def remember(self, entry: dict) -> None:
        self.memory.append(entry)

    def last_observation(self, site_id: str) -> dict | None:
        for m in reversed(self.memory):
            if m["kind"] == "observation" and m["site"] == site_id:
                return m
        return None

    def known_site_position(self, site_id: str) -> tuple[float, float] | None:
        for m in reversed(self.memory):
            if m["site"] == site_id and "x" in m:
                return (m["x"], m["y"])
        return None

    def known_site_ids(self) -> set[str]:
        return {m["site"] for m in self.memory}

    def unverified_recommendations(self) -> list[dict]:
        """Recommandations reçues pour un site positif, non vérifiées depuis, non déjà examinées."""
        recs = []
        for m in self.memory:
            if m["kind"] != "reçue" or m.get("considered"):
                continue
            obs = self.last_observation(m["site"])
            if obs and obs["t"] >= m["t"]:
                continue
            # recommandation positive pour un autre site, ou alerte sur mon propre site
            if (m["site"] == self.preference) == m["accessible"]:
                continue
            recs.append(m)
        return recs

    def to_view(self) -> dict:
        return {
            "id": self.id,
            "x": round(self.x, 1),
            "y": round(self.y, 1),
            "state": self.state,
            "profile": self.profile.name,
            "preference": self.preference,
            "preference_score": self.preference_score,
            "observations": [m for m in self.memory if m["kind"] == "observation"],
            "received": [m for m in self.memory if m["kind"] == "reçue"],
            "memory": list(self.memory),
            "dances_done": self.dances_done,
            "pending_decision": self.pending["kind"] if self.pending else None,
            "last_decision": self.last_decision,
        }


def _dist(ax, ay, bx, by) -> float:
    return math.hypot(ax - bx, ay - by)


class World:
    def __init__(self, seed: int, llm_bees: set[int] | None = None, n_bees: int = 10, run_id: str | None = None,
                 meta: dict | None = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.run_id = run_id or uuid.uuid4().hex[:12]
        self.time = 0.0
        self.sequence = 0
        self.llm_bees = llm_bees or set()
        self.sites = {s.id: s for s in default_sites()}
        self.bees = []
        for i in range(n_bees):
            bx, by = self._branch_spot()
            bee = Bee(i, PROFILES[PROFILE_PLAN[i % len(PROFILE_PLAN)]], bx, by)
            bee.timer = self.rng.uniform(0.5, REST_DECISION_INTERVAL)
            self.bees.append(bee)
        self.events: list[dict] = []          # enveloppes à publier sur Kafka
        self.llm_requests: list[dict] = []    # décisions à confier à Ollama
        self.decision_log: list[dict] = []    # décisions à sauvegarder dans SQLite
        self._next_heartbeat = 1.0
        self._decision_counter = 0
        self.emit(None, "run_started", {
            "seed": seed, "n_bees": n_bees, "llm_bees": sorted(self.llm_bees),
            "sites": [s.id for s in self.sites.values()], **(meta or {}),
        })

    # ------------------------------------------------------------------ événements
    def emit(self, agent_id: int | None, event_type: str, payload: dict) -> None:
        self.sequence += 1
        self.events.append({
            "schema_version": SCHEMA_VERSION,
            "event_id": uuid.uuid4().hex,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "sim_time": round(self.time, 2),
            "agent_id": agent_id,
            "event_type": event_type,
            "payload": payload,
        })

    def drain(self) -> tuple[list, list, list]:
        out = (self.events, self.llm_requests, self.decision_log)
        self.events, self.llm_requests, self.decision_log = [], [], []
        return out

    # ------------------------------------------------------------------ boucle
    def step(self, dt: float) -> None:
        self.time += dt
        for bee in self.bees:
            self._step_bee(bee, dt)
        if self.time >= self._next_heartbeat:
            self._next_heartbeat += 1.0
            self.emit(None, "heartbeat", {})

    def _step_bee(self, bee: Bee, dt: float) -> None:
        if bee.pending:  # attend une décision LLM : l'abeille reste sur place
            return
        if bee.state == "REPOS":
            bee.timer -= dt
            if bee.timer <= 0:
                bee.timer = REST_DECISION_INTERVAL
                self._request_decision(bee, "branche", self._branch_options(bee, just_returned=False))
        elif bee.state == "EXPLORATION":
            self._step_exploration(bee, dt)
        elif bee.state == "INSPECTION":
            bee.timer -= dt
            if bee.timer <= 0:
                self._finish_inspection(bee)
        elif bee.state == "RETOUR":
            if self._move(bee, dt):
                self._arrive_on_branch(bee)
        elif bee.state == "DANSE":
            self._step_dance(bee, dt)

    def _move(self, bee: Bee, dt: float) -> bool:
        tx, ty = bee.target
        d = _dist(bee.x, bee.y, tx, ty)
        step = bee.profile.speed * dt
        if d <= step:
            bee.x, bee.y = tx, ty
            return True
        bee.x += (tx - bee.x) / d * step
        bee.y += (ty - bee.y) / d * step
        return False

    def _step_exploration(self, bee: Bee, dt: float) -> None:
        bee.timer -= dt
        if bee.target_site:  # vol dirigé vers un site connu (vérification ou ré-inspection)
            if self._move(bee, dt):
                self._start_inspection(bee, bee.target_site)
            elif bee.timer <= 0:
                self._go_home(bee)
            return
        # exploration libre : marche aléatoire
        bee.heading += self.rng.gauss(0, 0.6) * dt * 3
        step = bee.profile.speed * dt
        bee.x += math.cos(bee.heading) * step
        bee.y += math.sin(bee.heading) * step
        if not (0 < bee.x < WIDTH):
            bee.heading = math.pi - bee.heading
            bee.x = min(max(bee.x, 1), WIDTH - 1)
        if not (0 < bee.y < HEIGHT):
            bee.heading = -bee.heading
            bee.y = min(max(bee.y, 1), HEIGHT - 1)
        for site in self.sites.values():
            if _dist(bee.x, bee.y, site.x, site.y) <= DISCOVERY_RADIUS:
                obs = bee.last_observation(site.id)
                if obs and self.time - obs["t"] < 60:
                    continue  # déjà inspecté récemment
                if site.id not in bee.known_site_ids():
                    self.emit(bee.id, "site_discovered", {"site": site.id, "x": site.x, "y": site.y})
                bee.target_site = site.id
                bee.target = (site.x, site.y)
                return
        if bee.timer <= 0:
            self._go_home(bee)

    def _start_inspection(self, bee: Bee, site_id: str) -> None:
        bee.state = "INSPECTION"
        bee.target_site = site_id
        bee.timer = INSPECTION_TIME

    def inspect(self, bee: Bee, site_id: str) -> dict:
        """Seul point où une abeille obtient les propriétés réelles d'un abri : elle doit y être."""
        site = self.sites[site_id]
        if _dist(bee.x, bee.y, site.x, site.y) > DISCOVERY_RADIUS + 1:
            raise PermissionError("inspection à distance interdite")
        obs = {"kind": "observation", "site": site.id, "t": round(self.time, 2), "x": site.x, "y": site.y,
               "accessible": site.accessible}
        if site.accessible:
            obs.update(volume_l=site.volume_l, dryness=site.dryness, entrance_cm2=site.entrance_cm2,
                       score=evaluate_site(site.volume_l, site.dryness, site.entrance_cm2))
        return obs

    def _finish_inspection(self, bee: Bee) -> None:
        obs = self.inspect(bee, bee.target_site)
        bee.remember(obs)
        self.emit(bee.id, "site_inspected", {k: v for k, v in obs.items() if k not in ("kind", "t")})
        if not obs["accessible"]:
            if bee.preference == obs["site"]:
                self._set_preference(bee, None, None, "observation")
            self._go_home(bee)
            return
        options = [{"action": "adopter", "site": obs["site"],
                    "label": f"Adopter l'abri {obs['site']} (score observé {obs['score']})"}]
        if bee.preference and bee.preference != obs["site"]:
            options.append({"action": "garder", "site": bee.preference,
                            "label": f"Garder ma préférence actuelle {bee.preference} (score {bee.preference_score})"})
        options.append({"action": "indecise", "site": None, "label": "Rester indécise (aucune préférence)"})
        self._request_decision(bee, "inspection", options)

    def _go_home(self, bee: Bee) -> None:
        bee.state = "RETOUR"
        bee.target = self._branch_spot()
        bee.target_site = None

    def _arrive_on_branch(self, bee: Bee) -> None:
        bee.state = "REPOS"
        bee.timer = REST_DECISION_INTERVAL
        # une abeille qui a constaté un abri inaccessible le signale aux présentes
        for m in reversed(bee.memory):
            if m["kind"] == "observation" and not m["accessible"] and self.time - m["t"] < 30:
                self._communicate(bee, m["site"], accessible=False)
                break
        self._request_decision(bee, "branche", self._branch_options(bee, just_returned=True))

    def _branch_options(self, bee: Bee, just_returned: bool) -> list[dict]:
        options = [{"action": "rester", "site": None, "label": "Rester au repos sur la branche"},
                   {"action": "explorer", "site": None, "label": "Partir explorer au hasard"}]
        for rec in bee.unverified_recommendations()[-2:]:
            what = "recommandé" if rec["accessible"] else "signalé inaccessible"
            options.append({"action": "verifier", "site": rec["site"],
                            "label": f"Aller vérifier l'abri {rec['site']} {what} par l'abeille {rec['from']}"})
        if just_returned and self._dance_duration(bee) >= DANCE_MIN_S:
            options.append({"action": "danser", "site": bee.preference,
                            "label": f"Danser pour recommander l'abri {bee.preference}"})
        return options

    def _dance_duration(self, bee: Bee) -> float:
        if not bee.preference or bee.preference_score is None:
            return 0.0
        obs = bee.last_observation(bee.preference)
        if not obs or not obs["accessible"]:
            return 0.0
        return DANCE_BASE_S * bee.preference_score * bee.profile.dance_vigor * DANCE_DECAY ** bee.dances_done

    def _step_dance(self, bee: Bee, dt: float) -> None:
        before = bee.timer
        bee.timer -= dt
        if math.floor(before) != math.floor(bee.timer):  # une fois par seconde simulée
            self._communicate(bee, bee.dance_site, accessible=True)
        if bee.timer <= 0:
            bee.dances_done += 1
            # après la danse, l'abeille retourne contrôler son site
            pos = bee.known_site_position(bee.dance_site)
            if pos is None:
                bee.state, bee.timer = "REPOS", REST_DECISION_INTERVAL
                return
            bee.state = "EXPLORATION"
            bee.target_site, bee.target = bee.dance_site, pos
            bee.timer = EXPLORATION_TIME

    def _communicate(self, bee: Bee, site_id: str, accessible: bool) -> None:
        pos = bee.known_site_position(site_id)
        recipients = []
        for other in self.bees:
            if other is bee or other.state != "REPOS" or other.pending:
                continue
            if _dist(other.x, other.y, *BRANCH) > BRANCH_RADIUS * 2:
                continue
            if self.rng.random() < DANCE_ATTEND_PROB:
                same = next((m for m in other.memory if m["kind"] == "reçue" and m["site"] == site_id
                             and m["from"] == bee.id and m["accessible"] == accessible), None)
                if same:  # même danseuse, même message : on rafraîchit au lieu de saturer la mémoire
                    same["t"] = round(self.time, 2)
                else:
                    other.remember({"kind": "reçue", "site": site_id, "t": round(self.time, 2), "from": bee.id,
                                    "accessible": accessible, "x": pos[0], "y": pos[1]})
                other.timer = min(other.timer, self.rng.uniform(0.3, 1.0))
                recipients.append(other.id)
                if not accessible and other.preference == site_id:
                    other.timer = 0.1
        if recipients:
            self.emit(bee.id, "communication", {"site": site_id, "accessible": accessible, "recipients": recipients})

    def _set_preference(self, bee: Bee, site_id: str | None, score: float | None, source: str) -> None:
        if bee.preference == site_id:
            bee.preference_score = score
            return
        old = bee.preference
        bee.preference, bee.preference_score = site_id, score
        bee.dances_done = 0
        self.emit(bee.id, "preference_changed", {"old": old, "new": site_id, "score": score, "source": source})

    # ------------------------------------------------------------------ décisions
    def _request_decision(self, bee: Bee, kind: str, options: list[dict]) -> None:
        for letter, opt in zip("ABCDEFGH", options):
            opt["letter"] = letter
        if bee.id in self.llm_bees:
            self._decision_counter += 1
            bee.pending = {"decision_id": f"{self.run_id}-{self._decision_counter}", "run_id": self.run_id,
                           "kind": kind, "options": options, "t": round(self.time, 2)}
            self.llm_requests.append({"bee": bee.id, **bee.pending})
            return
        self._apply(bee, kind, options, rule_choice(bee, kind, options, self.rng), "règles")

    def resolve_llm(self, decision_id: str, run_id: str, letter: str | None, source: str, info: dict) -> str:
        """Applique une réponse LLM (ou un repli). Retourne 'appliquée' ou la raison du rejet."""
        if run_id != self.run_id:
            return "ignorée: ancien run"
        bee = next((b for b in self.bees if b.pending and b.pending["decision_id"] == decision_id), None)
        if bee is None:
            return "ignorée: décision périmée"
        pending = bee.pending
        bee.pending = None
        options = self._revalidate(bee, pending)
        option = next((o for o in options if o["letter"] == letter), None)
        if option is None:
            source = "fallback"
            info = {**info, "fallback_reason": info.get("error") or f"lettre invalide ou plus autorisée: {letter!r}"}
            option = rule_choice(bee, pending["kind"], options, self.rng)
        self._apply(bee, pending["kind"], options, option, source, info, pending)
        return "appliquée"

    def _revalidate(self, bee: Bee, pending: dict) -> list[dict]:
        """Retire les options devenues impossibles pendant l'attente (ex. abri devenu connu inaccessible)."""
        valid = []
        for opt in pending["options"]:
            if opt["action"] in ("verifier", "adopter", "danser"):
                obs = bee.last_observation(opt["site"])
                if obs and not obs["accessible"]:
                    continue
                if opt["action"] == "verifier" and bee.known_site_position(opt["site"]) is None:
                    continue
            valid.append(opt)
        return valid

    def _apply(self, bee: Bee, kind: str, options: list[dict], option: dict, source: str,
               info: dict | None = None, pending: dict | None = None) -> None:
        action, site = option["action"], option["site"]
        if action == "rester":
            bee.state, bee.timer = "REPOS", REST_DECISION_INTERVAL
        elif action == "explorer":
            bee.state, bee.timer = "EXPLORATION", EXPLORATION_TIME
            bee.target_site, bee.target = None, None
            bee.heading = math.atan2(bee.y - BRANCH[1], bee.x - BRANCH[0]) + self.rng.uniform(-math.pi, math.pi)
        elif action == "verifier":
            for m in bee.memory:
                if m["kind"] == "reçue" and m["site"] == site:
                    m["considered"] = True
            bee.state, bee.timer = "EXPLORATION", EXPLORATION_TIME * 2
            bee.target_site, bee.target = site, bee.known_site_position(site)
        elif action == "danser":
            bee.state, bee.dance_site = "DANSE", site
            bee.timer = self._dance_duration(bee)
        elif action == "adopter":
            self._set_preference(bee, site, bee.last_observation(site)["score"], source)
            self._go_home(bee)
        elif action == "garder":
            self._go_home(bee)
        elif action == "indecise":
            self._set_preference(bee, None, None, source)
            self._go_home(bee)
        # les recommandations proposées mais non suivies sont considérées comme examinées
        for opt in options:
            if opt["action"] == "verifier" and opt is not option:
                for m in bee.memory:
                    if m["kind"] == "reçue" and m["site"] == opt["site"]:
                        m["considered"] = True
        bee.last_decision = {"source": source, "kind": kind, "action": action, "site": site,
                             "t": round(self.time, 2), **({"info": info} if info else {})}
        self.decision_log.append({
            "run_id": self.run_id, "agent_id": bee.id, "sim_time": round(self.time, 2), "kind": kind,
            "decision_id": pending["decision_id"] if pending else None,
            "requested_at": pending["t"] if pending else None,
            "options": options, "choice": option["letter"], "action": action, "source": source, **(info or {}),
        })

    # ------------------------------------------------------------------ commandes opérateur
    def set_site_accessible(self, site_id: str, accessible: bool) -> None:
        site = self.sites[site_id]
        if site.accessible != accessible:
            site.accessible = accessible
            self.emit(None, "site_access_changed", {"site": site_id, "accessible": accessible})

    def _branch_spot(self) -> tuple[float, float]:
        a, r = self.rng.uniform(0, 2 * math.pi), self.rng.uniform(0, BRANCH_RADIUS)
        return BRANCH[0] + math.cos(a) * r, BRANCH[1] + math.sin(a) * r * 0.4

    def snapshot(self) -> dict:
        return {
            "run_id": self.run_id, "seed": self.seed, "sim_time": round(self.time, 2),
            "world": {"width": WIDTH, "height": HEIGHT},
            "branch": {"x": BRANCH[0], "y": BRANCH[1], "radius": BRANCH_RADIUS},
            "sites": [{"id": s.id, "name": s.name, "x": s.x, "y": s.y, "accessible": s.accessible,
                       "volume_l": s.volume_l, "dryness": s.dryness, "entrance_cm2": s.entrance_cm2,
                       "score": evaluate_site(s.volume_l, s.dryness, s.entrance_cm2)} for s in self.sites.values()],
            "bees": [b.to_view() for b in self.bees],
        }


def rule_choice(bee: Bee, kind: str, options: list[dict], rng: random.Random) -> dict:
    """Politique à règles. Ne lit que l'abeille elle-même et les options proposées."""
    by_action = {}
    for o in options:
        by_action.setdefault(o["action"], []).append(o)
    p = bee.profile
    if kind == "inspection":
        adopt = by_action["adopter"][0]
        score = bee.last_observation(adopt["site"])["score"]
        if bee.preference is None:
            return adopt if score >= p.accept_threshold else by_action["indecise"][0]
        if bee.preference == adopt["site"] or score > (bee.preference_score or 0) + SWITCH_MARGIN:
            return adopt
        return by_action.get("garder", by_action["indecise"])[0]
    # décision sur la branche
    if "danser" in by_action and (bee.preference_score or 0) >= p.accept_threshold:
        return by_action["danser"][0]
    follow = p.follow_prob if bee.preference is None else p.follow_prob * 0.4
    for opt in reversed(by_action.get("verifier", [])):
        if opt["site"] == bee.preference:  # alerte sur mon site : je vais constater moi-même
            return opt
        if rng.random() < follow:
            return opt
    if bee.preference is None and rng.random() < min(1.0, p.explore_rate * REST_DECISION_INTERVAL):
        return by_action["explorer"][0]
    return by_action["rester"][0]
