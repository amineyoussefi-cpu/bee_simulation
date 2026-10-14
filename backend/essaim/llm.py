"""Client Ollama pour l'abeille pilotée par le LLM. Appelé uniquement aux moments de décision."""
import re
import time

import httpx

STATE_FR = {"REPOS": "au repos sur la branche", "EXPLORATION": "en exploration", "INSPECTION": "en inspection",
            "RETOUR": "en retour vers la branche", "DANSE": "en train de danser"}


def build_prompt(bee_view: dict, request: dict) -> str:
    p = bee_view
    obs = [f"- abri {m['site']} (t={m['t']} s) : " + (
        f"volume {m['volume_l']} L, sécheresse {m['dryness']}, entrée {m['entrance_cm2']} cm², score {m['score']}"
        if m["accessible"] else "INACCESSIBLE") for m in p["observations"]]
    rec = [f"- abeille {m['from']} (t={m['t']} s) : " + (
        f"recommande l'abri {m['site']}" if m["accessible"] else f"signale l'abri {m['site']} inaccessible")
        for m in p["received"]]
    pref = (f"abri {p['preference']} (score observé {p['preference_score']})" if p["preference"]
            else "aucune (indécise)")
    options = "\n".join(f"[{o['letter']}] {o['label']}" for o in request["options"])
    situation = ("Tu viens d'inspecter un abri." if request["kind"] == "inspection"
                 else "Tu es sur la branche de l'essaim.")
    return f"""Tu es l'abeille n°{p['id']} d'un essaim qui cherche un nouveau nid, sans chef.
Tu ne sais que ce que tu as observé toi-même ou ce que d'autres abeilles t'ont transmis par la danse.
Profil : {p['profile']}. État : {STATE_FR[p['state']]}. Temps simulé : {request['t']} s.
Ta préférence actuelle : {pref}.

Tes observations personnelles :
{chr(10).join(obs) or '- aucune'}

Informations reçues d'autres abeilles (non vérifiées par toi) :
{chr(10).join(rec) or '- aucune'}

{situation} Actions autorisées :
{options}

Réponds uniquement par la lettre de l'action choisie, sans autre texte."""


def parse_letter(text: str, allowed: list[str]) -> str | None:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    m = re.match(r"^\W*([A-Z])\b", text)
    if m and m.group(1) in allowed:
        return m.group(1)
    return None


class OllamaClient:
    def __init__(self, base_url: str, model: str, timeout_s: float, num_ctx: int):
        self.base_url, self.model, self.timeout_s, self.num_ctx = base_url.rstrip("/"), model, timeout_s, num_ctx
        self.status = "inconnu"

    async def check(self) -> str:
        """Vérifie le serveur et la présence du modèle demandé (sans jamais le remplacer)."""
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(f"{self.base_url}/api/tags")
                r.raise_for_status()
            names = {m["name"] for m in r.json().get("models", [])}
            self.status = "prêt" if self.model in names else "modèle absent"
        except Exception as exc:  # noqa: BLE001 — on affiche la cause exacte
            self.status = f"hors ligne ({type(exc).__name__})"
        return self.status

    async def decide(self, prompt: str, allowed: list[str]) -> dict:
        """Retourne {letter, latency_ms, prompt_tokens, completion_tokens, tokens_available, raw_response, error}."""
        started = time.perf_counter()
        info = {"letter": None, "error": None}
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                r = await client.post(f"{self.base_url}/api/generate", json={
                    "model": self.model, "prompt": prompt, "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 64, "num_ctx": self.num_ctx},
                })
                r.raise_for_status()
            data = r.json()
            raw = data.get("response", "")
            prompt_tokens = data.get("prompt_eval_count")
            info.update(raw_response=raw[:200], prompt_tokens=prompt_tokens,
                        completion_tokens=data.get("eval_count"), context_tokens=self.num_ctx,
                        tokens_available=self.num_ctx - prompt_tokens if prompt_tokens else None)
            info["letter"] = parse_letter(raw, allowed)
            if info["letter"] is None:
                info["error"] = f"réponse non conforme: {raw[:60]!r}"
        except httpx.TimeoutException:
            info["error"] = f"timeout après {self.timeout_s:g} s"
        except httpx.HTTPStatusError as exc:
            info["error"] = f"HTTP {exc.response.status_code}: {exc.response.text[:120]}"
        except Exception as exc:  # noqa: BLE001
            info["error"] = f"{type(exc).__name__}: {exc}"
        info["latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
        return info
