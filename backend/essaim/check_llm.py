"""Appel réel unique au modèle configuré, avec une vraie situation de décision.

    python -m essaim.check_llm
Code de sortie : 0 = réponse valide du modèle, 1 = modèle indisponible ou réponse invalide.
"""
import asyncio
import json
import sys

from .config import load_config
from .llm import OllamaClient, build_prompt
from .world import World


async def main() -> int:
    cfg = load_config()
    client = OllamaClient(cfg.ollama_base_url, cfg.ollama_model, cfg.ollama_timeout_s, cfg.ollama_num_ctx)
    status = await client.check()
    print(f"Ollama {cfg.ollama_base_url} — modèle {cfg.ollama_model} : {status}")
    if status != "prêt":
        return 1
    world = World(1, llm_bees={0})
    while not world.llm_requests:
        world.step(0.1)
    request = world.llm_requests[0]
    prompt = build_prompt(world.bees[0].to_view(), request)
    print("--- prompt ---\n" + prompt + "\n--------------")
    info = await client.decide(prompt, [o["letter"] for o in request["options"]])
    print(json.dumps(info, ensure_ascii=False, indent=2))
    return 0 if info["letter"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
