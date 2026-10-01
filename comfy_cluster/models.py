"""Model awareness helpers.

Each node's inventory comes from ComfyUI's own /models/{folder} endpoints. A workflow "requires"
a model when one of its literal string inputs equals a model file that some node in the cluster
has. This avoids hard-coding input names (ckpt_name, lora_name, ...), so custom loader nodes work too.
"""
from __future__ import annotations

import json


def norm(name: str) -> str:
    """Windows nodes report 'SD1.5\\model.safetensors'; compare everything with forward slashes."""
    return name.replace("\\", "/")


def required_models(body: bytes, known: set[str]) -> set[str]:
    """Model files referenced by a POST /prompt payload, limited to files the cluster knows about."""
    if not known:
        return set()
    try:
        prompt = json.loads(body).get("prompt")
    except (ValueError, AttributeError):
        return set()
    if not isinstance(prompt, dict):
        return set()
    found: set[str] = set()
    for node in prompt.values():
        inputs = node.get("inputs") if isinstance(node, dict) else None
        if not isinstance(inputs, dict):
            continue
        for value in inputs.values():
            if isinstance(value, str):
                v = norm(value)
                if v in known:
                    found.add(v)
    return found
