from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

try:
    from llama_cpp import Llama  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    Llama = None  # type: ignore

from loguru import logger

from ..config import get_settings


@dataclass
class LLMRequest:
    company_name: str
    address: str
    page_text: str


class LLMVerifier:
    def __init__(self) -> None:
        settings = get_settings()
        self.enabled = settings.llm_enabled and Llama is not None and settings.llm_model_path
        self._llm: Optional[Llama] = None
        self.settings = settings

    def _ensure_model(self) -> None:
        if not self.enabled:
            return
        if self._llm is None:
            logger.info("Loading LLaMA model from {path}", path=self.settings.llm_model_path)
            self._llm = Llama(
                model_path=str(self.settings.llm_model_path),
                n_gpu_layers=self.settings.llm_gpu_layers,
                n_ctx=self.settings.llm_context_window,
                embedding=False,
                logits_all=False,
            )

    def validate(self, request: LLMRequest) -> Optional[bool]:
        if not self.enabled:
            return None
        self._ensure_model()
        if self._llm is None:
            logger.warning("LLM requested but model failed to load.")
            return None
        prompt = (
            "以下は企業ウェブサイトのテキストです。会社名と住所が一致しているかを true/false で答えてください。\n"
            f"会社名: {request.company_name}\n住所: {request.address}\n---\n"
            f"{request.page_text[: self.settings.llm_context_window]}\n"
            "回答は JSON で {\"match\": true/false} のみを返してください。"
        )
        response = self._llm(prompt=prompt, max_tokens=64, temperature=0.1, echo=False)
        text = response.get("choices", [{}])[0].get("text", "").strip()
        logger.debug("LLM response: {text}", text=text)
        if "true" in text.lower():
            return True
        if "false" in text.lower():
            return False
        return None
