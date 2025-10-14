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
        self.enabled = (
            bool(settings.llm_enabled)
            and Llama is not None
            and bool(settings.llm_model_path)
        )
        self._llm: Optional[Llama] = None
        self.settings = settings

    def _ensure_model(self) -> None:
        if not self.enabled:
            return
        # Validate model path
        model_path = self.settings.llm_model_path
        if not model_path or not model_path.exists():
            logger.warning("LLM disabled: model path not found: {path}", path=model_path)
            self.enabled = False
            return
        if self._llm is None:
            try:
                logger.info("Loading LLaMA model from {path}", path=model_path)
                self._llm = Llama(
                    model_path=str(model_path),
                    n_gpu_layers=self.settings.llm_gpu_layers,
                    n_ctx=self.settings.llm_context_window,
                    embedding=False,
                    logits_all=False,
                )
            except Exception as exc:  # pragma: no cover - runtime safety
                logger.warning("LLM load failed: {exc}", exc=exc)
                self._llm = None
                self.enabled = False

    def validate(self, request: LLMRequest) -> Optional[bool]:
        if not self.enabled:
            return None
        self._ensure_model()
        if self._llm is None:
            logger.warning("LLM requested but model failed to load.")
            return None
                # Build a compact prompt and keep the page text well within the context window.
        # Use a conservative character budget to avoid token overflow on JP content.
        char_budget = max(512, min(2000, int(self.settings.llm_context_window * 0.6)))
        snippet = (request.page_text or "")[:char_budget]
        prompt = (
            "以下はあるウェブページの本文テキストです。次の2点を判定してください。\n"
            "1) 会社名と住所が一致しているか (match)\n"
            "2) このページが企業の公式ホームページか (official_homepage)。企業ディレクトリ、データベース、求人・転職、ニュース/PR配信サイトはNO。\n"
            "出力は JSON で {\"match\": true/false, \"official_homepage\": true/false} のみ返してください。余計な文字は出力しないでください。\n"
            f"会社名: {request.company_name}\n住所: {request.address}\n---\n"
            f"{snippet}\n"
        )
)
        try:
            response = self._llm(prompt=prompt, max_tokens=128, temperature=0.1, echo=False)
        except Exception as exc:  # pragma: no cover - runtime safety
            logger.warning("LLM inference failed: {exc}", exc=exc)
            return None
        text = response.get("choices", [{}])[0].get("text", "").strip()
        logger.debug("LLM response: {text}", text=text)
        try:
            import json
            obj = json.loads(text)
            match = bool(obj.get("match"))
            official = bool(obj.get("official_homepage"))
            return True if (match and official) else False
        except Exception:
            low = text.lower()
            if "official_homepage\"\s*:\s*true" in low and "match\"\s*:\s*true" in low:
                return True
            if "official_homepage\"\s*:\s*false" in low or "match\"\s*:\s*false" in low:
                return False
            if "true" in low and "false" not in low:
                return True
            if "false" in low:
                return False
            return None
