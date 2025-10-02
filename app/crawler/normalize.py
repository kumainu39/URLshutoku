from __future__ import annotations

import re
import unicodedata

KANA_TABLE = str.maketrans({
    "ｱ": "ア",
    "ｲ": "イ",
    "ｳ": "ウ",
    "ｴ": "エ",
    "ｵ": "オ",
    "ｶ": "カ",
    "ｷ": "キ",
    "ｸ": "ク",
    "ｹ": "ケ",
    "ｺ": "コ",
    "ｻ": "サ",
    "ｼ": "シ",
    "ｽ": "ス",
    "ｾ": "セ",
    "ｿ": "ソ",
    "ﾀ": "タ",
    "ﾁ": "チ",
    "ﾂ": "ツ",
    "ﾃ": "テ",
    "ﾄ": "ト",
    "ﾅ": "ナ",
    "ﾆ": "ニ",
    "ﾇ": "ヌ",
    "ﾈ": "ネ",
    "ﾉ": "ノ",
    "ﾊ": "ハ",
    "ﾋ": "ヒ",
    "ﾌ": "フ",
    "ﾍ": "ヘ",
    "ﾎ": "ホ",
    "ﾏ": "マ",
    "ﾐ": "ミ",
    "ﾑ": "ム",
    "ﾒ": "メ",
    "ﾓ": "モ",
    "ﾔ": "ヤ",
    "ﾕ": "ユ",
    "ﾖ": "ヨ",
    "ﾗ": "ラ",
    "ﾘ": "リ",
    "ﾙ": "ル",
    "ﾚ": "レ",
    "ﾛ": "ロ",
    "ﾜ": "ワ",
    "ｦ": "ヲ",
    "ﾝ": "ン",
})


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value)
    value = value.translate(KANA_TABLE)
    value = re.sub(r"\s+", " ", value)
    value = value.replace("株式會社", "株式会社")
    return value.strip().lower()
