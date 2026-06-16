from __future__ import annotations

import re
import unicodedata
from typing import Any

try:
    from utils import clean_text
except ModuleNotFoundError:
    from .utils import clean_text

TERRUQUEO_TERMS = [
    "terruco",
    "terruqueo",
    "senderista",
    "rojo",
    "comunista",
]

TERRUQUEO_TERMS_EXTRA = [
    "terruca",
    "terrucos",
    "terrucada",
    "terruquear",
    "terruquean",
    "senderistas",
    "sendero",
    "sendero luminoso",
    "emerretista",
    "mrta",
    "movadef",
    "roja",
    "rojos",
    "comunistas",
    "comunacho",
    "comunachos",
    "caviares",
]

FRAUDE_TERMS = [
    "fraude",
    "robo",
]

FRAUDE_TERMS_EXTRA = [
    "fraudulento",
    "fraudulenta",
    "fraudearon",
    "robaron votos",
    "robo electoral",
    "actas falsas",
    "actas perdidas",
    "actas duplicadas",
    "actas impugnadas",
    "mesas anuladas",
    "anular mesas",
    "votos inflados",
    "votos perdidos",
    "conteo manipulado",
    "conteo lento",
    "trampa electoral",
    "irregularidades",
    "nulidad",
    "impugnacion",
    "impugnación",
]

ELECTORAL_INSTITUTION_TERMS = [
    "onpe",
    "jne",
    "reniec",
    "personero",
    "personeros",
    "actas",
    "mesa",
    "mesas",
    "cedula",
    "cédula",
    "padron",
    "padrón",
    "votos",
    "conteo",
    "boca de urna",
    "flash electoral",
]

POLITICAL_TERMS = [
    "keiko",
    "fujimori",
    "fuerza popular",
    "fp",
    "roberto sánchez",
    "roberto sanchez",
    "sánchez",
    "sanchez",
    "jp",
    "juntos por el perú",
    "juntos por el peru",
    "castillo",
    "perú libre",
    "peru libre",
    "reniec",
    "vizcarra",
]

POLITICAL_TERMS_EXTRA = [
    "fujimorismo",
    "antifujimorismo",
    "pedro castillo",
    "lapiz",
    "lápiz",
    "lapicito",
    "cerron",
    "cerrón",
    "dina",
    "boluarte",
    "ppk",
    "acuña",
    "cesar acuña",
    "césar acuña",
    "app",
    "apra",
    "aprista",
    "lopez aliaga",
    "lópez aliaga",
    "rafael lopez aliaga",
    "rafael lópez aliaga",
    "rla",
    "porky",
    "renovacion popular",
    "renovación popular",
    "avanza pais",
    "avanza país",
    "antauro",
    "humala",
]

POLARIZATION_TERMS = [
    "zurdo",
    "zurdos",
    "caviar",
    "vendepatria",
    "corrupto",
    "corruptos",
    "dictadura",
    "terrorista",
    "odio",
    "lacra",
    "traidor",
    "mafiosa",
    "mafioso",
    "golpista",
]

POLARIZATION_TERMS_EXTRA = [
    "zurda",
    "izquierdista",
    "izquierdistas",
    "derecha bruta",
    "dba",
    "facho",
    "facha",
    "fachos",
    "fascista",
    "fascistas",
    "vendepatrias",
    "corrupta",
    "corruptas",
    "dictador",
    "terroristas",
    "lacras",
    "traidora",
    "traidores",
    "mafia",
    "delincuente",
    "delincuentes",
    "criminal",
    "criminales",
    "rata",
    "ratas",
    "basura",
    "escoria",
]

ETHNIC_RACIAL_DISCRIMINATION_TERMS = [
    "serrano",
    "serrana",
    "serranos",
    "serranas",
    "serranazo",
    "serranaza",
    "serranear",
    "cholo",
    "chola",
    "cholos",
    "cholas",
    "cholada",
    "cholear",
    "indio",
    "india",
    "indios",
    "indias",
    "indigena",
    "indígena",
    "paisano",
    "paisana",
    "provinciano",
    "provinciana",
    "ignorante de la sierra",
    "bajado del cerro",
    "llama",
    "llamas",
    "auquenido",
    "auquénido",
    "motoso",
    "mote",
]

HOMOPHOBIC_SLUR_TERMS = [
    "cabro",
    "cabros",
    "kbro",
    "kbros",
    "kbraso",
    "kbron",
    "kbrón",
    "cabron",
    "cabrón",
    "cabra",
    "cabrita",
    "maricon",
    "maricón",
    "marica",
    "rosquete",
    "rosquetes",
    "loca",
]

GENERAL_INSULT_TERMS = [
    "csm",
    "ctm",
    "mierda",
    "pendejo",
    "pendeja",
    "pendejos",
    "pendejas",
    "idiota",
    "estupido",
    "estúpido",
    "burro",
    "bruto",
]

GENERAL_INSULT_TERMS_EXTRA = [
    "imbecil",
    "imbécil",
    "idiota",
    "estupido",
    "estúpido",
    "burro",
    "bruto",
    "ignorante",
    "vago",
    "vaga",
    "payaso",
    "payasa",
    "miserable",
    "asqueroso",
    "asquerosa",
    "porqueria",
    "porquería",
]

RULE_CATEGORIES = {
    "terruqueo": TERRUQUEO_TERMS + TERRUQUEO_TERMS_EXTRA,
    "fraude": FRAUDE_TERMS + FRAUDE_TERMS_EXTRA,
    "electoral_institution": ELECTORAL_INSTITUTION_TERMS,
    "political_mention": POLITICAL_TERMS + POLITICAL_TERMS_EXTRA,
    "polarization": POLARIZATION_TERMS + POLARIZATION_TERMS_EXTRA,
    "ethnic_racial_slur": ETHNIC_RACIAL_DISCRIMINATION_TERMS,
    "homophobic_slur": HOMOPHOBIC_SLUR_TERMS,
    "general_insult": GENERAL_INSULT_TERMS + GENERAL_INSULT_TERMS_EXTRA,
}

ONLY_SYMBOLS_RE = re.compile(r"^[\W_]+$", re.UNICODE)
REPEATED_CHAR_RE = re.compile(r"(.)\1{5,}", re.IGNORECASE)
REPEATED_WORD_RE = re.compile(r"\b(\w+)(?:\s+\1){2,}\b", re.IGNORECASE)


def strip_accents(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")


def normalize_for_rules(text: Any) -> str:
    normalized = strip_accents(clean_text(text))
    normalized = re.sub(r"(.)\1{2,}", r"\1", normalized)
    normalized = re.sub(r"[^a-z0-9:_\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def term_pattern(term: str) -> re.Pattern:
    normalized = normalize_for_rules(term)
    parts = [re.escape(part) for part in normalized.split()]
    body = r"\s+".join(parts)
    return re.compile(rf"(?<!\w){body}(?!\w)", re.IGNORECASE)


COMPILED_RULES = {
    category: [(normalize_for_rules(term), term_pattern(term)) for term in terms]
    for category, terms in RULE_CATEGORIES.items()
}


def matched_terms(text_norm: str, category: str) -> list[str]:
    matches = []
    for term, pattern in COMPILED_RULES[category]:
        if pattern.search(text_norm):
            matches.append(term)
    return sorted(set(matches))


def get_rule_matches(text: str) -> dict[str, list[str]]:
    text_norm = normalize_for_rules(text)
    return {
        category: matched_terms(text_norm, category)
        for category in COMPILED_RULES
    }


def contains_term(text: str, terms: list[str]) -> bool:
    text_norm = normalize_for_rules(text)
    compiled = [(normalize_for_rules(term), term_pattern(term)) for term in terms]
    return any(pattern.search(text_norm) for _, pattern in compiled)


def is_emoji_or_symbol_only(text: str) -> bool:
    stripped = str(text or "").strip()
    if not stripped:
        return True

    shortcode_tokens = stripped.split()
    if shortcode_tokens and all(
        token.startswith(":") and token.endswith(":") and len(token) > 2 for token in shortcode_tokens
    ):
        return True

    without_emojis = re.sub(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]", "", stripped)
    without_spaces = re.sub(r"\s+", "", without_emojis)
    if without_spaces and ONLY_SYMBOLS_RE.fullmatch(without_spaces):
        return True

    normalized = normalize_for_rules(stripped)
    return bool(normalized) and ONLY_SYMBOLS_RE.fullmatch(normalized) is not None


def is_spam_noise(text: str) -> bool:
    raw = str(text or "")
    normalized = normalize_for_rules(raw)
    compact = re.sub(r"\s+", "", normalized)

    if not compact:
        return True

    if len(compact) <= 2:
        return True

    if is_emoji_or_symbol_only(raw):
        return True

    if REPEATED_CHAR_RE.search(clean_text(raw)):
        return True

    if REPEATED_WORD_RE.search(normalized):
        return True

    tokens = normalized.split()
    if len(tokens) >= 4 and len(set(tokens)) == 1:
        return True

    return False


def score_rules(flags: dict[str, bool]) -> int:
    score = 0
    score += 2 if flags["has_terruqueo"] else 0
    score += 2 if flags["has_discriminatory_language"] else 0
    score += 2 if flags["has_homophobic_slur"] else 0
    score += 2 if flags["has_fraude"] else 0
    score += 1 if flags["has_political_mention"] else 0
    score += 1 if flags["has_polarization_signal"] else 0

    strong_signal = any(
        flags[key]
        for key in (
            "has_terruqueo",
            "has_discriminatory_language",
            "has_homophobic_slur",
            "has_fraude",
        )
    )
    if flags["is_spam_noise"] and not strong_signal:
        score -= 1

    return max(score, 0)


def build_tags(flags: dict[str, bool]) -> str:
    tag_map = [
        ("has_terruqueo", "terruqueo"),
        ("has_fraude", "fraude"),
        ("has_electoral_institution", "electoral_institution"),
        ("has_political_mention", "political_mention"),
        ("has_polarization_signal", "polarization"),
        ("has_ethnic_racial_slur", "ethnic_racial_slur"),
        ("has_homophobic_slur", "homophobic_slur"),
        ("has_general_insult", "general_insult"),
        ("is_spam_noise", "spam_noise"),
    ]
    return "|".join(tag for flag, tag in tag_map if flags.get(flag))


def apply_rules(text: str) -> dict[str, Any]:
    text_norm = normalize_for_rules(text)
    matches = get_rule_matches(text)

    has_terruqueo = bool(matches["terruqueo"])
    has_fraude = bool(matches["fraude"])
    has_electoral_institution = bool(matches["electoral_institution"])
    has_political_mention = bool(matches["political_mention"])
    has_ethnic_racial_slur = bool(matches["ethnic_racial_slur"])
    has_homophobic_slur = bool(matches["homophobic_slur"])
    has_general_insult = bool(matches["general_insult"])
    has_discriminatory_language = has_ethnic_racial_slur or has_homophobic_slur

    has_polarization_signal = has_political_mention and (
        bool(matches["polarization"])
        or has_terruqueo
        or has_fraude
        or has_general_insult
        or has_discriminatory_language
    )

    flags: dict[str, Any] = {
        "text_norm": text_norm,
        "has_terruqueo": has_terruqueo,
        "has_fraude": has_fraude,
        "has_electoral_institution": has_electoral_institution,
        "has_political_mention": has_political_mention,
        "has_polarization_signal": has_polarization_signal,
        "has_discriminatory_language": has_discriminatory_language,
        "has_ethnic_racial_slur": has_ethnic_racial_slur,
        "has_homophobic_slur": has_homophobic_slur,
        "has_general_insult": has_general_insult,
        "is_spam_noise": is_spam_noise(text),
    }
    flags["local_risk_score"] = score_rules(flags)
    flags["local_rule_tags"] = build_tags(flags)
    return flags
