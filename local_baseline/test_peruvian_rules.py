from __future__ import annotations

from peruvian_rules import apply_rules


def assert_flags(text: str, expected: dict[str, bool]) -> None:
    result = apply_rules(text)
    for flag, expected_value in expected.items():
        actual_value = result[flag]
        assert actual_value == expected_value, (
            f"{text!r}: expected {flag}={expected_value}, got {actual_value}. "
            f"Full result: {result}"
        )


def main() -> None:
    assert_flags(
        "JP terruco",
        {
            "has_terruqueo": True,
            "has_political_mention": True,
            "has_polarization_signal": True,
        },
    )
    assert_flags(
        "fraude en la ONPE",
        {
            "has_fraude": True,
            "has_electoral_institution": True,
        },
    )
    assert_flags(
        "calla serrano",
        {
            "has_ethnic_racial_slur": True,
            "has_discriminatory_language": True,
        },
    )
    assert_flags(
        "vamos Keiko",
        {
            "has_political_mention": True,
            "has_polarization_signal": False,
        },
    )
    assert_flags(
        "rojos comunistas",
        {
            "has_terruqueo": True,
            "has_political_mention": False,
        },
    )
    assert_flags(
        "ptm que gane",
        {
            "has_general_insult": False,
            "is_spam_noise": False,
        },
    )
    assert_flags(
        "😂😂😂😂",
        {
            "is_spam_noise": True,
        },
    )
    assert_flags(
        "puro kbraso bota por keiko",
        {
            "has_homophobic_slur": True,
            "has_discriminatory_language": True,
            "has_political_mention": True,
            "has_polarization_signal": True,
        },
    )
    assert_flags(
        "actas del JNE",
        {
            "has_fraude": False,
            "has_electoral_institution": True,
        },
    )

    print("Todas las pruebas manuales de reglas peruanas pasaron.")


if __name__ == "__main__":
    main()
