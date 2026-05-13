from typing import Any


def append_result_warnings(answer: str, warnings: list[str]) -> str:
    cleaned = []
    for warning in warnings or []:
        text = str(warning or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    if not cleaned:
        return answer
    return f"{answer}\n\nLưu ý: " + " ".join(cleaned)


def safe_rows_by_country(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows if isinstance(rows, list) else []:
        code = row.get("country_code")
        if not code:
            continue
        grouped.setdefault(str(code), []).append(row)
    return grouped
