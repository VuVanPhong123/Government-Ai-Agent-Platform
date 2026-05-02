import re
from dataclasses import asdict, dataclass

from app.resolver.indicator_resolver import normalize_text


@dataclass(frozen=True)
class CountryMeta:
    code: str
    name: str
    aliases: tuple[str, ...] = ()


COUNTRIES: dict[str, CountryMeta] = {
    "VNM": CountryMeta("VNM", "Vietnam", ("vietnam", "viet nam", "việt nam", "vn")),
    "THA": CountryMeta("THA", "Thailand", ("thailand", "thai lan", "thái lan")),
    "USA": CountryMeta("USA", "United States", ("united states", "usa", "u.s.", "us", "america", "my", "mỹ")),
    "CHN": CountryMeta("CHN", "China", ("china", "trung quoc", "trung quốc")),
    "JPN": CountryMeta("JPN", "Japan", ("japan", "nhat ban", "nhật bản")),
    "KOR": CountryMeta("KOR", "South Korea", ("south korea", "korea", "han quoc", "hàn quốc")),
    "SGP": CountryMeta("SGP", "Singapore", ("singapore", "sing")),
    "IDN": CountryMeta("IDN", "Indonesia", ("indonesia", "indo")),
    "MYS": CountryMeta("MYS", "Malaysia", ("malaysia", "malay")),
    "PHL": CountryMeta("PHL", "Philippines", ("philippines", "philippine", "phil", "phi lip pin")),
    "IND": CountryMeta("IND", "India", ("india", "an do", "ấn độ")),
    "DEU": CountryMeta("DEU", "Germany", ("germany", "deutschland", "duc", "đức")),
    "FRA": CountryMeta("FRA", "France", ("france", "phap", "pháp")),
    "GBR": CountryMeta("GBR", "United Kingdom", ("united kingdom", "uk", "britain", "anh")),
    "ITA": CountryMeta("ITA", "Italy", ("italy", "italia", "y")),
    "ESP": CountryMeta("ESP", "Spain", ("spain", "tay ban nha", "tây ban nha")),
    "GRC": CountryMeta("GRC", "Greece", ("greece", "hy lap", "hy lạp")),
    "RUS": CountryMeta("RUS", "Russia", ("russia", "nga")),
    "AUS": CountryMeta("AUS", "Australia", ("australia", "uc", "úc")),
    "CAN": CountryMeta("CAN", "Canada", ("canada",)),
    "BRA": CountryMeta("BRA", "Brazil", ("brazil", "braxin", "brasil")),
    "MEX": CountryMeta("MEX", "Mexico", ("mexico", "mexico")),
    "ARG": CountryMeta("ARG", "Argentina", ("argentina",)),
    "TUR": CountryMeta("TUR", "Turkey", ("turkey", "turkiye", "tho nhi ky", "thổ nhĩ kỳ")),
    "DNK": CountryMeta("DNK", "Denmark", ("denmark", "dan mach", "đan mạch")),
    "IRL": CountryMeta("IRL", "Ireland", ("ireland", "ai len")),
    "ETH": CountryMeta("ETH", "Ethiopia", ("ethiopia", "ethiopie")),
    "BGD": CountryMeta("BGD", "Bangladesh", ("bangladesh",)),
    "NGA": CountryMeta("NGA", "Nigeria", ("nigeria",)),
}


@dataclass(frozen=True)
class CountryMatch:
    country: CountryMeta
    matched_alias: str


def _contains_country_alias(normalized_text: str, normalized_alias: str) -> bool:
    if not normalized_alias:
        return False

    return re.search(rf"(^|\s){re.escape(normalized_alias)}($|\s)", normalized_text) is not None


def resolve_countries(message: str) -> list[CountryMatch]:
    normalized_message = normalize_text(message)
    matches: list[CountryMatch] = []

    for country in COUNTRIES.values():
        aliases = (country.code, country.name, *country.aliases)

        for alias in aliases:
            normalized_alias = normalize_text(alias)

            if _contains_country_alias(normalized_message, normalized_alias):
                matches.append(CountryMatch(country=country, matched_alias=alias))
                break

    seen: set[str] = set()
    unique_matches: list[CountryMatch] = []

    for match in matches:
        if match.country.code not in seen:
            seen.add(match.country.code)
            unique_matches.append(match)

    return unique_matches


def country_match_to_dict(match: CountryMatch) -> dict:
    data = asdict(match.country)
    data["matched_alias"] = match.matched_alias
    return data