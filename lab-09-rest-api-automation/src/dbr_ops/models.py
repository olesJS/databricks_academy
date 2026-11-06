from dataclasses import dataclass

@dataclass(frozen=True)
class Identity:
    user_name: str
    display_name: str
    host: str
