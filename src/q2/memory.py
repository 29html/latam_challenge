from typing import List, Tuple

import emoji


def extract_emojis(s: str) -> List[str]:
    """Extrae todos los emojis de una cadena de texto."""
    return [char for char in s if char in emoji.UNICODE_EMOJI['en']]


def q2_memory(file_path: str = file_path) -> List[Tuple[str, int]]:
    pass


if __name__ == '__main__':
    top_users_by_date = q2_memory()
    print(top_users_by_date)
