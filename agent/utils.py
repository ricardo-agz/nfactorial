import re


def to_snake_case(camel: str) -> str:
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", camel)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake)
    return snake.lower()
