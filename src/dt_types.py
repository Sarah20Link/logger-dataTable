from typing import Any

KeyTyping = int | float
RowValueTyping = list[Any]
RowTyping = dict[KeyTyping, RowValueTyping]
SingleRowTyping = tuple[KeyTyping, RowValueTyping]
