from sql_metadata import Parser
from sql_metadata.keywords_lists import COLUMNS_SECTIONS
from sql_metadata.token import SQLToken
from sql_metadata.utils import UniqueList


class MViewParser(Parser):
    def __init__(self, sql: str = "") -> None:
        super(MViewParser, self).__init__(sql)

    def _add_to_columns_aliases_subsection(self, token: SQLToken, left_expand: bool = True) -> None:
        """
        Add alias to the section in which it appears in query
        """
        keyword = token.last_keyword_normalized
        alias = token.value if left_expand else token.value.split(".")[-1]
        if token.last_keyword_normalized in ["FROM", "WITH"]:
            if token.find_nearest_token("(").is_with_columns_start:
                keyword = "SELECT"
            else:
                keyword = "WHERE"
        section = COLUMNS_SECTIONS[keyword]
        self._columns_aliases_dict = self._columns_aliases_dict or {}
        self._columns_aliases_dict.setdefault(section, UniqueList()).append(alias)
