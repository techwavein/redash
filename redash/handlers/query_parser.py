import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import logging

logger = logging.getLogger(__name__)


def error_response(message, http_status=400):
    return {'job': {'status': 4, 'error': message}}, http_status


error_messages = {
    'invalid_query': error_response('You do not have permission to run this query with this data source.', 403),
}


def is_sub_select(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_sub_select(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value


def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))


def validate_query(query, org_slug):
    """
        validate_query will validate query if user has provided valid query of his own views
        or to check if user is trying to get some other org views
    """
    err = None
    logger.info("query: [%s] - [%s]", query, org_slug)
    tables = extract_tables(query)
    for table in tables:
        if table.lower().find(org_slug.lower()) == -1:
            err = error_messages['invalid_query']
            break
    return err
