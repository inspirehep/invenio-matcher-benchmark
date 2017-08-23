from itertools import product

from inspirehep.utils.record import get_value


def get_exact_queries(inspire_record):
    dois = get_value(inspire_record, 'dois.value')
    arxiv_eprints = get_value(inspire_record, 'arxiv_eprints.value')
    report_numbers = get_value(inspire_record, 'report_numbers.value')

    return [
        {'type': 'exact', 'match': 'dois.value.raw', 'values': dois},
        {'type': 'exact', 'match': 'arxiv_eprints.value.raw', 'values': arxiv_eprints},
        {'type': 'exact', 'match': 'report_numbers.value.raw', 'values': report_numbers}
    ]


def get_fuzzy_queries(inspire_record):
    mini_record = get_mlt_record(inspire_record)
    return [{'type': 'fuzzy', 'match': mini_record}]


def get_mlt_record(inspire_record):
    """Returns a small record to be used with ElasticSearch
    More Like This query."""
    records = []

    if inspire_record.get('titles'):
        records.append(
            {
                'titles': inspire_record['titles'],
                'boost': 20
            }
        )
    if inspire_record.get('abstracts'):
        records.append(
            {
                'abstracts': inspire_record['abstracts'],
                'boost': 20
            }
        )
    if inspire_record.get('report_numbers'):
        records.append(
            {
                'report_numbers': inspire_record['report_numbers'],
                'boost': 10
            }
        )
    if inspire_record.get('authors'):
        records.append(
            {
                'authors': inspire_record['authors'][:3]
            }
        )
    return records


def validator(record, result):
    """Validate results to avoid false positives."""
    from inspire_json_merger.comparators import AuthorComparator

    author_score = 0.5
    if record.get('authors') and result.record.get('authors'):
        number_of_authors = 5
        try:
            matches = len(
                AuthorComparator(
                    record['authors'][:number_of_authors],
                    result.record['authors'][:number_of_authors]
                ).matches
            )
            author_score = matches / float(number_of_authors)
        except:
            # FIXME json_merger fails internally in some author comparison
            pass
    title_max_score = 0.5
    if record.get('titles') and result.record.get('titles'):
        record_titles = [r['title'].lower() for r in record['titles']]
        result_titles = [r['title'].lower() for r in result.record['titles']]

        for titles in product(record_titles, result_titles):
            record_tokens = set(titles[0].split())
            result_tokens = set(titles[1].split())
            title_score = len(record_tokens & result_tokens) / \
                float(len(record_tokens | result_tokens))
            if title_score > title_max_score:
                title_max_score = title_score

    return (author_score + title_max_score) / 2 > 0.5
