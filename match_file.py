import sys

from dojson.contrib.marc21.utils import create_record as marc_create_record

from invenio_matcher.api import match as _match

from inspire_dojson.processors import overdo_marc_dict
from inspirehep.modules.migrator.tasks.records import split_stream
from inspirehep.utils.record import get_value
from inspirehep.factory import create_app


def generate_match_map():
    with open('test_matches_clean.txt', 'r') as fd:
        result = {}
        for line in fd:
            doi, recid = line.split('--')
            result[doi.strip()] = recid.strip()
        return result


def main():
    if len(sys.argv) < 2:
        print 'A filename is required'
        return 1

    app = create_app()

    with app.app_context():

        filename = sys.argv[1]

        correct_match_map = generate_match_map()

        with open(filename, 'r') as fd:
            for marcxml in split_stream(fd):
                marc_record = marc_create_record(
                    marcxml, keep_singletons=False)
                inspire_record = overdo_marc_dict(marc_record)
                dois = get_value(inspire_record, 'dois.value')
                arxiv_eprints = get_value(
                    inspire_record, 'arxiv_eprints.value')
                print 'Going to match DOIs: ', dois
                print 'Going to match arXiv eprints: ', arxiv_eprints

                # Step 1 - apply exact matches
                queries_ = [
                    {'type': 'exact', 'match': 'dois.value', 'values': dois},
                    {'type': 'exact', 'match': 'arxiv_eprints.value',
                        'values': arxiv_eprints}
                ]

                found = False
                for matched_record in _match(
                    inspire_record,
                    queries=queries_,
                    index='records-hep',
                    doc_type='hep'
                ):
                    matched_recid = matched_record.record.get('control_number')
                    if any([got_match(correct_match_map, str(matched_recid), doi) for doi in dois]):
                        found = True
                        print '## Got a good match! with recid: ', matched_recid
                if not found:
                    print 'Did not find a match for DOIs', dois


def got_match(correct_match_map, recid, doi):
    return correct_match_map[doi] == recid


if __name__ == '__main__':
    main()
