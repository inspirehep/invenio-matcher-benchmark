import argparse
import datetime
import glob
import os

import editdistance
from itertools import product

from dojson.contrib.marc21.utils import create_record as marc_create_record

from invenio_matcher.api import match as _match

from inspire_dojson.processors import overdo_marc_dict
from inspirehep.modules.migrator.tasks.records import split_stream
from inspirehep.utils.record import get_value
from inspirehep.factory import create_app


def generate_doi_map():
    with open('test_matches_clean.txt', 'r') as fd:
        result = {}
        for line in fd:
            doi, recid = line.split('--')
            recid = recid.strip()
            result[doi.strip()] = recid
        return result


def generate_recid_map():
    with open('manual_matches.txt', 'r') as fd:
        result = {}
        for line in fd:
            recid1, recid2 = line.split('--')
            recid2 = recid2.strip()
            result[recid1.strip()] = recid2
        return result


def generate_no_match_list():
    with open('no_match.txt', 'r') as fd:
        return fd.read().splitlines()


def get_mlt_record(inspire_record):
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
        number_of_authors = len(record['authors'])
        matches = len(AuthorComparator(record['authors'], result.record['authors']).matches)
        author_score = matches/float(number_of_authors)

    title_max_score = 0.5
    if record.get('titles') and result.record.get('titles'):
        record_titles = [r['title'].lower() for r in record['titles']]
        result_titles = [r['title'].lower() for r in result.record['titles']]

        for titles in product(record_titles, result_titles):
            record_tokens = set(titles[0].split())
            result_tokens = set(titles[1].split())
            title_score = len(record_tokens & result_tokens)/float(len(record_tokens | result_tokens))
            if title_score > title_max_score:
                title_max_score = title_score

    if (author_score + title_max_score)/2 > 0.5:
        return True
    else:
        return False


def is_good_match(doi_match_map, recid_match_map, dois, control_number, matched_recid):
    def _got_match(dictionary, key, value):
        return dictionary.get(key) == value

    if dois:
        if any([_got_match(doi_match_map, doi, str(matched_recid)) for doi in dois]):
            return True

    if control_number:
        if _got_match(recid_match_map, str(control_number), str(matched_recid)):
            return True

    return False


def write(content, filename):
    with open(filename, 'w') as fd:
        fd.write(content)


def main(args):
    total = 0
    true_positives = 0
    false_positives = 0
    false_negatives = 0
    true_negatives = 0
    multiple_exact = 0  # Keep track of cases where multiple records match a exact query
    doi_match_map = generate_doi_map()
    recid_match_map = generate_recid_map()
    no_match_list = generate_no_match_list()

    filenames = []

    for filename in args.names:
        if os.path.isfile(filename):
            filenames.append(filename)
        else:
            filenames.extend(glob.glob(filename + '/*.xml'))

    if args.output:
        false_positives_dir = 'false_positives_{0}'.format(datetime.datetime.now().isoformat())
        false_negatives_dir = 'false_negatives_{0}'.format(datetime.datetime.now().isoformat())
        os.makedirs(false_positives_dir)
        os.makedirs(false_negatives_dir)

    app = create_app()
    with app.app_context():
        for filename in filenames:
            with open(filename, 'r') as fd:
                for marcxml in split_stream(fd):
                    marc_record = marc_create_record(marcxml, keep_singletons=False)
                    try:
                        inspire_record = overdo_marc_dict(marc_record)
                    except TypeError:
                        # Some bad metadata in the record - skip
                        pass

                    control_number = get_value(inspire_record, 'control_number')
                    dois = get_value(inspire_record, 'dois.value')
                    arxiv_eprints = get_value(inspire_record, 'arxiv_eprints.value')        
                    report_numbers = get_value(inspire_record, 'report_numbers.value')

                    if not dois and not control_number:
                        # FIXME all the correct/incorrect match files are based on doi
                        continue

                    total += 1

                    print 'Going to match DOIs: ', dois
                    print 'Going to match arXiv eprints: ', arxiv_eprints

                    # Step 1 - apply exact matches
                    queries_ = [
                        {'type': 'exact', 'match': 'dois.value.raw', 'values': dois},
                        {'type': 'exact', 'match': 'arxiv_eprints.value.raw', 'values': arxiv_eprints},
                        {'type': 'exact', 'match': 'report_numbers.value.raw', 'values': report_numbers}
                    ]
                    matched_exact_records = list(_match(
                        inspire_record,
                        queries=queries_,
                        index='records-hep',
                        doc_type='hep'
                    ))

                    if len(matched_exact_records) == 1:
                        matched_recid = matched_exact_records[0].record.get('control_number')
                        if is_good_match(doi_match_map, recid_match_map, dois, control_number, matched_recid):
                            true_positives += 1
                            print '++ Got a good match! with recid: ', matched_recid
                        else:
                            false_positives += 1
                            if args.output:
                                write(marcxml, false_positives_dir + os.path.sep + str(total) + '.xml')
                            print '-- Got a wrong match', matched_exact_records[0].record.get('control_number')
                        continue
                    elif len(matched_exact_records) > 1:
                        # FIXME Do we treat multiple matches as a false positive?
                        false_positives += 1
                        multiple_exact += 1
                        if args.output:
                            write(marcxml, false_positives_dir + os.path.sep + str(total) + '.xml')
                        print '-- More than one match found: ', [m.record.get('control_number') for m in matched_exact_records]
                        continue

                    print 'Did not find a match for DOIs or arXiv ePrints'
                    # Step 2 - apply fuzzy queries
                    print 'Executing mlt query...'

                    match_record = get_mlt_record(inspire_record)
                    queries_ = [{'type': 'fuzzy', 'match': match_record}]
                    matched_fuzzy_records = list(_match(
                        inspire_record,
                        queries=queries_,
                        index='records-hep',
                        doc_type='hep',
                        validator=validator
                    ))


                    if len(matched_fuzzy_records) >= 1:
                        first_result = matched_fuzzy_records[0]
                        matched_recid = first_result.record.get('control_number')
                        print '++ Fuzzy result found: ', matched_recid
                        if is_good_match(doi_match_map, recid_match_map, dois, control_number, matched_recid):
                            true_positives += 1
                            print '++ Got a good match! with recid: ', matched_recid
                        else:
                            false_positives += 1
                            if args.output:
                                write(marcxml, false_positives_dir + os.path.sep + str(total) + '.xml')
                        continue

                    # No record matched, check if it was a true negative
                    if dois and (set(no_match_list) & set(dois) != set()):
                        true_negatives += 1
                    else:
                        false_negatives += 1
                        if args.output:
                            write(marcxml, false_negatives_dir + os.path.sep + str(total) + '.xml')

                    print '\n'

        print '#### STATS ####'
        print 'Total analyzed: ', total
        print 'True positives: ', true_positives
        print 'False positives: ', false_positives
        print 'True negatives: ', true_negatives
        print 'False negatives: ', false_negatives
        print 'Duplicate exact match: ', multiple_exact
        print '------------------------'
        if true_positives + false_positives > 0:
            precision = true_positives/float(true_positives + false_positives)
        else:
            precision = 0
        if true_positives + false_negatives > 0:
            recall = true_positives/float(true_positives + false_negatives)
        else:
            recall = 0
        print 'Precision: ', precision
        print 'Recall: ', recall
        if precision or recall:
            print 'F1 Score: ', 2.0/(1/precision + 1/recall)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test invenio-matcher.')
    parser.add_argument('names', nargs='+', help='File(s) or directory(ies) to run matcher against.')
    parser.add_argument('--output', help='Output files with false positives and false negatives', action='store_true')
    args = parser.parse_args()
    main(args)
