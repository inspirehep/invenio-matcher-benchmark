import argparse
import glob
import os
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
            recid = recid.strip()
            if recid.endswith('N'):
                recid = recid[:-1]
            result[doi.strip()] = recid
        return result


def main(args):
    app = create_app()

    true_positives = 0
    false_positives = 0
    false_negatives = 0
    true_negatives = 0
    multiple_exact = 0  # Keep track of cases where multiple records match a exact query

    with app.app_context():

        if os.path.isfile(args.name):
            filenames = [args.name]
        else:
            filenames = glob.glob(args.name + '/*.xml')

        correct_match_map = generate_match_map()

        total = 0  # Total number of records found in test_matches_clean

        for filename in filenames:
            with open(filename, 'r') as fd:
                for i, marcxml in enumerate(split_stream(fd)):
                    marc_record = marc_create_record(
                        marcxml, keep_singletons=False)
                    inspire_record = overdo_marc_dict(marc_record)
                    dois = get_value(inspire_record, 'dois.value')

                    if not dois:
                        continue
                    
                    if set(dois) & correct_match_map.viewkeys() == set():
                        print "DOI {} not found in test_matches_clean.txt".format(dois)
                        continue
                    
                    total += 1
                    arxiv_eprints = get_value(
                        inspire_record, 'arxiv_eprints.value')
                    print 'Going to match DOIs: ', dois
                    print 'Going to match arXiv eprints: ', arxiv_eprints

                    # Step 1 - apply exact matches
                    queries_ = [
                        {'type': 'exact', 'match': 'dois.value.raw', 'values': dois},
                        {'type': 'exact', 'match': 'arxiv_eprints.value.raw',
                            'values': arxiv_eprints}
                    ]

                    matched_exact_records = list(_match(
                        inspire_record,
                        queries=queries_,
                        index='records-hep',
                        doc_type='hep'
                    ))

                    if len(matched_exact_records) == 1:
                        matched_recid = matched_exact_records[0].record.get('control_number')
                        if any([got_match(correct_match_map, str(matched_recid), doi) for doi in dois]):
                            true_positives += 1
                            print '++ Got a good match! with recid: ', matched_recid
                        else:
                            false_positives += 1
                            print '-- Got a wrong match', matched_exact_records[0].record.get('control_number')
                            with open('false_positives/{0}.xml'.format(i), 'w') as fd:
                                fd.write(marcxml)
                        continue
                    elif len(matched_exact_records) > 1:
                        #FIXME Do we treat multiple matches as a false positive?
                        false_positives += 1
                        multiple_exact += 1
                        print '-- More than one match found: ', [m.record.get('control_number') for m in matched_exact_records]
                        with open('false_positives/{0}.xml'.format(i), 'w') as fd:
                            fd.write(marcxml)
                        continue
                        
                    print 'Did not find a match for DOIs or arXiv ePrints'
                    # Step 2 - apply fuzzy queries
                    print 'Executing mlt query...'

                    match_record = {}
                    if inspire_record.get('titles'):
                        match_record['titles'] = inspire_record['titles']
                    if inspire_record.get('abstracts'):
                        match_record['abstracts'] = inspire_record['abstracts']
                    if inspire_record.get('authors'):
                        match_record['authors'] = inspire_record['authors'][:3]

                    queries_ = [{'type': 'fuzzy', 'match': match_record}]

                    matched_fuzzy_records = list(_match(
                        inspire_record,
                        queries=queries_,
                        index='records-hep',
                        doc_type='hep'
                    ))

                    import ipdb; ipdb.set_trace()

                    if len(matched_fuzzy_records) == 1:
                        first_result = matched_fuzzy_records[0]                      
                        matched_recid = first_result.record.get('control_number')
                        if any([got_match(correct_match_map, str(matched_recid), doi) for doi in dois]):
                            true_positives += 1
                            print '++ Got a good match! with recid: ', matched_recid
                    elif len(matched_fuzzy_records) > 1:
                        first_result = matched_fuzzy_records[0]
                        second_result = matched_fuzzy_records[1]
                        ratio = first_result.score/second_result.score
                        # if ratio > 2 and first_result.score > 2:  # These numbers need to be learned
                        matched_recid = first_result.record.get('control_number')
                        print '++ Fuzzy result found: ', matched_recid
                        if any([got_match(correct_match_map, str(matched_recid), doi) for doi in dois]):
                            true_positives += 1
                            print '++ Got a good match! with recid: ', matched_recid
                        else:
                            false_positives += 1
                            with open('false_positives/{0}.xml'.format(i), 'w') as fd:
                                fd.write(marcxml)
                        # else:
                        #     with open('false_negatives/{0}.xml'.format(i), 'w') as fd:
                        #         fd.write(marcxml)
                        #     false_negatives += 1
                        #     print '-- No matches were found '
                    else:
                        with open('false_negatives/{0}.xml'.format(i), 'w') as fd:
                            fd.write(marcxml)
                        false_negatives += 1
                        print '-- No matches were found '

                    print '\n'

        print '#### STATS ####'
        print 'Total analyzed: ', total
        print 'True positives: ', true_positives
        print 'False positives: ', false_positives
        print 'False negatives: ', false_negatives
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





def got_match(correct_match_map, recid, doi):
    return correct_match_map.get(doi) == recid


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test invenio-matcher.')
    parser.add_argument('name', help='File or directory to run matcher against.')
    args = parser.parse_args()
    main(args)
