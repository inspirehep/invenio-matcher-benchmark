import glob
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


def main():
    if len(sys.argv) < 2:
        print 'A filename is required'
        return 1

    app = create_app()

    true_positives = 0
    false_positives = 0
    false_negatives = 0
    true_negatives = 0
    multiple_exact = 0  # Keep track of cases where multiple records match a exact query

    with app.app_context():

        folder = sys.argv[1]

        correct_match_map = generate_match_map()

        total = 0  # Total number of records found in test_matches_clean

        for filename in glob.glob(folder + '/*.xml'):
            with open(filename, 'r') as fd:
                for marcxml in split_stream(fd):
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
                        continue
                    elif len(matched_exact_records) > 1:
                        #FIXME Do we treat multiple matches as a false positive?
                        false_positives += 1
                        multiple_exact += 1
                        print '-- More than one match found: ', [m.record.get('control_number') for m in matched_exact_records]
                        continue
                        
                    print 'Did not find a match for DOIs or arXiv ePrints'
                    # Step 2 - apply fuzzy queries
                    print 'Executing mlt query...'

                    queries_ = [{'type': 'fuzzy', 'match': 'dummy'}]  # Match is needed in invenio-matcher

                    matched_fuzzy_records = list(_match(
                        inspire_record,
                        queries=queries_,
                        index='records-hep',
                        doc_type='hep',
                        json=inspire_record
                    ))

                    if len(matched_fuzzy_records) > 1:
                        first_result = matched_fuzzy_records[0]
                        second_result = matched_fuzzy_records[1]
                        ratio = first_result.score/second_result.score
                        if ratio > 2 and first_result.score > 2:  # These numbers need to be learned
                            matched_recid = first_result.record.get('control_number')
                            print '++ Fuzzy result found: ', matched_recid
                            if any([got_match(correct_match_map, str(matched_recid), doi) for doi in dois]):
                                true_positives += 1
                                print '++ Got a good match! with recid: ', matched_recid
                            else:
                                false_positives += 1
                        else:
                            false_negatives += 1
                            print '-- No matches were found '
                    else:
                        false_negatives += 1
                        print '-- No matches were found '

                    print '\n'

        print '#### STATS ####'
        print 'Total analyzed: ', total
        print 'True positives: ', true_positives
        print 'False positives: ', false_positives
        print 'False negatives: ', false_negatives
        print '------------------------'
        precision = true_positives/float(true_positives + false_positives)
        recall = true_positives/float(true_positives + false_negatives)
        print 'Precision: ', precision
        print 'Recall: ', recall
        print 'F1 Score: ', 2.0/(1/precision + 1/recall)





def got_match(correct_match_map, recid, doi):
    return correct_match_map.get(doi) == recid


if __name__ == '__main__':
    main()
