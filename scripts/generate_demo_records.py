import gzip
import json

from dojson.contrib.marc21.utils import create_record as marc_create_record

from invenio_indexer.signals import before_record_index

from inspirehep.factory import create_app
from inspirehep.modules.migrator.tasks.records import chunker, create_record, split_stream

files = [
    'demo_records/demo_records_manual_merges.tar.gz',
    'demo_records/demo_records_publisher.tar.gz',
    'demo_records/demo_records_random.tar.gz'
]

CHUNK_SIZE = 500

app = create_app()

with app.app_context():
    for filename in files:
        fd = gzip.open(filename)
        for i, chunk in enumerate(chunker(split_stream(fd), CHUNK_SIZE)):
            out = "demo_records/{0}{1}.json".format(filename, i)
            with open(out, 'w') as out_fd:
                for raw_record in chunk:
                    record = marc_create_record(raw_record, keep_singletons=False)
                    try:
                        json_record = create_record(record)
                        before_record_index.send(app, json=json_record)
                    except:
                        pass
                    out_fd.write('{ "index":  { "_index": "records-hep", "_type": "hep" }}\n')
                    out_fd.write(json.dumps(json_record) + '\n')
