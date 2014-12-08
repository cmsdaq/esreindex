from elasticsearch import Elasticsearch
from elasticsearch.helpers import *

import json
import sys
import math
import time
import datetime

def streaming_bulk2(client, actions, chunk_size=500, raise_on_error=False, expand_action_callback=expand_action, **kwargs):
    """
    Streaming bulk consumes actions from the iterable passed in and yields
    results per action. For non-streaming usecases use
    :func:`~elasticsearch.helpers.bulk` which is a wrapper around streaming
    bulk that returns summary information about the bulk operation once the
    entire input is consumed and sent.

    This function expects the action to be in the format as returned by
    :meth:`~elasticsearch.Elasticsearch.search`, for example::

        {
            '_index': 'index-name',
            '_type': 'document',
            '_id': 42,
            '_parent': 5,
            '_ttl': '1d',
            '_source': {
                ...
            }
        }

    Alternatively, if `_source` is not present, it will pop all metadata fields
    from the doc and use the rest as the document data.

    If you wish to perform other operations, like `delete` or `update` use the
    `_op_type` field in your actions (`_op_type` defaults to `index`)::

        {
            '_op_type': 'delete',
            '_index': 'index-name',
            '_type': 'document',
            '_id': 42,
        }
        {
            '_op_type': 'update',
            '_index': 'index-name',
            '_type': 'document',
            '_id': 42,
            'doc': {'question': 'The life, universe and everything.'}
        }

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterable containing the actions to be executed
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg raise_on_error: raise `BulkIndexError` containing errors (as `.errors`
        from the execution of the last chunk)
    :arg expand_action_callback: callback executed on each action passed in,
        should return a tuple containing the action line and the data line
        (`None` if data line should be omitted).
    """

    #print "STREAMING BULK2"
    
    actions = map(expand_action_callback, actions)

    # if raise on error is set, we need to collect errors per chunk before raising them
    errors = []

    while True:
        chunk = islice(actions, chunk_size)
        #print "READ" + str(time.time())
        bulk_actions = []
        for action, data in chunk:
            bulk_actions.append(action)
            if data is not None:
                bulk_actions.append(data)

        if not bulk_actions:
            return

        #print "WRITE " + str(time.time())

        
        #print json.dumps(bulk_actions,indent=2)
        #print len(bulk_actions)

        #sys.exit(0)


        # send the actual request
        resp = client.bulk(bulk_actions, **kwargs)

       # go through request-reponse pairs and detect failures
        for op_type, item in map(methodcaller('popitem'), resp['items']):
            ok = 200 <= item.get('status', 500) < 300
            if not ok and raise_on_error:
                errors.append({op_type: item})

            if not errors:
                # if we are not just recording all errors to be able to raise
                # them all at once, yield items individually
                yield ok, {op_type: item}

        if errors:
            raise BulkIndexError('%i document(s) failed to index.' % len(errors), errors)



def bulk2(client, actions, stats_only=False, **kwargs):
    """
    Helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks. It returns a tuple with summary
    information - number of successfully executed actions and either list of
    errors or number of errors if `stats_only` is set to `True`.

    See :func:`~elasticsearch.helpers.streaming_bulk` for more information
    and accepted formats.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterator containing the actions
    :arg stats_only: if `True` only report number of successful/failed
        operations instead of just number of successful and a list of error responses

    Any additional keyword arguments will be passed to
    :func:`~elasticsearch.helpers.streaming_bulk` which is used to execute
    the operation.
    """
    #print "BULK2"
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = []

    for ok, item in streaming_bulk2(client, actions, **kwargs):
        # go through request-reponse pairs and detect failures
        if not ok:
            if not stats_only:
                errors.append(item)
            failed += 1
        else:
            success += 1

    return success, failed if stats_only else errors

# preserve the name for backwards compatibility
bulk_index = bulk

def scan2(client, query=None, scroll='5m', **kwargs):
    """
    Simple abstraction on top of the
    :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
    yields all hits as returned by underlining scroll requests.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search

    Any additional keyword arguments will be passed to the initial
    :meth:`~elasticsearch.Elasticsearch.search` call.
    """
    # initial search to

    resp = client.search(body=query, search_type='scan', scroll=scroll, fields = "_routing,_source,_parent,_timestamp", **kwargs)

    scroll_id = resp['_scroll_id']

    while True:

        resp = client.scroll(scroll_id, scroll=scroll)
        
        if not resp['hits']['hits']:
            break
        for hit in resp['hits']['hits']:
            if "fields" in hit.keys():
                for field in hit['fields'].keys():
                    hit[field] = hit["fields"][field]
                del hit["fields"]
            yield hit
        scroll_id = resp['_scroll_id']

def reindex2(client, source_index, target_index, target_client=None, chunk_size=500, scroll='5m'):
    """
    Reindex all documents from one index to another, potentially (if
    `target_client` is specified) on a different cluster.

    .. note::

        This helper doesn't transfer mappings, just the data.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use (for
        read if `target_client` is specified as well)
    :arg source_index: index (or list of indices) to read documents from
    :arg target_index: name of the index in the target cluster to populate
    :arg target_client: optional, is specified will be used for writing (thus
        enabling reindex between clusters)
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    """
    target_client = client if target_client is None else target_client

    total = client.count(source_index)["count"]


        
    docs = scan2(client, index=source_index, scroll=scroll)
    def _change_doc_index(hits, index, total):
        eta = 0
        oldtime = time.time()
        
        timedelta = 0
        partial = 0
        chunk_num = 0
        pb_size = 30
        pb_update_int = chunk_size
        pb_bar_num = 0
        pb_space_num = pb_size
        perc = 0
        
        outstring = "\r[" + "=" * pb_bar_num +  " " * pb_space_num + "] " + str(perc) + "%, " + str(partial) + " of " + str(total) + " reindexed." 
        outstring += " time per chunk: N/A , ETA: N/A " 
        sys.stdout.write(outstring)
        sys.stdout.flush()

        for h in hits:
            h['_index'] = index
            yield h

            partial += 1
            if (partial % (pb_update_int)) == 0:
                chunk_num += 1
                timedelta = timedelta + (time.time() - oldtime)
                timePerChunk = timedelta / chunk_num
                oldtime = time.time()
                eta = str(datetime.timedelta(seconds = round(timePerChunk*(total - partial )/pb_update_int,0))) 
                perc = round(float(partial)/total*100,2)
                pb_bar_num = int(perc*pb_size/100)
                pb_space_num = pb_size-pb_bar_num
                outstring = "\r[" + "=" * pb_bar_num +  " " * pb_space_num + "] " + str(perc) + "%, " + str(partial) + " of " + str(total) + " reindexed." 
                outstring += " timePerChunk: " + str(round(timePerChunk,2)) + "s, ETA: " + eta
                sys.stdout.write(outstring)
                sys.stdout.flush()
    
    return bulk2(target_client, _change_doc_index(docs, target_index, total), chunk_size=chunk_size, stats_only=True)



if __name__ == '__main__':
    
    old_hosts = ["0:9300"]
    new_hosts = ["0:9200"]
    old_index = "runriver_stats"
    new_index = "runriver_stats"

    #read_aliases = ["runindex_prod_read"]
    #write_aliases = ["runindex_prod_write","runindex_prod"]
    chunk_size = 5000

    es_old = Elasticsearch(old_hosts)
    es_new = Elasticsearch(new_hosts)

    print "GET OLD MAPPING "
    mapping = es_old.indices.get_mapping(index = old_index)
    #print json.dumps(mapping,indent=2)

    print "GET OLD SETTINGS "
    settings = es_old.indices.get_settings(index = old_index)
    #print json.dumps(settings,indent=2)

    print "CREATE BODY FOR NEW INDEX "
    body = mapping[old_index].copy()
    body.update(settings[old_index])  
    
    """ Here you can change body['settings'] and body ['mappings'] fro the new index """

    #body["settings"]["index"]["number_of_shards"] = 2;
    #body["settings"]["index"]["number_of_replicas"] = 1;
    #print json.dumps(body,indent=2)

    print "CREATE NEW INDEX "
    print es_new.indices.create(index = new_index, body = body , ignore = 400)

#    print "LINK ALIASES {0} TO {1}".format(repr(read_aliases),new_index)
#    body = {"actions":[]};
#    for alias in read_aliases:
#        body["actions"].append({ "add":    { "index": new_index, "alias": alias }})
#    print es.indices.update_aliases(body)

#    print "SWITCH ALIASES {0} FROM {1} TO {2}".format(repr(write_aliases),old_index,new_index)
#    body = {"actions":[]};
#    for alias in write_aliases:
#        body["actions"].append({ "remove": { "index": old_index, "alias": alias }})
#        body["actions"].append({ "add":    { "index": new_index, "alias": alias }})
#    print es.indices.update_aliases(body)

      
    #print "SET {0} AS READONLY".format(old_index).ljust(80,">")
    #print es.indices.put_settings(index = old_index, body = { "index": { "blocks.read_only": True }} )


    print "{0} COPY INDEX {1} FROM {2} TO {3}".format( time.strftime("%Y-%m-%d %H:%M:%S") , old_index , old_hosts, new_hosts)
    startTime = time.time()
    resp =  reindex2(client = es_old, source_index = old_index , target_client = es_new, target_index = new_index, chunk_size = chunk_size )
    endTime = time.time()
    timeElapsed = str(datetime.timedelta( seconds = endTime - startTime) )
    print ""
    outstr =  time.strftime("%Y-%m-%d %H:%M:%S") + ' (success,failed): ' + repr(resp) + ' time elapsed: ' + timeElapsed
    print outstr.rjust(80,' ')
    
#    print "UNLINK ALIASES {0} FROM {1}".format(repr(read_aliases),old_index).ljust(80,">")
#    body = {"actions":[]};
#    for alias in read_aliases:
#        body["actions"].append({ "remove": { "index": old_index, "alias": alias }})
#        body["actions"].append({ "add":    { "index": new_index, "alias": alias }})
#    #print json.dumps(body,indent=2)
#    print es.indices.update_aliases(body)

    sys.exit(0)

#    'BACKUP OLD INDEX'
#
#    print "DELETE INDEX {0}".format(old_index)
#    print es.indices.delete(index = old_index)





