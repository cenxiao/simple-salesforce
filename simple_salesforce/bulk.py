""" Classes for interacting with Salesforce Bulk API """

try:
    from collections import OrderedDict
except ImportError:
    # Python < 2.7
    from ordereddict import OrderedDict

import json
import requests
from time import sleep
from simple_salesforce.util import call_salesforce
from simple_salesforce.util import getUniqueElementValueFromXmlString, getElementValuesFromXmlString, getElementsFromXmlString, getUniqueElementValueFromXmlElement
import os
import io
import pdb


class SFBulkHandler(object):
    """ Bulk API request handler
    Intermediate class which allows us to use commands,
     such as 'sf.bulk.Contacts.insert(...)'
    This is really just a middle layer, whose sole purpose is
    to allow the above syntax
    """

    def __init__(self, session_id, bulk_url, proxies=None, session=None):
        """Initialize the instance with the given parameters.

        Arguments:

        * session_id -- the session ID for authenticating to Salesforce
        * bulk_url -- API endpoint set in Salesforce instance
        * proxies -- the optional map of scheme to proxy server
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        """
        self.session_id = session_id
        self.session = session or requests.Session()
        self.bulk_url = bulk_url
        # don't wipe out original proxies with None
        if not session and proxies is not None:
            self.session.proxies = proxies

        # Define these headers separate from Salesforce class,
        # as bulk uses a slightly different format
        self.headers = {
            'Content-Type': 'application/json',
            'X-SFDC-Session': self.session_id,
            'X-PrettyPrint': '1'
        }

    def __getattr__(self, name):
        return SFBulkType(object_name=name, bulk_url=self.bulk_url,
                          headers=self.headers, session=self.session)

class SFBulkType(object):
    """ Interface to Bulk/Async API functions"""

    def __init__(self, object_name, bulk_url, headers, session):
        """Initialize the instance with the given parameters.

        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. `Lead` or `Contact`
        * bulk_url -- API endpoint set in Salesforce instance
        * headers -- bulk API headers
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        """
        self.object_name = object_name
        self.bulk_url = bulk_url
        self.session = session
        self.headers = headers
        self.csv_headers = {
            'Content-Type': 'text/csv',
            'X-SFDC-Session': headers['X-SFDC-Session'],
            'X-PrettyPrint': '1'
        }

    def _create_job(self, operation, object_name, external_id_field=None, isPkChunk = False):
        """ Create a bulk job

        Arguments:

        * operation -- Bulk operation to be performed by job
        * object_name -- SF object
        * external_id_field -- unique identifier field for upsert operations
        """
        #payload = "operation,object,contentType\\n{},{},{}".format(operation, object_name, 'CSV')
        payload = {
            'operation': operation,
            'object': object_name,
            'contentType': 'CSV'
        }

        if operation == 'upsert':
            payload['externalIdFieldName'] = external_id_field

        url = "{}{}".format(self.bulk_url, 'job')
        header = self.headers
        if isPkChunk:
            header['Sforce-Enable-PKChunking'] = 'true'

        result = call_salesforce(url=url, method='POST', session=self.session,
                                  headers=header,
                                  data=json.dumps(payload))
        return result.json(object_pairs_hook=OrderedDict)

    def _close_job(self, job_id):
        """ Close a bulk job """
        payload = {
            'state': 'Closed'
        }

        url = "{}{}{}".format(self.bulk_url, 'job/', job_id)

        result = call_salesforce(url=url, method='POST', session=self.session,
                                  headers=self.headers,
                                  data=json.dumps(payload))
        return result.json(object_pairs_hook=OrderedDict)

    def _get_job(self, job_id):
        """ Get an existing job to check the status """
        url = "{}{}{}".format(self.bulk_url, 'job/', job_id)

        result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers)
        return result.json(object_pairs_hook=OrderedDict)

    def _add_batch(self, job_id, data, operation):
        """ Add a set of data as a batch to an existing job
        Separating this out in case of later
        implementations involving multiple batches
        """

        url = "{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch')

        if operation != 'query':
            data = json.dumps(data)

        result = call_salesforce(url=url, method='POST', session=self.session,
                                  headers=self.csv_headers, data=data)

        result_json = {
            'jobId': getUniqueElementValueFromXmlString(result.content, 'jobId'),
            'id': getUniqueElementValueFromXmlString(result.content, 'id')
        }

        return result_json

    def _add_batch_stream(self, job_id, data, operation):
        """ Add a set of data as a batch to an existing job
        Separating this out in case of later
        implementations involving multiple batches
        """

        url = "{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch')

        result = call_salesforce(url=url, method='POST', session=self.session,
                                  headers=self.csv_headers, data=data, is_stream=True)

        result_json = {
            'jobId': getUniqueElementValueFromXmlString(result.content, 'jobId'),
            'id': getUniqueElementValueFromXmlString(result.content, 'id')
        }

        return result_json

    def _get_batch(self, job_id, batch_id):
        """ Get an existing batch to check the status """

        url = "{}{}{}{}{}".format(self.bulk_url, 'job/',
                                  job_id, '/batch/', batch_id)

        result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.csv_headers)
                                  
        resultJson = {
            'state' : getUniqueElementValueFromXmlString(result.content, 'state')
        }
        return resultJson

    def _get_batch_results(self, job_id, batch_id, operation):
        """ retrieve a set of results from a completed job """

        url = "{}{}{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch/',
                                    batch_id, '/result')

        result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers)

        if operation == 'query':
            query_result = []
            for batch_result in result.json():
                url_query_results = "{}{}{}".format(url, '/', batch_result)
                batch_result_json = call_salesforce(url=url_query_results,
                                    method='GET',
                                    session=self.session,
                                    headers=self.headers).json()
                query_result.extend(batch_result_json)
            return query_result
            

        return result.json()

    def _get_remaining_batches(self, job_id):
        url = "{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch')

        result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers)
        batch_infos = getElementsFromXmlString(result.content, 'batchInfo')
        batch_json_objs = list(map(lambda x: {
            'id': getUniqueElementValueFromXmlElement(x, 'id'),
            'jobId': getUniqueElementValueFromXmlElement(x, 'jobId'),
            'state': getUniqueElementValueFromXmlElement(x, 'state')
        }, batch_infos))
        self._print_remaining_batches(batch_json_objs)
        return batch_json_objs
    
    def _get_batch_results_path(self, job_id, batch_id, operation):
        """ retrieve a set of results from a completed job into the local file system"""

        url = "{}{}{}{}{}{}".format(self.bulk_url, 'job/', job_id, '/batch/',
                                    batch_id, '/result')

        if not os.path.exists(".process"):
            os.mkdir(".process")

        file_paths = []
        if operation == 'query':
            folder_name = ".process/{}".format(job_id)
            result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers)
            batch_results = getElementValuesFromXmlString(result.content, 'result')
            
            for index, batch_result in enumerate(batch_results, start=1):
                url_query_results = "{}{}{}".format(url, '/', batch_result)
                batch_result_stream = call_salesforce(url=url_query_results,
                                    method='GET',
                                    session=self.session,
                                    headers=self.headers, is_stream=True)
                if batch_result_stream.encoding is None:
                    batch_result_stream.encoding = 'utf-8'

                if not os.path.exists(folder_name):
                    os.mkdir(folder_name)
                file_name = "{}/{}_{}.csv".format(folder_name, batch_id, index)
                with io.open(file_name, "w+", encoding="utf-8") as f:
                    for chunk in batch_result_stream.iter_content(chunk_size=1024, decode_unicode=True):
                        if chunk:
                            f.write(chunk)
                print("Pulling batch {} into {}".format(batch_id, file_name))
                file_paths.append(file_name)
        else:
            folder_name = ".process/import/{}".format(job_id)
            if not os.path.exists(folder_name):
                    os.mkdir(folder_name)
            result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers, is_stream=True)
            file_name = "{}/Result_{}.csv".format(folder_name, batch_id)
            with io.open(file_name, "wb") as f:                      
                for chunk in result.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            file_paths.append(file_name)

        return file_paths
    
    #pylint: disable=R0913
    def _bulk_operation(self, object_name, operation, data,
                        external_id_field=None, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk API request

        Arguments:

        * object_name -- SF object
        * operation -- Bulk operation to be performed by job
        * data -- list of dict to be passed as a batch
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        """

        job = self._create_job(object_name=object_name, operation=operation,
                               external_id_field=external_id_field)

        batch = self._add_batch(job_id=job['id'], data=data,

                                operation=operation)

        self._close_job(job_id=job['id'])

        batch_status = self._get_batch(job_id=batch['jobId'],
                                       batch_id=batch['id'])['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=batch['jobId'],
                                           batch_id=batch['id'])['state']
        if operation == 'query':
            results = self._get_batch_results_path(job_id=batch['jobId'],
                                          batch_id=batch['id'], operation=operation)
        else:
            results = self._get_batch_results(job_id=batch['jobId'],
                                          batch_id=batch['id'],
                                          operation=operation)
        return results

    def _bulk_operation_to_file_pkchunk(self, object_name, operation, data,
                        external_id_field=None, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk API request

        Arguments:

        * object_name -- SF object
        * operation -- Bulk operation to be performed by job
        * data -- list of dict to be passed as a batch
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        """

        job = self._create_job(object_name=object_name, operation=operation,
                               external_id_field=external_id_field, isPkChunk=True)

        batch = self._add_batch(job_id=job['id'], data=data,
                                operation=operation)

        self._close_job(job_id=job['id'])

        #Original batch will be set to Not Processed. 
        #batch_status = self._get_batch(job_id=batch['jobId'],
        #                               batch_id=batch['id'])['state']

        remaining_batches = self._get_remaining_batches(job_id=batch['jobId'])

        results = []
        imported_batches = []
        while True: 
            #Check the completed batches:
            completed_batches = filter(lambda x: x['state'] == 'Completed' and x['id'] not in imported_batches, remaining_batches)
            #Process the completed batches:
            for batch in completed_batches:
                result = self._get_batch_results_path(job_id=batch['jobId'], batch_id=batch['id'], operation=operation)
                results.extend(result)
                imported_batches.append(batch['id'])
            
            #Check to see if there is any pending job
            if any(x['state'] not in ['Completed', 'Failed', 'Not Processed', 'NotProcessed'] for x in remaining_batches) and len(remaining_batches) > 0 :
                sleep(wait)
            else:
                break

            remaining_batches = self._get_remaining_batches(job_id=batch['jobId'])

        return results

    def _bulk_operation_to_file(self, object_name, operation, data,
                        external_id_field=None, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk API request

        Arguments:

        * object_name -- SF object
        * operation -- Bulk operation to be performed by job
        * data -- list of dict to be passed as a batch
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        """

        job = self._create_job(object_name=object_name, operation=operation,
                               external_id_field=external_id_field)

        batch = self._add_batch(job_id=job['id'], data=data,
                                operation=operation)

        self._close_job(job_id=job['id'])

        batch_status = self._get_batch(job_id=batch['jobId'],
                                       batch_id=batch['id'])['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=batch['jobId'],
                                           batch_id=batch['id'])['state']
        if operation == 'query':
            file_pathes = self._get_batch_results_path(job_id=batch['jobId'],
                                          batch_id=batch['id'], operation=operation)
            return file_pathes
        else:
            raise Exception('bulk operation to file only supports query')
        return []
    
    def _bulk_operation_with_large_input_file(self, object_name, operation, data_file_path,
                        external_id_field=None, wait=5):
        #########################    
        # Salesforce Limitation #
        #########################
        '''
        Batches for data loads can consist of a single CSV, XML, or JSON file that is no larger than 10 MB.
        A batch can contain a maximum of 10,000 records.
        A batch can contain a maximum of 10,000,000 characters for all the data in a batch.
        A field can contain a maximum of 32,000 characters.
        A record can contain a maximum of 5,000 fields.
        A record can contain a maximum of 400,000 characters for all its fields.
        A batch must contain some content or an error occurs.
        ''' 
        MAX_RECORDS_PER_BATCH= 10000
        MAX_CHARACTER= 10000000
        MAX_CHARACTER_PER_FIELD=32000
        MAX_FIELDS=5000
        MAX_FIELDS_CHARACTERS=400000
     
        #Create a job
        job = self._create_job(object_name=object_name, operation=operation, isPkChunk=True)
        print("Create a job: {}".format(job['id']))

        #Slice the input file into batches
        process_folder='.process/import/{}'.format(job['id'])
        if (not os.path.exists(process_folder)):
            os.makedirs(process_folder)
        
        chunked_files = []
        try:
            with open(data_file_path, "r", encoding="utf-8") as f:
                header = f.readline()
                previous_batch_number = -1
                for line_number, line in enumerate(f, start=0):
                    batch_number = int(line_number / MAX_RECORDS_PER_BATCH)
                    file_name = '{}/{}.{}'.format(process_folder, batch_number, 'csv')
                    #Add the header if the file doesn't exist
                    if (not os.path.exists(file_name)):
                        line = header + line
                    with open(file_name, 'a+', encoding="utf-8") as out:
                        out.write(line)
                    if (batch_number != previous_batch_number):
                        chunked_files.append(file_name)
                    previous_batch_number = batch_number

        except IOError:
            print("Input file not found.")

        #create batches
        batches = []
        for file in chunked_files:
            with open(file, 'rb') as f:
                batch = self._add_batch_stream(job_id=job['id'], data=f, operation=operation)
                print('     Add batch {} from chunked file {}'.format(batch['id'], file))
                batches.append(batch['id'])

        #Close job
        self._close_job(job_id=job['id'])
        
        #Check batch
        remaining_batches = self._get_remaining_batches(job_id=batch['jobId'])
        
        finished_batches = []
        results = []
        while True:
            #Check the completed batches
            completed_batches = filter(lambda x: x['state'] == 'Completed' and x['id'] not in finished_batches, remaining_batches)
            #Generate report
            for batch in completed_batches:
                result = self._get_batch_results_path(job_id=batch['jobId'], batch_id=batch['id'], operation=operation)
                results.extend(result)
                finished_batches.append(batch['id'])
            
            #Check to see if there is any pending job
            if any(x['state'] not in ['Completed', 'Failed', 'Not Processed', 'NotProcessed'] for x in remaining_batches) and len(remaining_batches) > 0 :
                sleep(wait)
            else:
                break

            remaining_batches = self._get_remaining_batches(job_id=batch['jobId'])
        
        return results
        
    def _print_remaining_batches(self, batches):
        print("Remaining Batches: {}".format(len(batches)))
        for index, batch in enumerate(batches, start=1):
            print("    {} batchId: {}, jobId: {}, state: {}".format(index, batch['id'], batch['jobId'], batch['state']))

    # _bulk_operation wrappers to expose supported Salesforce bulk operations
    def delete(self, data):
        """ soft delete records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='delete', data=data)
        return results

    def insert(self, data):
        """ insert/create records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='insert', data=data)
        return results

    def upsert(self, data, external_id_field):
        """ upsert records based on a unique identifier """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='upsert',
                                       external_id_field=external_id_field,
                                       data=data)
        return results

    def update(self, data):
        """ update records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='update', data=data)
        return results

    def update_from_file(self, data_file_path):
        """ update records """
        results = self._bulk_operation_with_large_input_file(object_name=self.object_name,
                                       operation='update', data_file_path=data_file_path)
        return results

    def hard_delete(self, data):
        """ hard delete records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='hardDelete', data=data)
        return results

    def query(self, data):
        """ bulk query """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='query', data=data)
        return results
    
    def query_to_file(self, data, isPkChunk = False):
        """ bulk query to file """
        if isPkChunk:
            results = self._bulk_operation_to_file_pkchunk(object_name=self.object_name,
                                       operation='query', data=data)
        else:
            results = self._bulk_operation_to_file(object_name=self.object_name,
                                       operation='query', data=data)
        return results