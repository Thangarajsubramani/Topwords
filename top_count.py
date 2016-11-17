import json
import re
import collections
from threading import *
from Queue import Queue
import logging
from time import time
import zipfile
import os
import shutil
import socket
import tarfile
from bottle import route, request, static_file, run,response

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


@route('/')
def root():
    """
        @GET HTTP method : return Web server Index"""
    return "Sample WSGI APP"


@route('/upload', method='POST')
def do_upload():

    """
    @POST HTTP method: return top10 words"""
    data = request.files.keys()[0]
    upload = request.files.get(data)
    name, file_ext = os.path.splitext(upload.filename)
    if file_ext not in ('.zip', '.gz', '.tgz', '.bz2', '.tbz', '.tar'):
        response.status = 400
        response.content_type = 'application/json'
        data = {'error': 'file format not supported'}
        logger.error(data['error'])
        return json.dumps(data)
    cwd = os.path.dirname(__file__)
    save_path = cwd

    file_path = "{path}/{file}".format(path=save_path, file=upload.filename)
    logger.info(file_path)

    try:
        upload.save(file_path)
    except IOError as e:
        logging.error(e)

    if os.path.exists(file_path):
        logging.info("File successfully saved to '{0}'.".format(save_path))
        return main(file_path)
    else:
        logging.error("File not saved")
        response.status = 400
        response.content_type = 'application/json'
        data = {'error': "File not saved"}
        return json.dumps(data)


def parse_file(filename):
    # @param: filename
    # parse the text file and extract the words
    words_list = []
    with open(filename, 'r') as f_handle:
        line = f_handle.read()
        p = re.compile('\w+', re.M)
        # lists = p.findall('drummers0?drumming#11\npipers$piping,y10&sorry lords-aleaping000 sorry 1')
        words_list.extend(p.findall(line.lower()))
    if words_list:
        return words_list
    else:
        return None


class ParseWorker(Thread):
    def __init__(self, queue, comp_words_list=None):
        Thread.__init__(self)
        self.queue = queue
        self.w = comp_words_list

    def run(self):

            while True:
                    # Get the work from the queue
                    file_name = self.queue.get()
                    logging.info('%s : Parses %s', currentThread().getName(), os.path.basename(file_name))
                    words = parse_file(file_name)
                    wordcount = {}
                    if words is not None:
                        for word in words:
                            if wordcount.get(word, None):
                                wordcount[word] += 1
                            else:
                                wordcount[word] = 1
                        self.w.extend(wordcount.items())
                    self.queue.task_done()


def main(path):

        # Create a queue to communicate with the worker threads
        ts = time()
        queue = Queue()
        words_list = []
        try:
            files = extract_zip(path)
        except Exception as e:
            logging.error(e)
            raise Exception(e)
        if not files:
            logging.warning("Does not contains any files to process:")
            return json.dumps({'msg': "Uploaded Archive does not contains any file to process"}, indent=1)

        # Create N=3 worker threads
        for x in range(3):
            worker = ParseWorker(queue, words_list)
            # Setting daemon to True will let the main thread exit even though the workers are blocking
            worker.daemon = True
            worker.start()
        # Put the tasks into the queue
        for f in files:
                logger.info('Queueing {}'.format(os.path.basename(f)))
                queue.put(f)
        # Causes the main thread to wait for the queue to finish processing all the tasks
        queue.join()
        logging.info('Took {}'.format(time() - ts))
        dir_path = os.path.dirname(path)
        base_name = os.path.splitext(path)[0]
        extract_path = os.path.join(dir_path, base_name)
        if os.path.exists(extract_path):
            try:

                shutil.rmtree(extract_path)
                logging.info("Removed extracted temporary files to read!!!")
            except Exception, e:
                logging.error(e)
        if os.path.exists(path):
            os.remove(path)
            logging.info("Removed the uploaded file")
        if not words_list:
            return json.dumps({'msg': "No words available in the file present under uploaded zip"}, indent=1)
        # print words_list
        wordcount = {}
        for word, count in words_list:
            if wordcount.get(word, None):
                wordcount[word] += count
            else:
                wordcount[word] = count
        top_words = sorted(wordcount.items(), key=lambda item: item[1], reverse=True)
        # response.content_type = 'application/json'
        return json.dumps({'Top_words': collections.OrderedDict(top_words[:10])}, indent=1,separators=(',', ': '))


def extract_zip(path_zip):
        # @param: Archive path
        if not zipfile.is_zipfile(path_zip) and not tarfile.is_tarfile(path_zip):
            raise "Please upload correct zip file"
        dir_path = os.path.dirname(path_zip)
        base_name = os.path.splitext(path_zip)[0]
        ext = os.path.splitext(path_zip)[1]

        if path_zip.endswith('.zip'):
            opener, mode = zipfile.ZipFile, 'r'
        elif path_zip.endswith('.tar'):
            opener, mode = tarfile.open, 'r:'
        elif path_zip.endswith('.tar.gz') or path_zip.endswith('.tgz'):
            opener, mode = tarfile.open, 'r:gz'
        elif path_zip.endswith('.tar.bz2') or path_zip.endswith('.tbz'):
            opener, mode = tarfile.open, 'r:bz2'
        else:
            raise ValueError, "Could not extract `%s` as no appropriate extractor is found" % ext
        extract_path = os.path.join(dir_path, base_name)
        with opener(path_zip, mode) as zip_ref:
            # namelist = zip_ref.namelist()
            if not os.path.exists(extract_path):
                zip_ref.extractall(extract_path)
            elif os.path.exists(extract_path):
                logging.info('Archive File extracted to process')
            else:
                raise "Failed: File archive extraction"

        file_paths = []
        for root, dirs, files in os.walk(extract_path):
            for f in files:
                file_paths.append(os.path.join(root, f))
        return file_paths

if __name__ == '__main__':
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("netapp.com", 80))
        host_ip = s.getsockname()[0]
        s.close()
    except Exception as e:
        logging.error(e)
        host_ip = socket.gethostbyname(socket.gethostname())
    run(host=host_ip, port=8089)