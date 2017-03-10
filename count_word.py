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
import web
import socket
import tarfile

urls = ('/', 'Index',
        '/upload', 'Upload',)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


class Index:
    """
    @GET method : return Web server Index"""
    def __init__(self):
        pass

    def GET(self):
        logging.info("Index Requested")
        return "Sample WSGI APP"


class Upload:
    """
        @POST method : return top 10 words"""
    def __init__(self):
        pass

    def POST(self):
        x = web.input(data={})
        try:

            file_name = x.data.filename
        except AttributeError:
            data = {'error':'Use keyword data=@filepath as parameter to specify file'}
            logging.error(data['error'])
            raise web.HTTPError('400 Bad request', {'Content-Type': 'application/json'}, data)

        file_ext = os.path.splitext(file_name)[1]

        if file_ext not in ('.zip', '.gz', '.tgz', '.bz2', '.tbz', '.tar'):
            data = {'error': 'File format not supported!!!'}
            raise web.HTTPError('400 Bad request', {'Content-Type': 'application/json'}, data)

        with open(x.data.filename,'wb') as saved:
            shutil.copyfileobj(x['data'].file, saved)

        cwd = os.path.dirname(__file__)
        path = os.path.join(cwd, file_name)
        logging.info("File Path:" + path)
        if os.path.exists(path):
            logging.info("File saved successfully")
            return main(path)
        else:
            logging.error("File not saved")
            headers = {'Content-Type': 'application/json'}
            data = {'error': "File not saved"}
            raise web.HTTPError('400 Bad request', headers, data)


class MyApp(web.application):
    """
     Override the default method of web.application to take specific Host address, Port
    """
    def run(self, server_address=('0.0.0.0', 8080), *middleware):
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, server_address)


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
                    if words is not None:
                        self.w.extend(words)
                    self.queue.task_done()


def main(path):

        # Create a queue to communicate with the worker threads
        ts = time()
        queue = Queue()
        words_list = []

        try:
            files = extract_zip(path)
        except Exception as e:
            logging.error(e.message)
            #raise e.message

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
                logging.info('Queueing {}'.format(os.path.basename(f)))
                queue.put(f)
        # Causes the main thread to wait for the queue to finish processing all the tasks
        queue.join()
        logging.info('Took {}'.format(time() - ts))
        dir_path = os.path.dirname(path)
        base_name = os.path.splitext(path)[0]
        extract_path = os.path.join(dir_path, base_name)

        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
            logging.info("Removed extracted temporary files to read!!!")

        if not words_list:
            return json.dumps({'msg': "No words available in the file present under uploaded zip"}, indent=1)
        # print words_list
        wordcount = {}
        for word in words_list:

            if wordcount.get(word, None):

                wordcount[word] += 1
            else:
                wordcount[word] = 1

        # print wordcount
        top_words = sorted(wordcount.items(), key=lambda item: item[1], reverse=True)
        # print top_words
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
            zip_ref.extractall(extract_path)

        if os.path.exists(extract_path):

            logging.info('Archive File extracted to process')
        else:
            raise "Failed: File archive extraction"

        file_paths = []
        for root, dirs, files in os.walk(extract_path):
            for f in files:

                file_paths.append(os.path.join(root, f))

        return file_paths


if __name__ == '__main__':

        app = MyApp(urls, globals())
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("gmail.com", 80))
        host_ip = s.getsockname()[0]
        s.close()
        logging.info("WSGI server started:" + str((host_ip, 50026)))
        app.run(('localhost', 50026))
