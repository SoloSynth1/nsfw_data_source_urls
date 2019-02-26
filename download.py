import threading
from os import path, listdir
import urllib3
import argparse


data_path = path.abspath("./raw_data")


class threadsafe_iter:
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def next(self):
        with self.lock:
            return next(self.it)


def threadsafe_generator(f):
    """A decorator that takes a generator function and makes it thread-safe.
    """
    def g(*a, **kw):
        return threadsafe_iter(f(*a, **kw))
    return g


def locate_url_txt(parent_path, url_files=None):
    files = [path.join(parent_path, file) for file in listdir(parent_path)]
    if not url_files:
        url_files = []
    for file in files:
        if path.isdir(file):
            url_files = locate_url_txt(file, url_files)
        elif path.basename(file) == "urls.txt":
            print("found {}".format(file))
            url_files.append(file)
    return url_files


@threadsafe_generator
def url_generator(url_files):
    for file_path in url_files:
        with open(file_path, 'r') as f:
            urls = f.read().split('\n')
            valid_urls = [(url, get_file_name(url, file_path)) for url in urls if url and not path.isfile(get_file_name(url,file_path))]
            while valid_urls:
                yield valid_urls.pop(0)


def download_urls(valid_urls, thread_count=100):
    threads = [threading.Thread(target=download_manager, args=(valid_urls,)) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def download_manager(url_generator):
    while True:
        try:
            url, file_name = url_generator.next()
            download(url, file_name)
        except StopIteration:
            break


def download(url, file_name, timeout=10.0, retries_max=1):
    print("{}  ==>  {}...".format(url, file_name))
    retries = 0
    while retries <= retries_max:
        try:
            response = urllib3.PoolManager().request('GET', url, timeout=timeout)
            if response.status < 400:
                write(response.data, file_name)
                break
            retries += 1
        except Exception as e:
            print("error: {}".format(e))
            retries += 1
            continue



def get_file_name(url, file_path):
    return path.join(path.dirname(path.realpath(file_path)), url.split('?')[0].split('/')[-1])


def write(file_binary, file_name):
    with open(file_name, 'wb') as f:
        f.write(file_binary)


def parse_arguments():
    parser = argparse.ArgumentParser(description='NSFW Data Source Downloader')
    parser.add_argument('-n', '--thread-counts', help='Download thread counts', type=int, required=False)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    urllib3.disable_warnings()
    args = parse_arguments()
    url_files = locate_url_txt(data_path)
    url_gen = url_generator(url_files)
    if args.thread_counts and args.thread_counts > 0:
        download_urls(url_gen, args.thread_counts)
    else:
        download_urls(url_gen)
    print("process finished")
