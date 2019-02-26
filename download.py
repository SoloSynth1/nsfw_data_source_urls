import threading
from os import path, listdir
import requests

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
            return self.it.next()


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


def download_urls(valid_urls, thread_count=500):
    threads = [threading.Thread(target=download_manager, args=(valid_urls,)) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def download_manager(url_generator):
    while True:
        try:
            url, file_name = next(url_generator)
            download(url, file_name)
        except StopIteration:
            break


def download(url, file_name, timeout=10, retries_max=1):
    print("{}  ==>  {}...".format(url, file_name))
    retries = 0
    while retries <= retries_max:
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code < 400:
                write(response.content, file_name)
                break
        except:
            continue
        finally:
            retries += 1


def get_file_name(url, file_path):
    return path.join(path.dirname(path.realpath(file_path)), url.split('?')[0].split('/')[-1])


def write(file_binary, file_name):
    with open(file_name, 'wb') as f:
        f.write(file_binary)


if __name__ == "__main__":
    url_files = locate_url_txt(data_path)
    url_gen = url_generator(url_files)
    download_urls(url_gen)
    print("process finished")
