import threading
from os import path, listdir
import requests

data_path = path.abspath("./raw_data")


def locate_url_txt(parent_path):
    files = [path.join(parent_path, file) for file in listdir(parent_path)]
    for file in files:
        if path.isdir(file):
            locate_url_txt(file)
        elif path.basename(file) == "urls.txt":
            print("found {}".format(file))
            download_urls(file)


def download_urls(file_path, thread_count=50):
    with open(file_path, 'r') as f:
        urls = f.read().split('\n')
    valid_urls = [(url, get_file_name(url, file_path)) for url in urls if url and not path.isfile(get_file_name(url, file_path))]
    url_count = len(valid_urls)
    threads = [threading.Thread(target=download_manager, args=(valid_urls,)) for _ in range(min(thread_count, url_count))]
    for thread in threads:
        thread.start()


def download_manager(urls_pool):
    while urls_pool:
        url, file_name = urls_pool.pop(0)
        download(url, file_name)


def download(url, file_name, timeout=5, retries_max=3):
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
    locate_url_txt(data_path)
