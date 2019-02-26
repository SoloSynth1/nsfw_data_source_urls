import threading
from os import path, listdir
import requests

data_path = path.abspath("./raw_data")


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


def generate_download_list(url_files):
    valid_urls = []
    for file_path in url_files:
        with open(file_path, 'r') as f:
            urls = f.read().split('\n')
            valid_urls.append([(url, get_file_name(url, file_path)) for url in urls if url and not path.isfile(get_file_name(url,file_path))])
            print("")
    print("URL list generation complete, {} files".format(len(valid_urls)))
    return valid_urls


def download_urls(valid_urls, thread_count=500):
    url_count = len(valid_urls)
    threads = [threading.Thread(target=download_manager, args=(valid_urls,)) for _ in range(min(thread_count, url_count))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def download_manager(urls_pool):
    while urls_pool:
        url, file_name = urls_pool.pop(0)
        download(url, file_name)


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
    valid_urls = generate_download_list(url_files)
    download_urls(valid_urls)
    print("process finished")
