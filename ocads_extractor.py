import os
import requests
import ijson
import time
from urllib.parse import urlparse
from ftplib import FTP

RATE_LIMIT_DELAY = 1
MAX_DELAY = 60

def download_xml(xml_url, save_path):
    headers = {"User-Agent": "CIOOS MirrorBot", "Accept": "application/xml"}
    delay = RATE_LIMIT_DELAY
    while True:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            response = requests.get(xml_url, headers=headers)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as err:
            if response.status_code == 503:
                print("503 error for {}. Waiting {} seconds.".format(xml_url, delay))
                time.sleep(delay)
                delay = min(delay * 2, MAX_DELAY)
                continue
            else:
                raise err
    with open(save_path, "wb") as f:
        f.write(response.content)
    print("XML downloaded to {}".format(save_path))
    time.sleep(RATE_LIMIT_DELAY)

def download_ftp_tree(ftp, local_dir):
    if os.path.exists(local_dir) and not os.path.isdir(local_dir):
        local_dir = local_dir + "_dir"
        print("Renaming target directory to {}".format(local_dir))
    os.makedirs(local_dir, exist_ok=True)
    try:
        entries = list(ftp.mlsd())
        use_mlsd = True
    except Exception:
        entries = [(name, {}) for name in ftp.nlst()]
        use_mlsd = False
    for name, facts in entries:
        if name in [".", ".."]:
            continue
        local_path = os.path.join(local_dir, name)
        entry_type = None
        if use_mlsd:
            entry_type = facts.get("type")
            if not entry_type:
                try:
                    ftp.cwd(name)
                    ftp.cwd("..")
                    entry_type = "dir"
                except Exception:
                    entry_type = "file"
        else:
            try:
                ftp.cwd(name)
                ftp.cwd("..")
                entry_type = "dir"
            except Exception:
                entry_type = "file"
        if entry_type == "dir":
            if os.path.exists(local_path) and not os.path.isdir(local_path):
                local_path = local_path + "_dir"
                print("Renaming directory target to {}".format(local_path))
            ftp.cwd(name)
            download_ftp_tree(ftp, local_path)
            ftp.cwd("..")
        else:
            with open(local_path, "wb") as f:
                delay_file = RATE_LIMIT_DELAY
                while True:
                    try:
                        ftp.retrbinary("RETR " + name, f.write)
                        break
                    except Exception as err:
                        print("FTP file download error for {}: {}. Waiting {} seconds.".format(name, err, delay_file))
                        time.sleep(delay_file)
                        delay_file = min(delay_file * 2, MAX_DELAY)
            print("Downloaded FTP file {} to {}".format(name, local_path))
            time.sleep(RATE_LIMIT_DELAY)

def download_ftp_directory(ftp_url, save_path):
    parsed_url = urlparse(ftp_url)
    delay = RATE_LIMIT_DELAY
    while True:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            ftp = FTP(parsed_url.hostname)
            ftp.login()
            ftp.cwd(parsed_url.path)
            break
        except Exception as err:
            print("FTP connection error for {}: {}. Waiting {} seconds.".format(ftp_url, err, delay))
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY)
    download_ftp_tree(ftp, save_path)
    ftp.quit()
    print("FTP directory downloaded to {}".format(save_path))

def download_http_file(url, save_path):
    headers = {"User-Agent": "CIOOS MirrorBot"}
    delay = RATE_LIMIT_DELAY
    while True:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as err:
            if response.status_code == 503:
                print("503 error for {}. Waiting {} seconds.".format(url, delay))
                time.sleep(delay)
                delay = min(delay * 2, MAX_DELAY)
                continue
            else:
                raise err
    with open(save_path, "wb") as f:
        f.write(response.content)
    print("HTTP file downloaded to {}".format(save_path))
    time.sleep(RATE_LIMIT_DELAY)

def extract_from_ocads_results(json_file):
    base_dir = "datasets"
    os.makedirs(base_dir, exist_ok=True)
    with open(json_file, "r", encoding="utf-8") as f:
        for entry in ijson.items(f, "item"):
            entry_id = entry["id"]
            safe_entry_id = entry_id.replace(":", "_")
            entry_dir = os.path.join(base_dir, safe_entry_id)
            if os.path.exists(entry_dir):
                print("Skipping previously downloaded dataset {}".format(entry_id))
                continue
            os.makedirs(entry_dir, exist_ok=True)
            links = entry.get("links", [])
            xml_link = next((l["href"] for l in links if l.get("type") == "application/xml"), None)
            if xml_link:
                xml_path = os.path.join(entry_dir, "{}.xml".format(safe_entry_id))
                print("Downloading XML: {}".format(xml_link))
                download_xml(xml_link, xml_path)
            else:
                print("No XML link found for {}".format(entry_id))
            ftp_url = entry.get("url_ftp_download_s") or entry.get("_source", {}).get("url_ftp_download_s")
            if ftp_url:
                ftp_dir = os.path.join(entry_dir, "ftp_files")
                print("Downloading FTP directory: {}".format(ftp_url))
                download_ftp_directory(ftp_url, ftp_dir)
            else:
                http_url = entry.get("url_http_download_s") or entry.get("_source", {}).get("url_http_download_s")
                if http_url:
                    file_path = os.path.join(entry_dir, os.path.basename(http_url))
                    print("No FTP URL for {}. Falling back to HTTP download: {}".format(entry_id, http_url))
                    download_http_file(http_url, file_path)
                else:
                    print("No FTP or HTTP URL found for {}".format(entry_id))
            time.sleep(RATE_LIMIT_DELAY)
    print("Extraction complete.")

if __name__ == "__main__":
    extract_from_ocads_results("ocads_results.json")
