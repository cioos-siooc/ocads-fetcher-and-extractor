import os
import requests
import ijson
import time
from urllib.parse import urlparse
from ftplib import FTP

def download_xml(xml_url, save_path):
    headers = {"User-Agent": "CIOOS MirrorBot", "Accept": "application/xml"}
    delay = 1
    max_delay = 60
    while True:
        try:
            response = requests.get(xml_url, headers=headers)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as err:
            if response.status_code == 503:
                print(f"503 error for {xml_url}. Waiting {delay} seconds.")
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
                continue
            else:
                raise err
    with open(save_path, "wb") as f:
        f.write(response.content)
    print(f"XML downloaded to {save_path}")
    time.sleep(1)

def download_ftp_tree(ftp, local_dir):
    if os.path.exists(local_dir) and not os.path.isdir(local_dir):
        new_local_dir = local_dir + "_dir"
        print(f"Conflict: {local_dir} exists as file; renaming directory target to {new_local_dir}")
        local_dir = new_local_dir
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
        # Determine entry type
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
                new_local_path = local_path + "_dir"
                print(f"Conflict: {local_path} exists as file; renaming directory target to {new_local_path}")
                local_path = new_local_path
            ftp.cwd(name)
            download_ftp_tree(ftp, local_path)
            ftp.cwd("..")
        else:
            with open(local_path, "wb") as f:
                delay_file = 1
                max_delay = 60
                while True:
                    try:
                        ftp.retrbinary("RETR " + name, f.write)
                        break
                    except Exception as err:
                        print(f"FTP file download error for {name}: {err}. Waiting {delay_file} seconds.")
                        time.sleep(delay_file)
                        delay_file = min(delay_file * 2, max_delay)
            print(f"Downloaded FTP file {name} to {local_path}")

def download_ftp_directory(ftp_url, save_path):
    parsed_url = urlparse(ftp_url)
    delay = 1
    max_delay = 60
    while True:
        try:
            ftp = FTP(parsed_url.hostname)
            ftp.login()
            ftp.cwd(parsed_url.path)
            break
        except Exception as err:
            print(f"FTP connection error for {ftp_url}: {err}. Waiting {delay} seconds.")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    download_ftp_tree(ftp, save_path)
    ftp.quit()
    print(f"FTP directory downloaded to {save_path}")

def download_http_file(url, save_path):
    headers = {"User-Agent": "CIOOS MirrorBot"}
    delay = 1
    max_delay = 60
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as err:
            if response.status_code == 503:
                print(f"503 error for {url}. Waiting {delay} seconds.")
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
                continue
            else:
                raise err
    with open(save_path, "wb") as f:
        f.write(response.content)
    print(f"HTTP file downloaded to {save_path}")
    time.sleep(1)

def extract_from_ocads_results(json_file):
    base_dir = "datasets"
    os.makedirs(base_dir, exist_ok=True)
    with open(json_file, "r", encoding="utf-8") as f:
        for entry in ijson.items(f, "item"):
            entry_id = entry["id"]
            entry_dir = os.path.join(base_dir, entry_id.replace(":", "_"))
            os.makedirs(entry_dir, exist_ok=True)
            links = entry.get("links", [])
            xml_link = next((l["href"] for l in links if l.get("type") == "application/xml"), None)
            if xml_link:
                xml_path = os.path.join(entry_dir, f"{entry_id.replace(':', '_')}.xml")
                print(f"Downloading XML: {xml_link}")
                download_xml(xml_link, xml_path)
            else:
                print(f"No XML link found for {entry_id}")
            ftp_url = entry.get("url_ftp_download_s") or entry.get("_source", {}).get("url_ftp_download_s")
            if ftp_url:
                ftp_dir = os.path.join(entry_dir, "ftp_files")
                print(f"Downloading FTP directory: {ftp_url}")
                download_ftp_directory(ftp_url, ftp_dir)
            else:
                http_url = entry.get("url_http_download_s") or entry.get("_source", {}).get("url_http_download_s")
                if http_url:
                    file_path = os.path.join(entry_dir, os.path.basename(http_url))
                    print(f"No FTP URL for {entry_id}. Falling back to HTTP download: {http_url}")
                    download_http_file(http_url, file_path)
                else:
                    print(f"No FTP or HTTP URL found for {entry_id}")
            time.sleep(1)
    print("Extraction complete.")

if __name__ == "__main__":
    extract_from_ocads_results("ocads_results.json")
