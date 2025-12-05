import os
import requests
import ijson
import time
from urllib.parse import urlparse
from ftplib import FTP
from dotenv import load_dotenv

# Load environment variables from .env file
if not os.path.exists('.env'):
    raise FileNotFoundError('.env file not found. Please create a .env file by copying .env.example and configuring it.')
load_dotenv()

RATE_LIMIT_DELAY = 1
MAX_DELAY = 60
MAX_FTP_RETRIES = 5
MAX_HTTP_RETRIES = 5

def download_xml(xml_url, save_path):
    if os.path.exists(save_path):
        print("File already exists, skipping: {}".format(save_path))
        return
    
    headers = {"User-Agent": "CIOOS MirrorBot", "Accept": "application/xml"}
    delay = RATE_LIMIT_DELAY
    attempts = 0
    while attempts < MAX_HTTP_RETRIES:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            response = requests.get(xml_url, headers=headers)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as err:
            attempts += 1
            if response.status_code == 503 and attempts < MAX_HTTP_RETRIES:
                print("503 error for {}. Waiting {} seconds (attempt {}/{}).".format(xml_url, delay, attempts, MAX_HTTP_RETRIES))
                time.sleep(delay)
                delay = min(delay * 2, MAX_DELAY)
                continue
            else:
                raise err
        except Exception as err:
            attempts += 1
            if attempts < MAX_HTTP_RETRIES:
                print("Error downloading XML from {}: {}. Waiting {} seconds (attempt {}/{}).".format(xml_url, err, delay, attempts, MAX_HTTP_RETRIES))
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
            if os.path.exists(local_path):
                print("File already exists, skipping: {}".format(local_path))
                time.sleep(RATE_LIMIT_DELAY)
                continue
            
            with open(local_path, "wb") as f:
                delay_file = RATE_LIMIT_DELAY
                file_attempts = 0
                while file_attempts < MAX_FTP_RETRIES:
                    try:
                        ftp.retrbinary("RETR " + name, f.write)
                        break
                    except Exception as err:
                        file_attempts += 1
                        if file_attempts < MAX_FTP_RETRIES:
                            print("FTP file download error for {}: {}. Waiting {} seconds (attempt {}/{}).".format(name, err, delay_file, file_attempts, MAX_FTP_RETRIES))
                            time.sleep(delay_file)
                            delay_file = min(delay_file * 2, MAX_DELAY)
                        else:
                            print("Max retries reached for FTP file {}. Skipping.".format(name))
                            raise err
            print("Downloaded FTP file {} to {}".format(name, local_path))
            time.sleep(RATE_LIMIT_DELAY)

def download_ftp_directory(ftp_url, save_path):
    parsed_url = urlparse(ftp_url)
    delay = RATE_LIMIT_DELAY
    attempts = 0
    while attempts < MAX_FTP_RETRIES:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            ftp = FTP(parsed_url.hostname)
            ftp.login()
            ftp.cwd(parsed_url.path)
            break
        except Exception as err:
            attempts += 1
            print("FTP connection error for {}: {}. Attempt {}/{}. Waiting {} seconds.".format(ftp_url, err, attempts, MAX_FTP_RETRIES, delay))
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY)
    else:
        print("Max FTP connection attempts reached for {}. Skipping FTP download.".format(ftp_url))
        return
    download_ftp_tree(ftp, save_path)
    ftp.quit()
    print("FTP directory downloaded to {}".format(save_path))

def download_http_file(url, save_path):
    if os.path.exists(save_path):
        print("File already exists, skipping: {}".format(save_path))
        return
    
    headers = {"User-Agent": "CIOOS MirrorBot"}
    delay = RATE_LIMIT_DELAY
    attempts = 0
    while attempts < MAX_HTTP_RETRIES:
        time.sleep(RATE_LIMIT_DELAY)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as err:
            attempts += 1
            if response.status_code == 503 and attempts < MAX_HTTP_RETRIES:
                print("503 error for {}. Waiting {} seconds (attempt {}/{}).".format(url, delay, attempts, MAX_HTTP_RETRIES))
                time.sleep(delay)
                delay = min(delay * 2, MAX_DELAY)
                continue
            else:
                raise err
        except Exception as err:
            attempts += 1
            if attempts < MAX_HTTP_RETRIES:
                print("Error downloading HTTP file from {}: {}. Waiting {} seconds (attempt {}/{}).".format(url, err, delay, attempts, MAX_HTTP_RETRIES))
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
    base_dir = os.getenv("DATASETS_DIR", "datasets")
    os.makedirs(base_dir, exist_ok=True)
    failed_items = []
    failed_files = []
    
    with open(json_file, "r", encoding="utf-8") as f:
        for entry in ijson.items(f, "item"):
            entry_id = entry["id"]
            safe_entry_id = entry_id.replace(":", "_")
            entry_dir = os.path.join(base_dir, safe_entry_id)
            os.makedirs(entry_dir, exist_ok=True)
            
            try:
                links = entry.get("links", [])
                xml_link = next((l["href"] for l in links if l.get("type") == "application/xml"), None)
                if xml_link:
                    xml_path = os.path.join(entry_dir, "{}.xml".format(safe_entry_id))
                    print("Downloading XML: {}".format(xml_link))
                    try:
                        download_xml(xml_link, xml_path)
                    except Exception as err:
                        print("Failed to download XML for {}: {}".format(entry_id, err))
                        failed_files.append({"entry_id": entry_id, "type": "XML", "url": xml_link, "error": str(err)})
                else:
                    print("No XML link found for {}".format(entry_id))
                
                ftp_url = entry.get("url_ftp_download_s") or entry.get("_source", {}).get("url_ftp_download_s")
                if ftp_url:
                    ftp_dir = os.path.join(entry_dir, "ftp_files")
                    print("Downloading FTP directory: {}".format(ftp_url))
                    try:
                        download_ftp_directory(ftp_url, ftp_dir)
                    except Exception as err:
                        print("Failed to download FTP directory for {}: {}".format(entry_id, err))
                        failed_files.append({"entry_id": entry_id, "type": "FTP", "url": ftp_url, "error": str(err)})
                else:
                    http_url = entry.get("url_http_download_s") or entry.get("_source", {}).get("url_http_download_s")
                    if http_url:
                        file_path = os.path.join(entry_dir, os.path.basename(http_url))
                        print("No FTP URL for {}. Falling back to HTTP download: {}".format(entry_id, http_url))
                        try:
                            download_http_file(http_url, file_path)
                        except Exception as err:
                            print("Failed to download HTTP file for {}: {}".format(entry_id, err))
                            failed_files.append({"entry_id": entry_id, "type": "HTTP", "url": http_url, "error": str(err)})
                    else:
                        print("No FTP or HTTP URL found for {}".format(entry_id))
            except Exception as err:
                print("Failed to process dataset {}: {}".format(entry_id, err))
                failed_items.append({"entry_id": entry_id, "error": str(err)})
            
            time.sleep(RATE_LIMIT_DELAY)
    
    print("\n" + "="*80)
    print("Extraction complete.")
    if failed_items:
        print("\nFailed datasets ({}):\n".format(len(failed_items)))
        for item in failed_items:
            print("  - {}: {}".format(item["entry_id"], item["error"]))
    if failed_files:
        print("\nFailed files ({}):\n".format(len(failed_files)))
        for file_info in failed_files:
            print("  - {} ({}): {} - {}".format(file_info["entry_id"], file_info["type"], file_info["url"], file_info["error"]))
    print("="*80)

if __name__ == "__main__":
    results_filename = os.getenv("RESULTS_FILENAME", "ocads_results.json")
    extract_from_ocads_results(results_filename)