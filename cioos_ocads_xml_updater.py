import os
from lxml import etree

def insert_cioos_online_resource_first(xml_file_path, dataset_id):
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(xml_file_path, parser)
    root = tree.getroot()
    nsmap = {
        "gmd": "http://www.isotc211.org/2005/gmd",
        "gco": "http://www.isotc211.org/2005/gco"
    }
    gmd = nsmap.get("gmd")
    gco = nsmap.get("gco")

    distribution_info = root.find(".//gmd:distributionInfo", namespaces=nsmap)
    if distribution_info is None:
        return False

    md_distribution = distribution_info.find(".//gmd:MD_Distribution", namespaces=nsmap)
    if md_distribution is None:
        return False

    digital_transfer = md_distribution.find(".//gmd:MD_DigitalTransferOptions", namespaces=nsmap)
    if digital_transfer is None:
        return False

    cioos_online = etree.Element(f"{{{gmd}}}onLine")
    online_resource = etree.SubElement(cioos_online, f"{{{gmd}}}CI_OnlineResource")

    linkage = etree.SubElement(online_resource, f"{{{gmd}}}linkage")
    url = etree.SubElement(linkage, f"{{{gmd}}}URL")
    url.text = f"https://data.cioospacific.ca/ocads/{dataset_id}/ftp_files/"

    protocol = etree.SubElement(online_resource, f"{{{gmd}}}protocol")
    protocol_text = etree.SubElement(protocol, f"{{{gco}}}CharacterString")
    protocol_text.text = "HTTPS"

    profile = etree.SubElement(online_resource, f"{{{gmd}}}applicationProfile")
    profile_text = etree.SubElement(profile, f"{{{gco}}}CharacterString")
    profile_text.text = "Web browser"

    name = etree.SubElement(online_resource, f"{{{gmd}}}name")
    name_text = etree.SubElement(name, f"{{{gco}}}CharacterString")
    name_text.text = "CIOOS Pacific OCADS Mirror"

    description = etree.SubElement(online_resource, f"{{{gmd}}}description")
    desc_text = etree.SubElement(description, f"{{{gco}}}CharacterString")
    desc_text.text = "CIOOS-hosted mirror of the original NOAA OCADS dataset."

    function = etree.SubElement(online_resource, f"{{{gmd}}}function")
    func_code = etree.SubElement(function, f"{{{gmd}}}CI_OnLineFunctionCode", codeListValue="download",
                                 codeList="http://www.isotc211.org/2005/resources/codeList.xml#CI_OnLineFunctionCode")
    func_code.text = "download"

    first_child = digital_transfer[0]
    digital_transfer.insert(digital_transfer.index(first_child), cioos_online)

    tree.write(xml_file_path, pretty_print=True, xml_declaration=True, encoding="UTF-8")
    return True

def process_all_datasets(base_dir="/CHANGE/ME/TO/DATASET/DIRECTORY"):
    for root, dirs, files in os.walk(base_dir):
        # Skip any directory that includes 'ftp_files' in the path
        if "ftp_files" in root:
            continue
        for file in files:
            if file.endswith(".xml"):
                xml_path = os.path.join(root, file)
                dataset_id = os.path.basename(os.path.dirname(xml_path))
                success = insert_cioos_online_resource_first(xml_path, dataset_id)
                if success:
                    print(f"Updated: {dataset_id}")
                else:
                    print(f"Skipped (structure issue): {dataset_id}")

if __name__ == "__main__":
    process_all_datasets("/CHANGE/ME/TO/DATASET/DIRECTORY")