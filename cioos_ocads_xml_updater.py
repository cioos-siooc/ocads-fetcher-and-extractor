import sys
import yaml
from lxml import etree
from collections import defaultdict
from metadata_xml.metadata_xml.template_functions import metadata_to_xml

namespaces = {
    "gmi": "http://www.isotc211.org/2005/gmi",
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
    "gml": "http://www.opengis.net/gml",
    "xlink": "http://www.w3.org/1999/xlink"
}

def extract_text(el):
    return el.text.strip() if el is not None and el.text else None

def extract_bbox(root):
    bbox = root.find(".//gmd:identificationInfo/*/gmd:extent//gmd:EX_GeographicBoundingBox", namespaces)
    return [
        float(extract_text(bbox.find("gmd:westBoundLongitude/gco:Decimal", namespaces))),
        float(extract_text(bbox.find("gmd:southBoundLatitude/gco:Decimal", namespaces))),
        float(extract_text(bbox.find("gmd:eastBoundLongitude/gco:Decimal", namespaces))),
        float(extract_text(bbox.find("gmd:northBoundLatitude/gco:Decimal", namespaces)))
    ] if bbox is not None else None

def extract_keywords(root):
    keywords = root.findall(".//gmd:descriptiveKeywords/gmd:MD_Keywords", namespaces)
    result = defaultdict(list)
    for kw in keywords:
        title_el = kw.find(".//gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString", namespaces)
        title = extract_text(title_el).lower() if title_el is not None else "default"
        keyword_els = kw.findall(".//gmd:keyword/gco:CharacterString", namespaces)
        words = [extract_text(k) for k in keyword_els if extract_text(k)]
        if "eov" in title:
            category = "eov"
        elif "taxa" in title or "taxonomic" in title:
            category = "taxa"
        else:
            category = "default"
        result[category].extend(words)
    return {k: sorted(set(v)) for k, v in result.items()}

def extract_contacts(root):
    contacts = []
    for c in root.findall(".//gmd:contact/gmd:CI_ResponsibleParty", namespaces):
        role = extract_text(c.find(".//gmd:role/gmd:CI_RoleCode", namespaces))
        org_name = extract_text(c.find(".//gmd:organisationName/gco:CharacterString", namespaces))
        email = extract_text(c.find(".//gmd:contactInfo//gmd:address//gmd:electronicMailAddress/gco:CharacterString", namespaces))
        contact = {
            "roles": [role] if role else [],
            "organization": {"name": org_name, "email": email}
        }
        contacts.append(contact)
    return contacts

def parse_xml_to_record(xml_path):
    tree = etree.parse(xml_path)
    root = tree.getroot()

    identifier = extract_text(root.find(".//gmd:fileIdentifier/gco:CharacterString", namespaces))
    language = extract_text(root.find(".//gmd:language/gmd:LanguageCode", namespaces))
    title = extract_text(root.find(".//gmd:identificationInfo/*/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString", namespaces))
    abstract = extract_text(root.find(".//gmd:identificationInfo/*/gmd:abstract/gco:CharacterString", namespaces))

    record = {
        "metadata": {
            "naming_authority": "ca.cioos",
            "identifier": identifier,
            "language": "en",
            "maintenance_note": "auto-generated",
            "use_constraints": {
                "limitations": {
                    "en": "limitations in english",
                    "fr": "limitations in french",
                    "translations": {"fr": {"validated": False, "message": "Auto-translated using AWS"}}
                },
                "licence": {
                    "title": "Creative Commons Attribution 4.0",
                    "code": "CC-BY-4.0",
                    "url": "https://creativecommons.org/licenses/by/4.0/"
                }
            },
            "comment": {
                "en": "auto-generated comment",
                "fr": "auto-generated comment",
            },
            "dates": {
                "creation": "2024-01-01"
            },
            "scope": "model"
        },
        "spatial": {
            "bbox": extract_bbox(root),
            "polygon": "polygon_data",
            "description": "description of the spatial extent or study area of the dataset",
            "descriptionIdentifier": "A2345-FS323-DG434-345DG",
            "vertical": [0, 10],
            "vertical_positive": "down"
        },
        "identification": {
            "title": {
                "en": title,
                "fr": f"{title} (FR)",
                "translations": {"fr": {"validated": False, "message": "Auto-translated using AWS"}}
            },
            "abstract": {
                "en": abstract,
                "fr": f"{abstract} (FR)",
                "translations": {"fr": {"validated": False, "message": "Auto-translated using AWS"}}
            },
            "keywords": extract_keywords(root),
            "temporal_begin": "1950-07-31",
            "temporal_end": "now",
            "temporal_duration": "P1D",
            "time_coverage_resolution": "P1D",
            "acknowledgement": "acknowledgement",
            "status": "onGoing",
            "project": {
                "en": ["project_a", "project_b"],
                "fr": ["project_a in french", "project_b in french"]
            },
        },
        "contact": extract_contacts(root),
        "distribution": [
            {"url": u} for u in root.xpath(".//gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL/text()", namespaces=namespaces)
        ]
    }
    return record

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py input.xml output.yaml")
        sys.exit(1)

    input_xml = sys.argv[1]
    output_yaml = sys.argv[2]

    record = parse_xml_to_record(input_xml)
    from lxml import etree

    xml = metadata_to_xml(record)

    # Save YAML
    with open(output_yaml, "w") as f:
        yaml.dump(record, f, allow_unicode=True, sort_keys=False)

    # Save XML
    output_xml = output_yaml.replace(".yaml", ".xml")
    with open(output_xml, "w", encoding="utf-8") as f:
        f.write(xml)

    print(f"YAML metadata written to {output_yaml}")
    print(f"ISO XML metadata written to {output_xml}")
