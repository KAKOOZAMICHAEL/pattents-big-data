import xml.etree.ElementTree as ET
from pathlib import Path
import csv
import gzip

def extract_patents(xml_path: Path, output_csv: Path):
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_csv = str(output_csv)
    
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["doc_number", "title", "country", "date_publ", "applicants", "inventors", "classifications"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        context = ET.iterparse(str(xml_path), events=("start", "end"))
        context = iter(context)
        event, root = next(context)
        
        for event, elem in context:
            if event == "end" and elem.tag == "us-patent-grant":
                patent = extract_patent_data(elem)
                if patent:
                    writer.writerow(patent)
                root.clear()
                elem.clear()
    
    print(f"Extracted patents to {output_csv}")

def extract_patent_data(elem):
    try:
        bib = elem.find("us-bibliographic-data-grant")
        if not bib:
            return None
        
        doc_id = bib.find("publication-reference/document-id")
        doc_number = doc_id.findtext("doc-number", "") if doc_id else ""
        date_publ = doc_id.findtext("date", "") if doc_id else ""
        title = bib.findtext("invention-title", "")
        country = doc_id.findtext("country", "") if doc_id else ""
        
        applicants = [p.findtext("name") for p in bib.findall(".//us-parties[@role='applicant']/us-party/name")]
        inventors = [p.findtext("name") for p in bib.findall(".//us-parties[@role='inventor']/us-party/name")]
        classes = [c.findtext("main-classification") for c in bib.findall("classification-national")]
        
        return {
            "doc_number": doc_number,
            "title": title,
            "country": country,
            "date_publ": date_publ,
            "applicants": "; ".join(applicants),
            "inventors": "; ".join(inventors),
            "classifications": "; ".join(classes)
        }
    except:
        return None