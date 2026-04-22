import json
from pathlib import Path

from lxml import etree


BASE_DIR = Path(__file__).resolve().parent


def element_text(element):
    """Return normalized text content for an element."""
    if element is None:
        return None

    text = " ".join(part.strip() for part in element.itertext() if part and part.strip())
    return text or None


def normalize_value(value, default="N/A"):
    """Normalize blank values into a stable default."""
    if value is None:
        return default

    normalized = " ".join(str(value).split())
    return normalized if normalized else default


def build_person_name(addressbook):
    """Build a display name from an addressbook block."""
    if addressbook is None:
        return None

    orgname = element_text(addressbook.find("orgname"))
    if orgname:
        return orgname

    first_name = element_text(addressbook.find("first-name"))
    middle_name = element_text(addressbook.find("middle-name"))
    last_name = element_text(addressbook.find("last-name"))
    parts = [part for part in [first_name, middle_name, last_name] if part]
    return " ".join(parts) if parts else None


def extract_patent_data(patent):
    """Extract normalized fields from a single patent element."""
    publication_reference = patent.find("./us-bibliographic-data-grant/publication-reference/document-id")
    application_reference = patent.find("./us-bibliographic-data-grant/application-reference/document-id")

    abstract_text = element_text(patent.find(".//abstract"))
    first_claim_text = element_text(patent.find(".//claims/claim/claim-text"))

    inventors = []
    for inventor in patent.findall(".//inventors/inventor"):
        addressbook = inventor.find("addressbook")
        full_name = normalize_value(build_person_name(addressbook), default="Unknown Inventor")
        country = normalize_value(
            element_text(addressbook.find("address/country")) if addressbook is not None else None,
            default="Unknown",
        )
        inventors.append({"full_name": full_name, "country": country})

    assignees = []
    assignee_nodes = patent.findall(".//assignees/assignee")
    if not assignee_nodes:
        assignee_nodes = patent.findall(".//us-applicants/us-applicant")

    for assignee in assignee_nodes:
        addressbook = assignee.find("addressbook")
        company_name = normalize_value(build_person_name(addressbook), default="Unknown Company")
        assignees.append({"company_name": company_name})

    return {
        "patent_id": normalize_value(
            element_text(publication_reference.find("doc-number")) if publication_reference is not None else None
        ),
        "title": normalize_value(element_text(patent.find(".//invention-title"))),
        "description": normalize_value(abstract_text or first_claim_text),
        "filing_date": normalize_value(
            element_text(application_reference.find("date")) if application_reference is not None else None
        ),
        "publication_date": normalize_value(
            element_text(publication_reference.find("date")) if publication_reference is not None else None
        ),
        "main_classification": normalize_value(
            element_text(patent.find(".//classification-national/main-classification"))
        ),
        "locarno_classification": normalize_value(
            element_text(patent.find(".//classification-locarno/main-classification"))
        ),
        "inventors": inventors,
        "assignees": assignees,
    }


def extract_patents(xml_path):
    """Load an XML file and extract all patent records."""
    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    patents = root.findall(".//us-patent-grant")
    if not patents and root.tag == "us-patent-grant":
        patents = [root]

    return [extract_patent_data(patent) for patent in patents]


def main(xml_path=None, output_path=None):
    """Extract patent data into a JSON intermediate file."""
    xml_path = Path(xml_path) if xml_path else BASE_DIR / "sample_patents.xml"
    output_path = Path(output_path) if output_path else BASE_DIR / "patents_data.json"

    patents_data = extract_patents(xml_path)
    print(f"Extracted data for {len(patents_data)} patents.")

    with output_path.open("w", encoding="utf-8") as file_handle:
        json.dump(patents_data, file_handle, indent=4, ensure_ascii=False)

    print(f"Data saved to {output_path}")
    if patents_data:
        print("\nSample patent data:")
        print(json.dumps(patents_data[0], indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()