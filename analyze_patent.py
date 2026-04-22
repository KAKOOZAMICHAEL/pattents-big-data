from lxml import etree

def print_element_tree(element, level=0):
    """Recursively print the XML element tree with indentation."""
    indent = "  " * level
    attrs = ' ' + ' '.join(f'{k}="{v}"' for k, v in element.attrib.items()) if element.attrib else ''
    print(f"{indent}<{element.tag}{attrs}>")
    if element.text and element.text.strip():
        print(f"{indent}  {element.text.strip()}")
    for child in element:
        print_element_tree(child, level + 1)
    if element.tail and element.tail.strip():
        print(f"{indent}  {element.tail.strip()}")
    print(f"{indent}</{element.tag}>")

def extract_key_info(patent):
    """Extract and return key information from the patent."""
    info = {}
    
    # Patent ID
    doc_id = patent.find('.//document-id')
    if doc_id is not None:
        country = doc_id.find('country')
        doc_number = doc_id.find('doc-number')
        kind = doc_id.find('kind')
        date = doc_id.find('date')
        info['Patent ID'] = f"{country.text if country is not None else ''}{doc_number.text if doc_number is not None else ''} ({kind.text if kind is not None else ''}) - {date.text if date is not None else ''}"
    
    # Invention Title
    title = patent.find('.//invention-title')
    info['Invention Title'] = title.text if title is not None else 'N/A'
    
    # Filing Date
    app_ref = patent.find('.//application-reference')
    if app_ref is not None:
        app_doc_id = app_ref.find('document-id')
        if app_doc_id is not None:
            filing_date = app_doc_id.find('date')
            info['Filing Date'] = filing_date.text if filing_date is not None else 'N/A'
    
    # Inventors
    inventors = patent.findall('.//inventor')
    inventor_names = []
    for inv in inventors:
        name = inv.find('name')
        if name is not None:
            first = name.find('first-name')
            last = name.find('last-name')
            full_name = f"{first.text if first is not None else ''} {last.text if last is not None else ''}".strip()
            inventor_names.append(full_name)
    info['Inventors'] = ', '.join(inventor_names) if inventor_names else 'N/A'
    
    # Assignees
    assignees = patent.findall('.//assignee')
    assignee_names = []
    for ass in assignees:
        name = ass.find('name')
        if name is not None:
            assignee_names.append(name.text)
    info['Assignees'] = ', '.join(assignee_names) if assignee_names else 'N/A'
    
    return info

def main():
    # Load the XML file
    tree = etree.parse('sample_patents.xml')
    root = tree.getroot()
    
    # Get the first patent
    first_patent = root.find('us-patent-grant')
    if first_patent is None:
        print("No patent found in the file.")
        return
    
    # Extract and print key information
    key_info = extract_key_info(first_patent)
    print("=== Key Patent Information ===")
    for key, value in key_info.items():
        print(f"{key}: {value}")
    
    print("\n=== Full XML Structure ===")
    print_element_tree(first_patent)

if __name__ == "__main__":
    main()