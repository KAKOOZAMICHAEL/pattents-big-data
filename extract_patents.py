from lxml import etree
import re

def extract_first_100_patents(input_file, output_file):
    """
    Extracts the first 100 complete <us-patent-grant> elements from a large XML file
    and writes them to a new well-formed XML file.
    Handles concatenated XML documents by parsing them individually.
    """
    patents = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        current_patent_lines = []
        in_patent = False
        
        for line in f:
            if '<?xml version="1.0"' in line:
                if current_patent_lines:
                    # Save the previous patent
                    patent_content = ''.join(current_patent_lines)
                    patents.append(patent_content)
                    current_patent_lines = []
                in_patent = True
                continue  # Skip the XML declaration line
            
            if in_patent:
                current_patent_lines.append(line)
            
            if '</us-patent-grant>' in line:
                # End of patent
                patent_content = ''.join(current_patent_lines)
                patents.append(patent_content)
                current_patent_lines = []
                in_patent = False
                if len(patents) >= 100:
                    break
    
    # Now, process the first 100 patents
    root = etree.Element("patents")
    for patent_str in patents[:100]:
        # Remove DOCTYPE if present
        patent_str = re.sub(r'<!DOCTYPE[^>]*>', '', patent_str, flags=re.DOTALL)
        # Parse the patent XML
        patent_elem = etree.fromstring(patent_str.strip())
        root.append(patent_elem)
    
    # Write to output file with XML declaration
    tree = etree.ElementTree(root)
    tree.write(output_file, encoding='utf-8', xml_declaration=True, pretty_print=True)

if __name__ == "__main__":
    input_file = 'ipg230103.xml'  # Note: Corrected from 'ipg23103.xml' to match the actual file
    output_file = 'sample_patents.xml'
    extract_first_100_patents(input_file, output_file)
    print(f"Extracted first 100 patents to {output_file}")