from pathlib import Path
import shutil

def create_sample(input_xml: Path, output_xml: Path, num_patents: int = 100):
    output_xml.parent.mkdir(parents=True, exist_ok=True)
    
    with open(input_xml, "r", encoding="utf-8") as fin, open(output_xml, "w", encoding="utf-8") as fout:
        # Write XML header if exists
        fout.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        fout.write('<root>\n')
        
        count = 0
        in_patent = False
        for line in fin:
            # Skip the XML declaration
            if line.startswith('<?xml'):
                continue
            
            if "<us-patent-grant" in line and not in_patent:
                if count >= num_patents:
                    break
                in_patent = True
                fout.write(line)
                count += 1
            elif in_patent:
                fout.write(line)
                if "</us-patent-grant>" in line:
                    in_patent = False
        
        fout.write('</root>\n')
    
    print(f"Created sample with {count} patents: {output_xml}")