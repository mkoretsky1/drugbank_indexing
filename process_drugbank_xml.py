import os

# XML scrubbing helper function
def extract_tag_complex(text, starttag, endtag):
    text_start = text[text.find("<"+starttag+">"):]
    return text_start[:text_start.find("</"+endtag+">") + len(endtag)+3]

def process_drugbank_xml(drugbank_xml, out_dir):
    """Split and reformat DrugBank XML for indexing with Elastic."""
    # split on drug type tag
    split_drugbank_xml = drugbank_xml.split('<drug type')
    print(len(split_drugbank_xml))

    # first item is the overall xml header so we can drop that
    split_drugbank_xml = split_drugbank_xml[1:]
    print(len(split_drugbank_xml))

    # loop over list
    for i in range(len(split_drugbank_xml)):
        # add beginning of drug type tag back in
        split_drugbank_xml[i] = '<drug type' + split_drugbank_xml[i]

        # extract the name tag and clean up
        drug_name = extract_tag_complex(split_drugbank_xml[i], 'name', 'name')
        drug_name = drug_name[6:]
        drug_name = drug_name[:-7]
        drug_name = drug_name.replace(' ','_')
        drug_name = drug_name.replace('/','_')

        # write to clean file
        with open(f'{out_dir}/{drug_name}.xml', 'w') as f:
            f.write(split_drugbank_xml[i])
            f.close()

if __name__ == '__main__':
    # paths
    xml_file_path = f'drugbank/db_fulldb324.xml'

    # make output file dir
    out_dir = f'drugbank/clean'
    os.makedirs(f'{out_dir}', exist_ok=True)

    # read xml
    with open(xml_file_path, 'r') as f:
        drugbank_xml = f.read()
        f.close()

    # process xml
    process_drugbank_xml(drugbank_xml=drugbank_xml, out_dir=out_dir)