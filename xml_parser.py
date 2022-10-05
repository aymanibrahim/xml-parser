import requests
import logging
import re
from pathlib import Path
import zipfile
import csv
from tqdm import tqdm
from lxml import etree

# setup logging
logging.basicConfig(level=logging.DEBUG)

# download the XML file
def download_xml(download_link, download_dir, file_name=""):
    """Download the XML file
    :param download_link
    :param download_dir
    :return file_path
    """
    
    pass

# parse the xml file
def parse_xml(file_path, file_type):
    """Parse through to the first download link whose file_type is DLTINS
    and download the zip
    :param file_path
    :param file_type
    :return zip file
    """
    pass


# extract the xml from the zip
def get_xml_file(xml_zip):
    """Extract the xml from the zip
    :param xml_zip
    :retrun xml file
    """
    pass


# extract xml contents
def extract_xml_contents(xml_file):
    """"Extract XML contents from XML file
    :param xml_file
    :return xml_contents
    """
    # create a custom XML parser 
    parser = etree.XMLParser(recover=True) 
    
    # create tree
    logging.info(f"Creating tree for the XML document from {xml_file}")
    tree = etree.parse(xml_file, parser) 
    
    # keep log of wellformedness errors
    parser.error_log
    
    # get root
    root = tree.getroot()
    
    # find namespace
    # for child in root[1]:
    #     print(child.tag)
    # {urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Document
    
    # define namespace prefix mapping to perform XPath queries on namespaced elements
    namespaces = {'Document': 'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02'} 
    
    # search for FinInstrmGnlAttrbts elements in the Document namespace
    # find all entries with a child element of <FinInstrmGnlAttrbts> 
    logging.info("Searching for required XML elements")
    entries = tree.xpath("//Document:FinInstrmGnlAttrbts/..", namespaces=namespaces)
    
    # calculate number of elements
    num_entries = len(entries)
    
    # get entr_text
    # entries[0].xpath('./Document:FinInstrmGnlAttrbts/*/text()', namespaces=namespaces)
    # 'DE000A1R07V3',
    # 'Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021)',
    # 'KFW/1.625 ANL 20210115 GGAR',
    # 'DBFTFB',
    # 'EUR',
    # 'false']
    
    # inilialize rows
    rows = list()
    
    # process entries
    logging.info(f"Processing XML {num_entries} entries")
    for entry in tqdm(entries):
        entry_text = entry.xpath('./Document:FinInstrmGnlAttrbts/*/text()', namespaces=namespaces)
        id_element = entry_text[0]
        name_element = entry_text[1]
        class_element = entry_text[3]
        is_derived_element = entry_text[5]
        curr_element = entry_text[4]
        issr_element = entry.xpath('./Document:Issr/text()', namespaces=namespaces)[0]        
        rows.append((id_element,
                     name_element,
                     class_element,
                     is_derived_element,
                     curr_element,
                     issr_element))
    return rows


# convert the contents of the xml into a CSV
def convert_contents(xml_contents, headers, csv_file_path):
    """Convert the contents of the xml into a CSV
    :param xml_contents
    :param headers
    :param csv_file_path
    :return CSV file path
    """
    
    # write csv file
    logging.info(f"Creating CSV file at {csv_file_path}")
    with open(csv_file_path, 'w', newline='') as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(headers)
        for row in tqdm(xml_contents):
                writer.writerow(row)
    
    return csv_file_path

# store the csv in an AWS S3 bucket
def store_csv_s3(csv_file, s3_bucket):
    """"Store the csv in an AWS S3 bucket
    :param csv_file
    :param s3_bucket
    retruns AWS S3 bucket location
    """
    pass


if __name__ == '__main__':
    download_link = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    download_dir = "."
    file_path = download_xml(download_link, download_dir)
    
    file_type = "DLTINS"

    xml_zip = parse_xml(file_path, file_type)
    # xml_zip => "DLTINS_20210117_01of01.zip"
    
    xml_file = get_xml_file(xml_zip)
    # xml_file => DLTINS_20210117_01of01.xml
    
    xml_contents = extract_xml_contents(xml_file)

    # specify the headers for csv file
    headers = ('FinInstrmGnlAttrbts.Id',
            'FinInstrmGnlAttrbts.FullNm',
            'FinInstrmGnlAttrbts.ClssfctnTp',
            'FinInstrmGnlAttrbts.CmmdtyDerivInd',
            'FinInstrmGnlAttrbts.NtnlCcy',
            'Issr')
    csv_file = convert_contents(xml_contents, headers, csv_file_path)
    # csv_file => contents.csv


    store_csv_s3(csv_file, s3_bucket)
    # csv_s3 => s3//s3_bucket/contents.csv
