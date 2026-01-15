import logging
import shutil
import tempfile
import time
from typing import Tuple


logger = logging.getLogger(__name__)

import sys
from ftplib import FTP
from pathlib import Path
from urllib.parse import urlparse
from lxml import etree as ET
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By

import requests
import boto3
from botocore.handlers import disable_signing


class QueryPmcAws(object):

    # Set up a boto3 resource to allow access without authorization
    boto3_resource = boto3.resource('s3', region_name='us-east-1')
    boto3_resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    pmc_bucket = boto3_resource.Bucket('pmc-oa-opendata')

    # Try to download the fulltext as xml from PMC
    def download_fulltext_from_pmc_aws(self, pmc_download_dir:Path, pmc_id:str) -> bool:

        xml_fulltext_download_path = pmc_download_dir / f'{pmc_id}_aws.xml'

        pmc_key_prefixes = ['ao_comm', 'oa_noncomm', 'author_manuscript']

        for prefix in pmc_key_prefixes:
            try:
                self.pmc_bucket.download_file(f'{prefix}/xml/all/{pmc_id}.xml', xml_fulltext_download_path)
                logger.info(f'{prefix}/xml/all/{pmc_id}.xml was found and saved to {xml_fulltext_download_path}')
                return True
            except Exception as e:
                logger.log(msg=f'{prefix}/xml/all/{pmc_id}.xml not found', level=5)

        else:
            logger.info(f'No PMC xml fulltext available for {pmc_id} through the PMC aws cloud.')
            return False


class QueryPmcOaiPmh(object):

    base_url = 'https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/'

    def download_fulltext_from_pmc_pmh(self, pmc_download_dir:Path, pmc_id:str) -> bool:

        xml_fulltext_download_path = pmc_download_dir / f'{pmc_id}_pmh.xml'

        payload = {'verb': 'GetRecord',
                   'identifier': 'oai:pubmedcentral.nih.gov:' + pmc_id[3:],
                   'metadataPrefix': 'pmc',
                   }

        pmc_oai_pmh_response = requests.get(self.base_url, params=payload)
        if pmc_oai_pmh_response.status_code == 200:
            with open(xml_fulltext_download_path, 'wb') as xml_file:
                xml_file.write(pmc_oai_pmh_response.content)
            logger.info(f'A xml fulltext for the PMC-ID {pmc_id} was found using the PMC OAI-PMH API and saved to {xml_fulltext_download_path}')
            return True
        else:
            logger.info(f'No PMC xml fulltext available for {pmc_id} through the PMC OAI-PMH API.')
            return False


class QueryPmcFtp(object):

    base_ftp_url = 'ftp.ncbi.nlm.nih.gov'

    # API to retrieve ftp links for given PMCIDs
    oa_webservice_api_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi'
    request_header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
    }

    def __init__(self):
        self.ftp_connection = FTP(self.base_ftp_url)
        self.ftp_connection.login()


    # Close the ftp session
    def __del__(self):
       self.ftp_connection.quit()

    # Get the ftp link to download the fulltext pdf or tar.gz
    def _get_ftp_link(self, pmc_id:str) -> Tuple[str, str]|Tuple[None, None]:

        req_payload = {'id': pmc_id}

        pmc_info_response = requests.get(self.oa_webservice_api_url,
                                         params=req_payload,
                                         headers= self.request_header)

        if int(pmc_info_response.status_code) != 200:
            logger.info(f'Response of PMC API when retrieving record info for PMC ID {pmc_id}: {pmc_info_response.status_code}')
            return None, None
        else:
            xml_tree = ET.fromstring(pmc_info_response.text)
            records_entry = xml_tree.find("records")
            if int(records_entry.attrib['total-count']) > 0:
                for pmc_record in records_entry.findall("record"):
                    link = pmc_record.find('link')
                    if link.attrib['format'] == 'tgz':
                        break

                return link.attrib['format'], link.attrib['href']
            return None, None

    def download_pmc_fulltext_from_ftp(self, pmc_download_dir:Path, pmc_id:str) -> bool:

        xml_fulltext_download_path = pmc_download_dir / f'{pmc_id}_ftp'

        ftp_format, ftp_link = self._get_ftp_link(pmc_id)
        ftp_file_path = urlparse(ftp_link).path

        if ftp_link:
            if ftp_format == 'pdf':
                with open(xml_fulltext_download_path.with_suffix('pdf'), 'wb') as pdf_file:
                    self.ftp_connection.retrbinary(f'RETR {ftp_file_path}', pdf_file.write)

                logger.info(
                    f'A pdf fulltext for the PMC-ID {pmc_id} was found on the PMC ftp server and saved to {xml_fulltext_download_path}')
                return True

            if ftp_format == 'tgz':
                with tempfile.TemporaryDirectory() as temp_dir:
                    extracted_path = Path(temp_dir) / pmc_id
                    tgz_file_path = Path(temp_dir) / f'{pmc_id}.tgz'
                    with open(tgz_file_path, 'wb') as tgz_file:
                        self.ftp_connection.retrbinary(f'RETR {ftp_file_path}', tgz_file.write)
                    shutil.unpack_archive(tgz_file_path, temp_dir)
                    for file in extracted_path.glob('*.nxml'):
                        shutil.copy(file, xml_fulltext_download_path.with_name(f'{pmc_id}_ftp.xml'))

                logger.info(
                    f'A xml fulltext for the PMC-ID {pmc_id} was found on the PMC ftp server and saved to {xml_fulltext_download_path}')
                return True

        logger.info(f'No PMC xml or pdf fulltext available for {pmc_id} on the PMC ftp server.')
        return False


class QueryKopsPdf(object):

    kops_search_url = 'https://kops.uni-konstanz.de/search?spc.page=1&view=list&query='
    kops_download_url = 'https://kops.uni-konstanz.de/bitstreams/'

    def __init__(self):
        # instantiate a Chrome options object
        options = webdriver.ChromeOptions()
        # set the options to use Chrome in headless mode
        options.add_argument("--headless=new")
        # initialize an instance of the chrome driver (browser)
        self.driver = webdriver.Chrome(options=options)

    def __del__(self):
        self.driver.quit()

    def download_fulltext_pdf_from_kops(self, kops_download_dir:Path, pubmed_id:str) -> bool:

        # request the serach page
        self.driver.get(self.kops_search_url + pubmed_id)

        # wait for the page to finish loading
        WebDriverWait(self.driver, 10).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

        # Wait another second, as a laoding delay of elements has been set (.X seconds)
        time.sleep(1)

        # Identify the link to the publication_page and load it
        link_to_publication_page = ''
        for element in self.driver.find_elements(By.CLASS_NAME, 'dont-break-out'):
            if element.get_attribute('href'):
                link_to_publication_page = element.get_attribute('href')
                break

        if link_to_publication_page:
            self.driver.get(link_to_publication_page)

            # Find the link to the pdf file and store it
            for element in self.driver.find_elements(By.TAG_NAME, 'link'):
                link_url = element.get_attribute("href")
                if link_url.startswith(self.kops_download_url):
                    response = requests.get(link_url)
                    with open(kops_download_dir / f'{pubmed_id}_kops.pdf', 'wb') as pdf_file:
                        pdf_file.write(response.content)
                    logger.info(f'PubMed ID {pubmed_id} was found in KOPS and saved to {kops_download_dir / f'{pubmed_id}_kops.pdf'}.')
                    return True

        print(logger.info(f'PubMed ID {pubmed_id} has no KOPS entry.'))
        return False

if __name__ == '__main__':


    publication_dir = Path('D:/PubTracker/test_pubs/2020')
    pmc_xmlfulltext_subdirectory = 'pmc_xml_fulltexts'
    pdf_fulltext_subdirectory = 'pdf_fulltexts'

    # ftp_query = QueryPmcFtp()
    #
    # ftp_query.download_pmc_fulltext_from_ftp(publication_dir / pmc_xmlfulltext_subdirectory, 'PMC7118124')

    # pmc_request = QueryPmcAws()

    # pmc_request.get_ftp_links()

    kops_query = QueryKopsPdf()
    kops_query.download_fulltext_pdf_from_kops(publication_dir / pdf_fulltext_subdirectory, '32072999')