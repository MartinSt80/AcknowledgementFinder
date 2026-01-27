import logging
logger = logging.getLogger(__name__)

import shutil
import tempfile
import time

from typing import Tuple, List
from pathvalidate import sanitize_filename
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

from FilePathsUrls import Urls, FilePaths


class PublicationLog:

    log_headers = ['citation_file_name',
                   'Pubmed Id',
                   'PMC Id',
                   'DOI',
                   'pmc_xml',
                   'kops_pdf',
                   'doi_html',
                   'fac_acknowledged',
                   'fac_suspected',
                   ]

    def __init__(self) -> None:

        self.citation_file_name = ''

        self.pmid = ''
        self.doi = ''
        self.pmcid = ''

        self.is_xml_fulltext = False
        self.is_pdf_fulltext = False
        self.is_html_fulltext = False


    # get the PubmedId from the retrieved Pubmed citation entry
    def _get_pmid_doi(self, citation_file_path:Path) -> None:

        # read the pubmed entry's xml-tree and identify the PubMed-ID
        with citation_file_path.open(mode='rb') as pub_entry_file:
            content = pub_entry_file.read()
        pubentry_xml_root = ET.fromstring(content)
        self.pmid = pubentry_xml_root.find('DocSum').find('Id').text

        # in the items identify a doi entry
        item_list = pubentry_xml_root.find('DocSum').findall('Item')
        for item in item_list:
            if item.get('Name') == 'DOI':
                self.doi = item.text
                break


    # Get current PMCID if available
    # Query the NCBI PubmedId to PMC converter, since a PMCID is not always assigned when the pubmed entry is generated
    def _get_pmcid(self) -> None:

        # For testing, known working PubmedID, to have a PMC entry
        # converter_response = requests.get(self.id_converter_url + '28729661')

        converter_response = requests.get(Urls.pubmed_to_pmc_id_converter_url + self.pmid)

        if not converter_response.ok:
            logger.info(
                f'{Path(self.citation_file_name).stem} with the PubmedID {self.pmid} the API responded with an error code of {converter_response.status_code}.')
            return

        xml_root = ET.fromstring(converter_response.content)
        for record in xml_root.iter('record'):
            if not (record.get('status') == 'error'):
                logger.info(f'{Path(self.citation_file_name).stem} with the PubmedID {self.pmid} has the PMC entry {record.get('pmcid')}.')
                self.pmcid = record.get('pmcid')
                return

            logger.info(
                f'{Path(self.citation_file_name).stem} with the PubmedID {self.pmid} has no PMC entry.')
        return


    # Initialize from a citation file
    def initialize_from_citation(self, citation_path:Path) -> None:
        self.citation_file_name = citation_path.name
        self._get_pmid_doi(citation_path)
        self._get_pmcid()


    # Initialize from log
    def initialize_from_log_entry(self, pub_info_dict:dict) -> None:
        self.citation_file_name = pub_info_dict[self.log_headers[0]]
        self.pmid = pub_info_dict[self.log_headers[1]]
        self.pmcid = pub_info_dict[self.log_headers[2]]
        self.doi = pub_info_dict[self.log_headers[3]]

        self.is_xml_fulltext = pub_info_dict[self.log_headers[4]]
        self.is_pdf_fulltext = pub_info_dict[self.log_headers[5]]
        self.is_html_fulltext = pub_info_dict[self.log_headers[6]]


    # Compound the necessary information into a csv-string to write to the log.
    def list_publication_info(self) -> List[str]:
        # citation_file_name, Pubmed Id, PMC Id, DOI, as_xml, as_pdf, as_html, fac_acknowledged, fac_suspected
        data = [self.citation_file_name,
                self.pmid,
                self.pmcid,
                self.doi,
                str(self.is_xml_fulltext),
                str(self.is_pdf_fulltext),
                str(self.is_html_fulltext),
                'False',
                'False',
                ]

        return data

    @property
    def has_fulltext(self):
        return self.is_xml_fulltext or self.is_pdf_fulltext or self.is_html_fulltext


class QueryPmcAws(object):

    # Set up a boto3 resource to allow access without authorization
    boto3_resource = boto3.resource('s3', region_name='us-east-1')
    boto3_resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    pmc_bucket = boto3_resource.Bucket('pmc-oa-opendata')

    # Try to download the fulltext as xml from PMC
    def download_fulltext_from_pmc_aws(self, pmid: str, pmc_id:str) -> bool:

        xml_fulltext_download_path = FilePaths.xml_dir_fullpath / f'{pmc_id}_aws.xml'

        pmc_key_prefixes = ['ao_comm', 'oa_noncomm', 'author_manuscript']

        for prefix in pmc_key_prefixes:
            try:
                self.pmc_bucket.download_file(f'{prefix}/xml/all/{pmc_id}.xml', xml_fulltext_download_path)
                logger.info(f'Pubmed Id {pmid}: {prefix}/xml/all/{pmc_id}.xml was found and saved to {xml_fulltext_download_path}')
                return True
            except Exception as e:
                logger.info(msg=f'Pubmed Id {pmid}: {prefix}/xml/all/{pmc_id}.xml could not be retrieved: {e}')

        else:
            logger.info(f'Pubmed Id {pmid}: No PMC xml fulltext available for {pmc_id} through the PMC aws cloud.')
            return False


class QueryPmcOaiPmh(object):

    @staticmethod
    def download_fulltext_from_pmc_pmh(pmid:str, pmc_id:str) -> bool:

        xml_fulltext_download_path = FilePaths.xml_dir_fullpath / f'{pmid}_{pmc_id}_pmh.xml'

        payload = {'verb': 'GetRecord',
                   'identifier': 'oai:pubmedcentral.nih.gov:' + pmc_id[3:],
                   'metadataPrefix': 'pmc',
                   }

        pmc_oai_pmh_response = requests.get(Urls.pmc_pmh_base_url, params=payload)
        if pmc_oai_pmh_response.ok:
            with open(xml_fulltext_download_path, 'wb') as xml_file:
                xml_file.write(pmc_oai_pmh_response.content)
            logger.info(f'Pubmed Id {pmid}: A xml fulltext for the PMC-ID {pmc_id} was found using the PMC OAI-PMH API and saved to {xml_fulltext_download_path}')
            return True
        else:
            logger.info(f'Pubmed Id {pmid}: No PMC xml fulltext available for {pmc_id} through the PMC OAI-PMH API.')
            return False


class QueryPmcFtp(object):

    request_header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
    }

    def __init__(self):
        self.ftp_connection = FTP(Urls.pmc_ftp_url)
        self.ftp_connection.login()

    # Close the ftp session
    def __del__(self):
       self.ftp_connection.quit()

    # Get the ftp link to download the fulltext pdf or tar.gz
    def _get_ftp_link(self, pmid:str, pmc_id:str) -> Tuple[str, str]|Tuple[None, None]:

        req_payload = {'id': pmc_id}

        pmc_info_response = requests.get(Urls.oa_webservice_api_url,
                                         params=req_payload,
                                         headers= self.request_header)

        if not pmc_info_response.ok:
            logger.info(f'Pubmed Id {pmid}: Response of PMC API when retrieving record info for PMC ID {pmc_id}: {pmc_info_response.status_code}')
            return None, None
        else:
            xml_tree = ET.fromstring(pmc_info_response.text)
            records_entry = xml_tree.find("records")
            if int(records_entry.attrib['total-count']) > 0:
                for pmc_record in records_entry.findall("record"):
                    link = pmc_record.find('link')
                    if link.attrib['format'] == 'tgz':
                        return link.attrib['format'], link.attrib['href']

        return None, None

    def download_pmc_fulltext_from_ftp(self, pmid:str, pmc_id:str) -> bool:

        xml_fulltext_download_path = FilePaths.xml_dir_fullpath / f'{pmc_id}_ftp'

        ftp_format, ftp_link = self._get_ftp_link(pmid, pmc_id)
        ftp_file_path = urlparse(ftp_link).path

        if ftp_link:
            if ftp_format == 'pdf':
                with open(xml_fulltext_download_path.with_suffix('pdf'), 'wb') as pdf_file:
                    self.ftp_connection.retrbinary(f'RETR {ftp_file_path}', pdf_file.write)

                logger.info(
                    f'Pubmed Id {pmid}: A pdf fulltext for the PMC-ID {pmc_id} was found on the PMC ftp server and saved to {xml_fulltext_download_path}')
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
                    f'Pubmed Id {pmid}: A xml fulltext for the PMC-ID {pmc_id} was found on the PMC ftp server and saved to {xml_fulltext_download_path}')
                return True

        logger.info(f'Pubmed Id {pmid}: No PMC xml or pdf fulltext available for {pmc_id} on the PMC ftp server.')
        return False


class QueryKopsPdf(object):

    def __init__(self):
        # instantiate a Chrome options object
        options = webdriver.ChromeOptions()
        # set the options to use Chrome in headless mode
        options.add_argument("--headless=new")
        # initialize an instance of the chrome driver (browser)
        self.driver = webdriver.Chrome(options=options)

    def __del__(self):
        self.driver.quit()

    def download_fulltext_pdf_from_kops(self, pubmed_id:str) -> bool:

        pdf_download_fullpath = FilePaths.pdf_dir_fullpath / f'{pubmed_id}_kops.pdf'

        # request the search page
        self.driver.get(Urls.kops_search_base_url + pubmed_id)

        # wait for the page to finish loading
        WebDriverWait(self.driver, 10).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

        # Wait another second, as a loading delay of elements has been set (.X seconds)
        time.sleep(1)

        try:
            # Identify the link to the publication_page and load it
            link_to_publication_page = ''
            for element in self.driver.find_elements(By.CLASS_NAME, 'dont-break-out'):
                if element.get_attribute('href'):
                    link_to_publication_page = element.get_attribute('href')
                    break

            if link_to_publication_page:
                self.driver.get(link_to_publication_page)

                # Identify the link to the publication_page and load it
                for element in self.driver.find_elements(By.TAG_NAME, 'link'):
                    link_url = element.get_attribute("href")
                    if link_url.startswith(Urls.kops_download_base_url):
                        response = requests.get(link_url)
                        with open(pdf_download_fullpath, 'wb') as pdf_file:
                            pdf_file.write(response.content)
                        logger.info(f'PubMed ID {pubmed_id} was found in KOPS and saved to {pdf_download_fullpath}.')
                        return True

            logger.info(f'PubMed ID {pubmed_id} has no KOPS entry.')
            return False

        except Exception as e:
            logger.info(f'Getting PubMed ID {pubmed_id} from KOPS raised the exception: {repr(e)}')
            return False


class QueryDoiHtml(object):

    def __init__(self):
        # instantiate a Chrome options object
        options = webdriver.ChromeOptions()
        # set the options to use Chrome in headless mode
        options.add_argument("--headless=new")
        # initialize an instance of the chrome driver (browser)
        self.driver = webdriver.Chrome(options=options)

    def __del__(self):
        self.driver.quit()

    def download_fulltext_html_via_doi(self, pmid:str, doi:str) -> bool:

        html_download_fullpath = FilePaths.html_dir_fullpath / f'{sanitize_filename(doi)}.html'

        # request the search page
        self.driver.get(Urls.doi_base_url + doi)

        # wait for the page to finish loading
        WebDriverWait(self.driver, 10).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

        # Wait another second, as a loading delay of elements may have been set
        time.sleep(1)

        # Store the webpage
        with open(html_download_fullpath, 'w') as html_file:
            html_file.write(str(self.driver.page_source.encode('utf-8')))

        logger.info(f'Pubmed Id {pmid}: Html page for DOI {doi} was saved to {html_download_fullpath}.')
        return True


class QueryShadowPdf(object):

    def __init__(self):
        # instantiate a Chrome options object
        options = webdriver.ChromeOptions()
        # set the options to use Chrome in headless mode
        options.add_argument("--headless=new")
        # initialize an instance of the chrome driver (browser)
        self.driver = webdriver.Chrome(options=options)

    def __del__(self):
        self.driver.quit()

    def download_fulltext_pdf_from_shadow(self, pubmed_id:str, doi:str) -> bool:

        pdf_download_fullpath = FilePaths.pdf_dir_fullpath / f'{pubmed_id}_shadow.pdf'

        # request the search page
        self.driver.get(Urls.shadow_lib_base_url + doi)

        # wait for the page to finish loading
        WebDriverWait(self.driver, 10).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
            )

        # Wait another second, as a loading delay of elements has been set (.X seconds)
        time.sleep(1)

        for element in self.driver.find_elements(By.TAG_NAME, 'embed'):
            if element.get_attribute('type') == 'application/pdf':
                shadow_download_link = element.get_attribute('src')
                download_response = requests.get(shadow_download_link)
                if download_response.status_code == 200:
                    with open(pdf_download_fullpath, 'wb') as pdf_file:
                        pdf_file.write(download_response.content)
                        logger.info(
                            f'PubMed ID {pubmed_id} was found in shadow library and saved to {pdf_download_fullpath}.')
                        return True
                logger.info(f'PubMed ID {pubmed_id} has no shadow lib entry.')
                return False

        else:
            print(f'Pubmed Id {pubmed_id}: No download link found for DOI {doi}')
            return False


if __name__ == '__main__':

    # aws_query = QueryPmcAws()
    # aws_query.download_fulltext_from_pmc_aws('PMC7118124')
    #
    # ftp_query = QueryPmcFtp()
    # ftp_query.download_pmc_fulltext_from_ftp('PMC7118124')
    #
    # pmh_query = QueryPmcOaiPmh()
    # pmh_query.download_fulltext_from_pmc_pmh('PMC7118124')
    #
    # kops_query = QueryKopsPdf()
    # kops_query.download_fulltext_pdf_from_kops('32072999')
    #
    # html_query = QueryDoiHtml()
    # html_query.download_fulltext_html_via_doi('10.3390/biom10060951')
    #
    # shadow_query = QueryShadowPdf()
    # shadow_query.download_fulltext_pdf_from_shadow('32275434',
    #                                                '10.1021/acs.jpca.0c01844',
    #                                                )
    exit()


