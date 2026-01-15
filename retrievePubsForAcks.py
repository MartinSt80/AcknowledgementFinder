import sys
from logging import Logger

from typing import Tuple
import logging

from lxml import etree as ET
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from PublicationDownloadServices import QueryPmcAws, QueryPmcOaiPmh, QueryPmcFtp, QueryKopsPdf


class PublicationLog:

	id_converter_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids='

	def __init__(self, path:Path) -> None:
		self.path = path
		self.file_name = path.name
		self.pmid, self.doi = self._get_pmid_doi()
		self.pmcid = self._get_pmcid()
		self.is_pmc_xml_fulltext = False
		self.is_kops_pdf_fulltext = None
		self.is_doi_pdf_fulltext = None

	# get the PubmedId from the retrieved Pubmed citation entry
	def _get_pmid_doi(self) -> Tuple[str, str]:

		doi = None

		# read the pubmed entry's xml-tree and identify the PubMed-ID
		with self.path.open(mode='rb') as pub_entry_file:
			content = pub_entry_file.read()
		pubentry_xml_root = ET.fromstring(content)
		pubmedid = pubentry_xml_root.find('DocSum').find('Id').text

		# in the items identify a doi entry
		item_list = pubentry_xml_root.find('DocSum').findall('Item')
		for item in item_list:
			if item.get('Name') == 'DOI':
				doi = item.text
				break
		return pubmedid, doi

	# Get current PMCID if available
	# Query the NCBI PubmedId to PMC converter, since a PMCID is not always assigned when the pubmed entry is generated
	def _get_pmcid(self) -> str|None:

		# For testing, known working PubmedID, to have a PMC entry
		# converter_response = requests.get(self.id_converter_url + '28729661')

		converter_response = requests.get(self.id_converter_url + self.pmid)
		if converter_response.status_code == 200:
			xml_root = ET.fromstring(converter_response.content)
			for record in xml_root.iter('record'):
				if not (record.get('status') == 'error'):
					root_logger.info(f'{Path(self.file_name).stem} with the PubmedID {self.pmid} has the PMC entry {record.get('pmcid')}.')
					return record.get('pmcid')

				root_logger.info(
					f'{Path(self.file_name).stem} with the PubmedID {self.pmid} has no PMC entry.')
			return None
		root_logger.info(
			f'{Path(self.file_name).stem} with the PubmedID {self.pmid} the API responded with an error code of {converter_response.status_code}.')
		return None


	# Get the pdf from by its doi, might run into a paywall
	def retrievePDFbyDOI(self):
		if self.doi:
			try:
				doi_page_content = requests.get('https://doi.org/' + self.doi)
				soup = BeautifulSoup(doi_page_content.text, "lxml")
				for meta_element in soup.find_all('meta'):
					try:
						if meta_element['name'] == 'citation_pdf_url':
							# Try to get the pdf data, do not follow redirects (Often runs into a paywall)
							pdf_file_response = requests.get(meta_element['content'], allow_redirects=False)
							if pdf_file_response.status_code == 200:
								pdf_file_data = pdf_file_response.content
							else:
								print(f"Getting the pdf for: {self.doi} failed: {pdf_file_response.status_code}")
								pdf_file_data = None
							break
					except KeyError:
						pass
				else:
					print(f'No pdf dowload url found for {self.doi}')
					pdf_file_data = None
				self.is_doi_pdf_fulltext = True if pdf_file_data else False
				return pdf_file_data
			except Exception as e:
				print(f"Getting the pdf for: {self.doi} failed: {str(e)}")

	# Create a message to track the progress
	def log_message(self):
		if self.is_pmc_xml_fulltext:
			message_string = (f'{self.file_name}: has been downloaded as a xml fulltext from PMC.')
		elif self.is_kops_pdf_fulltext:
			message_string = (f'{self.file_name}: has been downloaded as a pdf fulltext from KOPS.')
		elif self.is_doi_pdf_fulltext:
			message_string = (f'{self.file_name}: has been downloaded as a pdf from {DOI_url + self.doi}.')
		else:
			message_string = (f'{self.file_name}: No fulltext could be downloaded.')
		return message_string

	# Compound the necessary information into a csv-string to write to the log.
	def log_csv_string(self):
		# file_name, as_xml, as_pdf, fac_acknowledged, fac_suspected\n'
		data = [self.file_name, str(self.is_pmc_xml_fulltext), str(self.is_doi_pdf_fulltext or self.is_kops_pdf_fulltext), 'None', 'None\n']
		return ','.join(data)


def configure_logger(dl_log_file_path:Path) -> Logger:
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=logging.INFO,
						handlers=[logging.FileHandler(dl_log_file_path),
								  logging.StreamHandler(sys.stdout),
								  ]
						)
	return logger

def create_directories_and_files(pub_dir:Path, log_subdir:str, dl_load_file_name:str, pub_log_file_name:str, pmc_xml_subdir:str) -> None:

	# Create, reset the logfiles
	if not (pub_dir / log_subdir).is_dir():
		(pub_dir / log_subdir).mkdir()

	with (pub_dir / log_subdir / pub_log_file_name).open(mode='w') as log_file:
		log_file.write('file_name,as_xml,as_pdf,fac_acknowledged,fac_suspected\n')

	with (pub_dir / log_subdir / dl_load_file_name).open(mode='w') as log_file:
		log_file.write('# Download results for the publications\n')

	# Make sure the fulltext directories exist
	if not (pub_dir / pmc_xml_subdir).is_dir():
		(pub_dir / pmc_xml_subdir).mkdir()


if __name__ == '__main__':

	# Necessary URLs
	# pmc_xmlfulltext_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id='

	KOPS_url = 'https://kops.uni-konstanz.de'
	DOI_url = 'https://doi.org/'

	# Directories and log files
	publication_dir = Path('D:/PubTracker/test_pubs/2020')
	log_subdirectory = 'logs'
	pmc_xmlfulltext_subdirectory = 'pmc_xml_fulltexts'
	pdf_fulltext_subdirectory = 'pdf_fulltexts'

	publication_log_file_name = 'publications_log.csv'
	download_log_file_name = 'download_log.txt'

	# Setup logging
	root_logger = configure_logger(publication_dir / log_subdirectory / download_log_file_name)

	# Create directories and logs
	create_directories_and_files(publication_dir,
								 log_subdirectory,
								 download_log_file_name,
								 publication_log_file_name,
								 pmc_xmlfulltext_subdirectory
								 )

	# Setup download interfaces, if needed
	# pmc_request = QueryPmcAws()
	# ftp_request = QueryPmcFtp()
	kops_request = QueryKopsPdf()

	# Use only the pubmed entries in xml format
	for pub_entry in publication_dir.glob('*.txt'):

		# create the Publication object
		current_publication = PublicationLog(pub_entry)

		# # If the paper is available in PMC AWS cloud as xml, get the XML, not needed, is subset of pmh
		# if current_publication.pmcid:
		# 		current_publication.is_pmc_xml_fulltext = pmc_request.download_fulltext_from_pmc_aws(publication_dir / pmc_xmlfulltext_subdirectory,
		# 																							 current_publication.pmcid)

		# else try to get it from the PMC AOI PMH API
		if current_publication.pmcid :
			current_publication.is_pmc_xml_fulltext = QueryPmcOaiPmh().download_fulltext_from_pmc_pmh(
				publication_dir / pmc_xmlfulltext_subdirectory,
				current_publication.pmcid)

		# # else try to get it from the PMC ftp server, not needed, can be used as backup
		# if current_publication.pmcid:
		# 	current_publication.is_pmc_xml_fulltext = ftp_request.download_pmc_fulltext_from_ftp(publication_dir / pmc_xmlfulltext_subdirectory,
		# 		current_publication.pmcid)

		# If we didn't get the fulltext by PMC, try to download the pdf from KOPS
		if not current_publication.is_pmc_xml_fulltext:
			current_publication.is_kops_pdf_fulltext = kops_request.download_fulltext_pdf_from_kops(publication_dir / pdf_fulltext_subdirectory,
				current_publication.pmid)

		#
		# # If neither of the above methods worked, try to get pdf link by from 'https://doi.org/'
		# if not (current_publication.is_pmc_xml_fulltext or current_publication.is_kops_pdf_fulltext):
		# 	pdf_stream = current_publication.retrievePDFbyDOI()
		# 	if pdf_stream:
		# 		with pub_entry.with_suffix('.pdf').open(mode='wb') as pdf_fulltext_file:
		# 			pdf_fulltext_file.write(pdf_stream)

		# Output a status message
		# print(current_publication.log_message())
		#
		# # Write the result to the log_files
		# with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='ab') as log_file:
		# 	log_file.write(current_publication.log_csv_string().encode('utf8'))
		#
		# with (pub_dir / log_subdirectory / download_log_file_name).open(mode='ab') as log_file:
		# 	log_file.write((current_publication.log_message() + '\n').encode('utf8'))
