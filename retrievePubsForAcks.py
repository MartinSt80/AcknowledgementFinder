from typing import Tuple

import logging
logger = logging.getLogger(__name__)


from lxml import etree as ET
from pathlib import Path

import requests
from bs4 import BeautifulSoup


class PublicationLog:

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

		# Known working PubmedID, to habe a PMC entry
		# converter_response = requests.get(id_converter_url + '28729661')

		converter_response = requests.get(id_converter_url + self.pmid)
		if converter_response.status_code == 200:
			xml_root = ET.fromstring(converter_response.content)
			for record in xml_root.iter('record'):
				if not (record.get('status') == 'error'):
					return record.get('pmcid')


    # Try to get the fulltext from PMC as xml
	def retrieveXMLfrom_PMC(self) -> str|None:
		if self.pmcid:
			pmc_xml_text = requests.get(pmc_xmlfulltext_url + self.pmcid).text

			# Check if the record is fulltext
			pmc_entry_xml_root = ET.fromstring(pmc_xml_text)
			xml_instructions = pmc_entry_xml_root.xpath("//processing-instruction()")
			xml_instructions = [instr.text for instr in xml_instructions]
			if 'open_access' in xml_instructions:
				self.is_pmc_xml_fulltext = True
				return pmc_xml_text
			else:
				self.is_pmc_xml_fulltext = False
				return None
		else:
			return None

	# Get the pdf from KOPS
	def retrievePDFfromKOPS(self):
		soup = BeautifulSoup(requests.get(KOPS_url + '/discover?scope=%2F&query=' + self.pmid + '&submit=Los').text, 'lxml')
		links = soup.find_all('a')
		for link in links:
			if link['href'].startswith('/bitstream/handle'):
				self.is_kops_pdf_fulltext = True
				return requests.get(KOPS_url + link['href'], stream=True).content
		else:
			self.is_kops_pdf_fulltext = False
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


def create_directories_and_files() -> None:

	# Create, reset the log_file
	if not (pub_dir / log_subdirectory).is_dir():
		(pub_dir / log_subdirectory).mkdir()

	with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='w') as log_file:
		log_file.write('file_name,as_xml,as_pdf,fac_acknowledged,fac_suspected\n')

	with (pub_dir / log_subdirectory / download_log_file_name).open(mode='w') as log_file:
		log_file.write('# Download results for the publications\n')

	# Make sure the fulltext directories exist
	if not (pub_dir / pmc_xmlfulltext_subdirectory).is_dir():
		(pub_dir / pmc_xmlfulltext_subdirectory).mkdir()


if __name__ == '__main__':

	# Necessary URLs
	pmc_xmlfulltext_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id='
	id_converter_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids='
	KOPS_url = 'https://kops.uni-konstanz.de'
	DOI_url = 'https://doi.org/'

	# Directory to process
	pub_dir = Path('D:/PubTracker/test_pubs/2020')
	log_subdirectory = 'logs'
	pmc_xmlfulltext_subdirectory = 'pmc'

	publication_log_file_name = 'publications_log.csv'
	download_log_file_name = 'download_log.txt'

	# Create directories and logs
	create_directories_and_files()

	# Use only the pubmed entries in xml format
	for pub_entry in pub_dir.glob('*.txt'):

		# create the Publication object
		current_publication = PublicationLog(pub_entry)

		# If the paper is available in pmc, get the XML
		pmc_xml_text = current_publication.retrieveXMLfrom_PMC()
		if pmc_xml_text:
			if current_publication.is_pmc_xml_fulltext:
				file_name = pub_entry.stem + '_full.xml'
			else:
				file_name = pub_entry.stem + '_overview.xml'
			with (pub_dir / pmc_xmlfulltext_subdirectory / file_name).open(mode='w') as xml_text_file:
				xml_text_file.write(pmc_xml_text)

		# # If we didn't get the fulltext, try to download the pdf from KOPS
		# if not current_publication.is_pmc_xml_fulltext:
		# 	pdf_stream = current_publication.retrievePDFfromKOPS()
		# 	if pdf_stream:
		# 		with pub_entry.with_suffix('.pdf').open(mode='wb') as pdf_fulltext_file:
		# 			pdf_fulltext_file.write(pdf_stream)
		#
		# # If neither of the above methods worked, try to get pdf link by from 'https://doi.org/'
		# if not (current_publication.is_pmc_xml_fulltext or current_publication.is_kops_pdf_fulltext):
		# 	pdf_stream = current_publication.retrievePDFbyDOI()
		# 	if pdf_stream:
		# 		with pub_entry.with_suffix('.pdf').open(mode='wb') as pdf_fulltext_file:
		# 			pdf_fulltext_file.write(pdf_stream)

		# Output a status message
		print(current_publication.log_message())

		# Write the result to the log_files
		with (pub_dir / log_subdirectory / publication_log_file_name).open(mode='ab') as log_file:
			log_file.write(current_publication.log_csv_string().encode('utf8'))

		with (pub_dir / log_subdirectory / download_log_file_name).open(mode='ab') as log_file:
			log_file.write((current_publication.log_message() + '\n').encode('utf8'))
