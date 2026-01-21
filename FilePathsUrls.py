from dataclasses import dataclass
from pathlib import Path


@dataclass
class FilePaths:

   publication_dir = Path('D:/PubTracker/test_pubs/2020')

   log_subdirectory = 'logs'
   pmc_xmlfulltext_subdirectory = 'pmc_xml_fulltexts'
   pdf_fulltext_subdirectory = 'pdf_fulltexts'
   html_fulltext_subdirectory = 'html_fulltexts'

   publication_log_file_name = 'publications_log.csv'
   download_log_file_name = 'download_log.txt'

   publication_log_fullpath = publication_dir / log_subdirectory / publication_log_file_name
   download_log_fullpath = publication_dir / log_subdirectory / download_log_file_name


@dataclass
class Urls:

   pubmed_to_pmc_id_converter_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids='
   pmc_pmh_base_url = 'https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/'

   oa_webservice_api_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi'
   pmc_ftp_url = 'ftp.ncbi.nlm.nih.gov'

   kops_search_base_url = 'https://kops.uni-konstanz.de/search?spc.page=1&view=list&query='
   kops_download_base_url = 'https://kops.uni-konstanz.de/bitstreams/'

   doi_base_url = 'https://doi.org/'

   shadow_lib_base_url = 'https://www.wellesu.com/'