import sys
from typing import Tuple, List
import logging

import numpy as np
from pathlib import Path

import pandas as pd

from FilePathsUrls import FilePaths

from PublicationDownloadServices import QueryPmcAws, QueryPmcOaiPmh, QueryPmcFtp, QueryKopsPdf, QueryDoiHtml, \
    QueryShadowPdf, PublicationLog


def configure_logger(dl_log_file_path:Path) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        handlers=[logging.FileHandler(dl_log_file_path),
                                  logging.StreamHandler(sys.stdout),
                                  ]
                        )
    return logger


def create_directories_and_files(pub_dir:Path,
                                 log_subdir:str,
                                 dl_load_file_name:str,
                                 pub_log_file_name:str,
                                 pmc_xml_subdir:str,
                                 pdf_subdir:str,
                                 html_subdir:str) -> None:

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

    if not (pub_dir / pdf_subdir).is_dir():
        (pub_dir / pdf_subdir).mkdir()

    if not (pub_dir / html_subdir).is_dir():
        (pub_dir / html_subdir).mkdir()


def perform_downloads(pub_dir:Path,
                      pmc_subdir:str,
                      pdf_subdir:str,
                      html_subdir:str,
                      query_pmc=True,
                      query_kops=True,
                      query_doi=False,
                      query_shadow=True
                      ) -> List[PublicationLog]:

    pub_list = []

    # Use only the pubmed entries in xml format
    for pub_entry in pub_dir.glob('*.txt'):

        # create the Publication object
        current_publication = PublicationLog(pub_entry)

        # # If the paper is available in PMC AWS cloud as xml, get the XML, not needed, is subset of pmh
        # if current_publication.pmcid:
        # 		current_publication.is_pmc_xml_fulltext = pmc_request.download_fulltext_from_pmc_aws(pub_dir / pmc_subdir,
        # 																							 current_publication.pmcid)

        # else try to get it from the PMC AOI PMH API
        if current_publication.pmcid and query_pmc:
            current_publication.is_pmc_xml_fulltext = QueryPmcOaiPmh().download_fulltext_from_pmc_pmh(
                pub_dir / pmc_subdir,
                current_publication.pmcid)

        # # else try to get it from the PMC ftp server, not needed, can be used as backup
        # if current_publication.pmcid and query_pmc:
        # 	current_publication.is_pmc_xml_fulltext = ftp_request.download_pmc_fulltext_from_ftp(pub_dir / pmc_subdir,
        # 		current_publication.pmcid)

        # If we didn't get the fulltext by PMC, try to download the pdf from KOPS
        if not current_publication.is_pmc_xml_fulltext and query_kops:
            current_publication.is_pdf_fulltext = kops_request.download_fulltext_pdf_from_kops(pub_dir / pdf_subdir,
                                                                                               current_publication.pmid)

        # If nothing else worked try to get the html version of the file via the DOI, Warning scraper protection is hit really quick
        if (not current_publication.is_pmc_xml_fulltext and not current_publication.is_pdf_fulltext) and query_doi:
            current_publication.is_doi_fulltext = doi_request.download_fulltext_html_via_doi(pub_dir / html_subdir, current_publication.doi)

        # Currentl it is easier to try to retrieve fulltexts from shadow libraries
        if (not current_publication.is_pmc_xml_fulltext and not current_publication.is_pdf_fulltext) and query_shadow:
            current_publication.is_pdf_fulltext = shadow_request.download_fulltext_pdf_from_shadow(pub_dir / pdf_subdir,
                                                                                               current_publication.pmid, current_publication.doi)

        pub_list.append(current_publication)

    return pub_list



def create_publication_log(pub_dir:Path, log_subdir:str, pub_log_file_name:str, publications:List[PublicationLog]) -> None:

    headers = ['file_name', 'Pubmed Id', 'PMC Id', 'DOI', 'pmc_xml' , 'kops_pdf', 'doi_html', 'fac_acknowledged', 'fac_suspected']
    publication_data = [pub.list_publication_info() for pub in publications]
    publication_df = pd.DataFrame(publication_data, columns=headers)
    publication_df.to_csv(pub_dir / log_subdir / pub_log_file_name, index=False)
    return

def perform_download_from_shadow(pub_dir:Path,
                                 log_subdir:str,
                                 pdf_subdir:str,
                                 from_log=False,
                                 pub_log_filename='',
                                 pubmed_list=None,
                                 doi_list=None,
                                 ) -> None:

    def _retrieve_pubmed_doi_from_log() -> Tuple[List[str], List[str]]:
        # Read in the logs to see which publications couldn't be retrived via PMC or KOPS
        with open(pub_dir / log_subdir / pub_log_filename, 'r') as csv_file:
            pub_df = pd.read_csv(csv_file, header=0)

        # perform the NOR-operation on the PMC and KOPS entries, get the respective DOIs
        pmc_series = pub_df['pmc_xml']
        pdf_series = pub_df['kops_pdf']
        unretrieved_entries = ~pmc_series.combine(pdf_series, np.logical_or)
        unretrieved_dois = pub_df.loc[unretrieved_entries, ['DOI']].values.tolist()
        unretrieved_pubmed = pub_df.loc[unretrieved_entries, ['Pubmed Id']].values.tolist()

        return (unretrieved_pubmed, unretrieved_dois)

    if pubmed_list is None:
        pubmed_list = []

    if doi_list is None:
        doi_list = []

    if from_log:
        pubmed_list, doi_list = _retrieve_pubmed_doi_from_log()

    for pubmed_id, doi in zip(pubmed_list, doi_list):
        shadow_request.download_fulltext_pdf_from_shadow(pub_dir / pdf_subdir, pubmed_id[0], doi[0])
    return


if __name__ == '__main__':


    # Setup logging
    root_logger = configure_logger(FilePaths.download_log_fullpath)

    # Create directories and logs
    create_directories_and_files(FilePaths.publication_dir,
    							 FilePaths.log_subdirectory,
    							 FilePaths.download_log_file_name,
    							 FilePaths.publication_log_file_name,
    							 FilePaths.pmc_xmlfulltext_subdirectory,
    							 FilePaths.pdf_fulltext_subdirectory,
    							 FilePaths.html_fulltext_subdirectory,
    							 )

    # Setup download interfaces, if needed
    pmc_request = QueryPmcAws()
    ftp_request = QueryPmcFtp()
    kops_request = QueryKopsPdf()
    doi_request = QueryDoiHtml()
    shadow_request = QueryShadowPdf()


    publication_list = perform_downloads(FilePaths.publication_dir,
    									 FilePaths.pmc_xmlfulltext_subdirectory,
    									 FilePaths.pdf_fulltext_subdirectory,
    									 FilePaths.html_fulltext_subdirectory,
    									 )

    # # not needed, if performed before
    # perform_download_from_shadow(FilePaths.publication_dir,
    #                              FilePaths.log_subdirectory,
    #                              FilePaths.pdf_fulltext_subdirectory,
    #                              FilePaths.from_log=True,
    #                              FilePaths.pub_log_filename=publication_log_file_name,
    #                              )

    # Write the info
    create_publication_log(FilePaths.publication_dir,
    					   FilePaths.log_subdirectory,
    					   FilePaths.publication_log_file_name,
    					   FilePaths.publication_list,
    					   )