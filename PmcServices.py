import logging
logger = logging.getLogger(__name__)

import requests
import boto3
from botocore.handlers import disable_signing


class PmcRequestOpenAccessFulltext(object):

    # Setup a boto3 resource to allow access without authorication
    boto3_resource = boto3.resource('s3', region_name='us-east-1')
    boto3_resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

    pmc_aws_bucket_arn = 'pmc-oa-opendata'

    # API to retrieve ftp links for given PMCIDs
    oa_webservice_api_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi'
    request_header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
    }

    def __init__(self, pmc_id: str) -> None:

        self.pmc_id = pmc_id

    # Get the ftp link to download the fulltext pdf
    def get_ftp_link(self) -> str:

        req_payload = {'id': self.pmc_id}

        pmc_info_response = requests.get(self.oa_webservice_api_url,
                                         params=req_payload,
                                         headers= self.request_header)

        #TODO: process response and extract the needed ftp-links
        print(pmc_info_response.status_code)
        print(pmc_info_response.text)


    def download_fulltext_from_pmc_aws(self):

        pmc_bucket = self.boto3_resource.Bucket(self.pmc_aws_bucket_arn)

        pmc_key_prefixes = ['ao_comm', 'oa_noncomm', 'author_manuscript']

        for prefix in pmc_key_prefixes:
            try:
                pmc_bucket.download_file(f'{prefix}/xml/all/{self.pmc_id}.xml', f'{prefix}_{self.pmc_id}.xml')
                logger.
                break
            except:
                print(f'{prefix}/xml/all/{self.pmc_id}.xml not found')


        # for my_bucket_object in pmc_bucket.objects.filter(Prefix="oa_com"):
        #     print(my_bucket_object)


        # pmc_bucket.download_file('author_manuscript/xml/all/PMC10000017.xml', 'test.xml')
        # s3.Bucket(BUCKET_NAME).download_file(KEY, 'my_local_image.jpg')




pmc_request = PmcRequestOpenAccessFulltext('PMC5334499')
pmc_request.download_fulltext_from_pmc_aws()
# pmc_request.get_ftp_links()