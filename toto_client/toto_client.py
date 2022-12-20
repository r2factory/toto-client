import json
import os
import time
from io import StringIO
from typing import Dict, List
import base64
import requests
import pandas as pd

class TotoClient:
    def __init__(self, host=None, login_required=True):
        if host is None:
            host = os.environ.get('TOTO_HOST', "https://toto.dev.r2-factory.com")
        self.host = host

        if "LOGIN_REQUIRED" in os.environ:
            login_required = os.environ["LOGIN_REQUIRED"] != "False"

        if login_required:
            import google.auth
            import google.auth.transport.requests
            creds, project = google.auth.default(scopes=['https://www.googleapis.com/auth/userinfo.email'])

            # creds.valid is False, and creds.token is None
            # Need to refresh credentials to populate those

            auth_req = google.auth.transport.requests.Request()
            creds.refresh(auth_req)

            r = requests.get("https://r2-auth.dev.r2-factory.com/token", headers={'Authorization': f"Bearer {creds.token}"})
            if not (200 <= r.status_code < 300):
                raise ConnectionError(r.text)
            self.r2_token = r.text
        else:
            self.r2_token = "no_token"

    def upload_document(self, file_path: str):
        file_name = os.path.basename(file_path)
        document_uuid = self.generate_document_uuid(file_path)
        with open(file_path, "rb") as file:
            file_content_base64 = base64.b64encode(file.read()).decode("utf-8")

        if file_path.endswith(".pdf"):
            file_content_base64 = "data:application/pdf;base64," + file_content_base64
        if file_path.endswith(".png"):
            file_content_base64 = "data:image/png;base64," + file_content_base64
        if file_path.endswith(".jpg") or file_path.endswith(".jpeg"):
            file_content_base64 = "data:image/jpeg;base64," + file_content_base64
        if file_path.endswith(".tif") or file_path.endswith(".tiff"):
            file_content_base64 = "data:image/tiff;base64," + file_content_base64


        values = {'fileContentBase64': file_content_base64, 'fileName': file_name, 'fileId': document_uuid}

        r = requests.post(f"{self.host}/upload_doc", json=values)
        if r.status_code != 200:
            raise ValueError(f"Failed uploading {r.status_code} {r.text}")
        return document_uuid

    def generate_document_uuid(self, file_path):
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        file_last_modified = os.path.getmtime(file_path)
        document_uuid = f"{file_name}-{file_size}-{file_last_modified}"
        return document_uuid

    def queue_job(self, job_name: str, job_arguments: Dict, force=False):
        values = {"jobName": job_name, "inputArguments": json.dumps(job_arguments)}
        if force:
            values["force"] = "True"

        r = requests.get(f"{self.host}/queue_job", params=values)
        if r.status_code != 200:
            raise ValueError(f"Failed queuing job {r.status_code} {r.text}")
        return r.json()["job_identifier"]

    def jobs(self, job_identifiers=None):
        values = None
        if job_identifiers is not None:
            values = {"jobIdentifiers": job_identifiers}
        r = requests.get(f"{self.host}/jobs", json=values)
        if r.status_code != 200:
            raise ValueError(f"Failed querying for jobs {r.status_code} {r.text}")
        return r.json()

    def wait_for_jobs_to_complete(self, job_identifiers: List[str], timeout: int = None, debug_prints: bool = False):
        if timeout is not None:
            raise NotImplementedError("timeout not implemented")
        job_identifiers = job_identifiers.copy()
        while True:
            if debug_prints:
                print(".", end="", flush=True)
            jobs = self.jobs(job_identifiers)
            for job_identifier_index in range(len(job_identifiers)):
                job_identifier = job_identifiers[job_identifier_index]
                if jobs[job_identifier]["status"] != "Running":
                    job_identifiers.pop(job_identifier_index)
            if len(job_identifiers) == 0:
                break
            time.sleep(1)

    def get_document(self, document_id):
        query = """query {
                      document(documentId:"%s") {
                        id
                        fileId
                        areas {
                          id
                          properties
                        }
                      }
                    }
                """ % (document_id,)
        data = {"query": query, "variables": None}
        headers = {
            'Authorization': f"Bearer {self.r2_token}",
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        r = requests.post(f"{self.host}/graphql", headers=headers, json=data)
        if not (200 <= r.status_code < 300):
            raise ConnectionError(r.text)
        return r.json()['data']['document']

    def get_area(self, area_id):
        query = """query {
                     area(areaId: "%s") {
                       id
                       properties
                       areas {
                         id
                         properties
                         polygonParent
                       }
                     }
                   }
                """ % (area_id,)
        data = {"query": query, "variables": None}
        headers = {
            'Authorization': f"Bearer {self.r2_token}",
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        r = requests.post(f"{self.host}/graphql", headers=headers, json=data)
        if not (200 <= r.status_code < 300):
            raise ConnectionError(r.text)
        return r.json()['data']['area']

    def detect_table(self, area_id):
        job_identifier = self.queue_job("pageimg2tablebox_base64", {"area_id": area_id})
        self.wait_for_jobs_to_complete([job_identifier])
        area = self.get_area(area_id)
        return area["areas"]

    def extract_table(self, area_id):
        job_identifier = self.queue_job("hf_recognise_table_base64", {"area_id": area_id})
        self.wait_for_jobs_to_complete([job_identifier])

        area = self.get_area(area_id)
        csvString = json.loads(area["properties"])["table"]
        csvStringIO = StringIO(csvString)
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        return df