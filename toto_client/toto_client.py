import json
import os
import requests
import base64
from typing import Dict


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
        file_size = os.path.getsize(file_path)
        file_last_modified = os.path.getmtime(file_path)
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

        document_uuid = f"{file_name}-{file_size}-{file_last_modified}"

        values = {'fileContentBase64': file_content_base64, 'fileName': file_name, 'fileId': document_uuid}

        r = requests.post(f"{self.host}/upload_doc", json=values)
        if r.status_code != 200:
            raise ValueError(f"Failed uploading {r.status_code} {r.text}")
        return document_uuid

    def queue_job(self, job_name: str, job_arguments: Dict):
        job_identifier_items = [job_name]
        job_identifier_items += [x for k, v in job_arguments.items() for x in [k, v]]
        job_identifier = '--'.join(job_identifier_items)

        values = {"jobName": job_name, "inputArguments": json.dumps(job_arguments), "identifier": job_identifier}

        r = requests.get(f"{self.host}/queue_job", params=values)
        if r.status_code != 200:
            raise ValueError(f"Failed queuing job {r.status_code} {r.text}")
        return job_identifier

    def jobs(self):
        r = requests.get(f"{self.host}/jobs")
        if r.status_code != 200:
            raise ValueError(f"Failed querying for jobs {r.status_code} {r.text}")
        return r.json()

    def get_document(self, document_id):
        query = """query {
                      document(fileId:"%s") {
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
                      area(areaId:"%s") {
                        id
                        properties
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
