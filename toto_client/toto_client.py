import json
import os
import time
from io import StringIO
from typing import Optional, Dict, List
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

    def upload_file(self, file_path: str):
        file_name = os.path.basename(file_path)
        file_uuid = self.generate_file_uuid(file_path)
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

        values = {'fileContentBase64': file_content_base64, 'fileName': file_name, 'uuid': file_uuid}

        r = requests.post(f"{self.host}/upload_file", json=values)
        if r.status_code != 200:
            raise ValueError(f"Failed uploading {r.status_code} {r.text}")
        return r.json()['data_id']

    def generate_file_uuid(self, file_path):
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        file_last_modified = os.path.getmtime(file_path)
        file_uuid = f"{file_name}-{file_size}-{file_last_modified}"
        return file_uuid

    def queue_job(self, job_name: str, data_id: str, extra_arguments: Optional[Dict] = None, force=False):
        values = {"jobName": job_name, "dataId": data_id}
        if extra_arguments is not None:
            values["extraArguments"] = json.dumps(extra_arguments)
        if force:
            values["force"] = "True"

        r = requests.get(f"{self.host}/queue_job", params=values)
        if r.status_code != 200:
            raise ValueError(f"Failed queuing job {r.status_code} {r.text}")
        return r.json()["job_id"]

    def jobs(self, job_ids=None):
        values = None
        if job_ids is not None:
            values = {"jobIds": job_ids}
        r = requests.get(f"{self.host}/jobs", json=values)
        if r.status_code != 200:
            raise ValueError(f"Failed querying for jobs {r.status_code} {r.text}")
        return r.json()

    def wait_for_jobs_to_complete(self, job_ids: List[str], timeout: int = None, debug_prints: bool = False):
        if timeout is not None:
            raise NotImplementedError("timeout not implemented")
        job_ids = job_ids.copy()
        while True:
            if debug_prints:
                print(".", end="", flush=True)
            jobs = self.jobs(job_ids)
            for job_ids_index in range(len(job_ids)):
                job_id = job_ids[job_ids_index]
                if jobs[job_id]["status"] != "Running":
                    job_ids.pop(job_ids_index)
            if len(job_ids) == 0:
                break
            time.sleep(1)

    def get_data(self, data_id, tags=None, jobs=None):
        query = ""
        if tags is not None:
            if isinstance(tags, str):
                tags = [tags]
            for tag in tags:
                query += """
                    %s: datas(tagName: "%s") {
                      id
                      dataType
                      pageNumber
                      polygonRelativeToParent
                      tableCsv
                      text
                    }
                """ % (tag,tag)
        if jobs is not None:
            if isinstance(jobs, str):
                jobs = [jobs]
            for job in jobs:
                query += """
                    %s: datas(jobName: "%s") {
                      id
                      dataType
                      pageNumber
                      polygonRelativeToParent
                      tableCsv
                      text
                    }
                """ % (job,job)

        query = """query {
                      data(dataId:"%s") {
                        id
                        dataType
                        tableCsv
                        pageNumber
                        polygonRelativeToParent
                        text
                        %s
                      }
                    }
                """ % (data_id, query)
        data = {"query": query, "variables": None}
        headers = {
            'Authorization': f"Bearer {self.r2_token}",
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        r = requests.post(f"{self.host}/graphql", headers=headers, json=data)
        if not (200 <= r.status_code < 300):
            raise ConnectionError(r.text)
        return r.json()['data']['data']

    def detect_table(self, data_id):
        data = self.get_data(data_id, jobs=["pageimg2tablebox_base64"])
        assert data["dataType"] == "image"
        job_identifier = self.queue_job("pageimg2tablebox_base64", data_id=data_id)
        self.wait_for_jobs_to_complete([job_identifier])
        data = self.get_data(data_id, jobs=["pageimg2tablebox_base64"])
        return data["pageimg2tablebox_base64"]

    def extract_table(self, data_id):
        data = self.get_data(data_id, jobs=["hf_recognise_table_base64"])
        assert data["dataType"] == "image"
        job_identifier = self.queue_job("hf_recognise_table_base64", data_id=data_id)
        self.wait_for_jobs_to_complete([job_identifier])

        data = self.get_data(data_id, jobs=["hf_recognise_table_base64"])
        table_data_id = data["hf_recognise_table_base64"][0]["id"]
        return table_data_id

    def get_df_from_table(self, table_data_id):
        data = self.get_data(table_data_id)
        assert data["dataType"] == "dataframe"
        csvString = data["tableCsv"]
        csvStringIO = StringIO(csvString)
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        return df

