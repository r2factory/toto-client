import os
import requests


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

    def graphql(self, doc_id):
        query = """query {
                      getPdfDoc(id: "%s") {
                        totoDocLink {
                          pageLinks {
                            img
                            blockLinks {
                              cellLinks {
                                boundingBox
                                textLinks {
                                  content
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                """ % (doc_id,)
        data = {"query": query, "variables": None}
        r = requests.post(f"{self.host}/graphql", headers={'Authorization': f"Bearer {self.r2_token}", }, data=data)
        if not (200 <= r.status_code < 300):
            raise ConnectionError(r.text)
        return r.json()
