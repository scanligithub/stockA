from huggingface_hub import HfApi
import os

class HFManager:
    def __init__(self, token, repo_id):
        self.api = HfApi(token=token)
        self.repo_id = repo_id
        
    def upload_file(self, local_path, path_in_repo):
        print(f"🚀 Uploading {local_path} to HF: {path_in_repo}...")
        self.api.upload_file(
            path_or_fileobj=local_path,
            path_in_repo=path_in_repo,
            repo_id=self.repo_id,
            repo_type="dataset"
        )
