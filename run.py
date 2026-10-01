"""Start the local cluster manager:  python run.py"""
import uvicorn

from comfy_cluster.config import settings

if __name__ == "__main__":
    print(f"ComfyUI Cluster dashboard: http://localhost:{settings.port}/cluster")
    print(f"Node list file: {settings.data_file}")
    uvicorn.run("comfy_cluster.app:app", host=settings.host, port=settings.port, log_level="info")
