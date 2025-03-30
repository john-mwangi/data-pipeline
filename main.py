"""This will run the pipeline and serve the API."""

import uvicorn

from src.api import app
from src.pipeline import main

if __name__ == "__main__":
    main(use_local=False)
    uvicorn.run(app="main:app", port=12000, reload=True)
