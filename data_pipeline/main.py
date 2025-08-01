"""This will run the pipeline and serve the API."""

import logging

import uvicorn
import yaml

from data_pipeline.src.api import app as app
from data_pipeline.src.pipeline import main as pipeline_main
from data_pipeline.src.utils import ROOT_DIR, config_path, setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main(api_only: bool, use_local: bool):
    if api_only:
        uvicorn.run(app="main:app", port=12000, reload=True)

    elif use_local:
        data_dir = ROOT_DIR / "data"
        paths = list(data_dir.glob("*"))
        for fp in paths:
            try:
                pipeline_main(use_local=use_local, file_path=fp)
            except Exception as e:
                logger.exception(e)

        uvicorn.run(app="main:app", port=12000, reload=True)

    else:
        with open(config_path, mode="r") as f:
            config = yaml.safe_load(f)

        urls = config["pipeline"]["urls"]

        for file, url in urls.items():
            try:
                pipeline_main(url=url)
            except Exception as e:
                logger.error(f"there was a problem reading the url {url}")

        uvicorn.run(app="main:app", port=12000, reload=True)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Run the pipeline and/or the API")
    parser.add_argument(
        "--use_local",
        default=False,
        action="store_true",
        help="Use the contents in the `data` folder to run the pipeline",
    )

    parser.add_argument(
        "--api_only",
        default=False,
        action="store_true",
        help="Run the API only and not the pipeline",
    )

    args = parser.parse_args()
    main(api_only=args.api_only, use_local=args.use_local)

# python main.py --use_local
# python main.py --api_only
# python main.py # run api with remote sources
