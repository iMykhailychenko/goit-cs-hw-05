import argparse
import asyncio

import aiopath
import aioshutil
import logging
from beaupy.spinners import Spinner

format = "%(threadName)s %(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

parser = argparse.ArgumentParser(description="Sorting files")
parser.add_argument("--source", "-s", required=True, help="Source dir")
parser.add_argument("--output", "-o", help="Output dir", default="dist")
args = vars(parser.parse_args())

source_dir = aiopath.AsyncPath(args["source"])
output_dir = aiopath.AsyncPath(args["output"])


async def read_folder(path: aiopath.AsyncPath):
    async for file in path.iterdir():
        if await file.is_dir():
            await read_folder(file)
        else:
            await copy_file(file)


async def copy_file(file: aiopath.AsyncPath):
    folder = output_dir / file.suffix[1:]
    try:
        await folder.mkdir(parents=True, exist_ok=True)
        await aioshutil.copyfile(file, folder / file.name)
    except OSError as e:
        logging.error("Error while copying file: {e}")


if __name__ == "__main__":
    spinner = Spinner()
    spinner.start()
    asyncio.run(read_folder(source_dir))
    spinner.stop()
    logging.info(f"Done ✅")
