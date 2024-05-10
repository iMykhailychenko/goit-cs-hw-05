import asyncio
import logging
import string
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

import httpx
from beaupy.spinners import Spinner
from matplotlib import pyplot as plt

format = "%(threadName)s %(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


async def get_text(url):
    spinner = Spinner(text="Loading text from gutenberg...\n")
    spinner.start()
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        spinner.stop()
        if response.status_code == 200:
            return response.text.translate(str.maketrans("", "", string.punctuation))
        else:
            return None


def map_function(word):
    return word, 1


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


async def map_reduce(url):
    text = await get_text(url)
    
    spinner = Spinner(text="Running MapReduce...\n")
    spinner.start()
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, text))

    shuffled_values = shuffle_function(mapped_values)

    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    spinner.stop()
    return dict(reduced_values)


def visualize_top_words(result, top_n=10):
    top_words = Counter(result).most_common(top_n)
    # Розділимо дані на слова та їх частоти
    words, counts = zip(*top_words)
    # Створимо графік
    plt.figure(figsize=(12, 9))
    plt.barh(words, counts)
    plt.xlabel("Частота використання")
    plt.ylabel("Слова")
    plt.title("10 найчастіше вживаних у тексті слів")
    plt.gca().invert_yaxis()
    plt.show()


if __name__ == "__main__":
    try:
        url = "https://gutenberg.net.au/ebooks01/0100021.txt"
        res = asyncio.run(map_reduce(url))
        visualize_top_words(res)
    except KeyboardInterrupt:
        logging.info("Bye!")
