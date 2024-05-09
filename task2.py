import string
import asyncio
from collections import defaultdict, Counter
from beaupy.spinners import Spinner
import logging

import httpx
from matplotlib import pyplot as plt

format = "%(threadName)s %(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


# Отримаємо текст за наданим посиланням
async def get_text(url):
    spinner = Spinner(text="Loading text from gutenberg...")
    spinner.start()
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        spinner.stop()
        if response.status_code == 200:
            return response.text
        else:
            return None


# Створимо функцію для видалення пунктуації і асинхронного Мапінгу
def map_function(text):
    text.translate(str.maketrans("", "", string.punctuation))
    words = text.split()
    return [(word, 1) for word in words]


# Створимо функцію для виконання Shuffle
def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


# І створимо асинхронну функцію для виконання Reduce
async def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


# Виконаємо пошук і формування списку слів за допомогою MapReduce
async def map_reduce(url):
    # Отримаємо текст з вебсайту
    text = await get_text(url)
    # Запускаємо Мапінг і обробку тексту

    spinner = Spinner(text="Running MapReduce...")
    spinner.start()
    mapped_result = map_function(text)

    # Виконаємо Shuffle використовуючи результати Мапінга
    shuffled_words = shuffle_function(mapped_result)

    # Зберемо разом отримані результати та підрахуємо частоти використання
    reduced_result = await asyncio.gather(
        *[reduce_function(values) for values in shuffled_words])

    # Повернемо результати розрахунків у main
    spinner.stop()
    return dict(reduced_result)


# Позначимо, що нам потрібні 10 найчастіших слів та побудуємо графік
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


if __name__ == '__main__':
    try:
        url = "https://gutenberg.net.au/ebooks01/0100021.txt"
        res = asyncio.run(map_reduce(url))
        visualize_top_words(res)
    except KeyboardInterrupt:
        logging.info('Bye!')