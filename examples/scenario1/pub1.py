import asyncio

from broker_wizard.publisher import Publisher
from dict_generator.generator import (
    FrequencyBasedKeyGenerator,
    SchemaBasedDictGenerator,
)


def generate():
    keys = ["priority", "content", "type"]
    percentages = [0.3, 0.4, 0.3]
    schema = {
        "priority": {
            "type": "int",
            "values": ["high", "low", "medium"],
        },
        "content": {
            "type": "float",
            "values": [0, 1],  # Generate floats between 0 and 1
        },
        "type": {
            "type": "choice",
            "values": ["news", "sport", "business"],  # Choose randomly from 'a', 'b', 'c'
        },
    }

    dict_generator = SchemaBasedDictGenerator(schema)
    generator = FrequencyBasedKeyGenerator(10000, dict(zip(keys, percentages)))
    for dictionary in generator:
        # suppress the output
        # print(dict_generator.generate(dictionary))
        yield dict_generator.generate(dictionary)

async def main():
    publisher = Publisher("ws://localhost:8905/ws")

    # Publish a message
    for message in generate():
        await publisher.publish(message)

if __name__ == "__main__":
    asyncio.run(main())
