from pathlib import Path

from event_model.documents import ALL_DOCUMENTS
from event_model.documents.generate.typeddict_to_schema import typeddict_to_schema


def main():
    schema_dir = Path(__file__).parent.parent.parent / "schemas"
    for document in ALL_DOCUMENTS:
        typeddict_to_schema(document, schema_dir)


if __name__ == "__main__":
    main()
