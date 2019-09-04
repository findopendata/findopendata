import magic
import cchardet as chardet


magic_encoding = magic.Magic(mime_encoding=True)

magic_parser = magic.Magic(mime=True, mime_encoding=True)


def guess_encoding_from_buffer(buf, chardet_threshold=0.5):
    result = chardet.detect(buf)
    if result["confidence"] < chardet_threshold:
        # Try magic
        encoding = magic_encoding.from_buffer(buf)
        if not encoding.startswith("unknown"):
            return encoding
    return result["encoding"]

