import os
import magic
import cchardet as chardet


magic_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
        "magic.mgc")
magic_encoding = magic.Magic(mime_encoding=True, magic_file=magic_file)


def guess_encoding_from_buffer(buf, chardet_threshold=0.5):
    result = chardet.detect(buf)
    confidence = result.get("confidence")
    if not confidence or confidence < chardet_threshold:
        # Try magic
        encoding = magic_encoding.from_buffer(buf)
        if not encoding.startswith("unknown"):
            return encoding
    return result["encoding"]
