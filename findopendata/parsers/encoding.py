import cchardet as chardet


def guess_encoding_from_buffer(buf, chardet_threshold=0.5):
    result = chardet.detect(buf)
    confidence = result.get("confidence")
    if not confidence or confidence < chardet_threshold:
        raise ValueError("Failed to detect encoding")
    return result["encoding"]
