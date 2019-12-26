import cchardet as chardet


def guess_encoding_from_buffer(buf, chardet_threshold=0.5):
    result = chardet.detect(buf)
    confidence = result.get("confidence")
    if not confidence or confidence < chardet_threshold:
        raise ValueError("Failed to detect encoding")
    encoding = result["encoding"]
    return encoding


def guess_encoding_from_stream(stream, chunk_size=4096, chardet_threshold=0.5):
    detector = chardet.UniversalDetector()
    chunk = stream.read(chunk_size)
    while not detector.done and chunk:
        detector.feed(chunk)
        chunk = stream.read(chunk_size)
    detector.close()
    result = detector.result
    confidence = result.get("confidence")
    if not confidence or confidence < chardet_threshold:
        raise ValueError("Failed to detect encoding")
    encoding = result["encoding"]
    return encoding
