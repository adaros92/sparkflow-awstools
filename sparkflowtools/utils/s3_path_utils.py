

def is_valid_s3_path(path: str) -> bool:
    """Checks whether the given path is a valid S3 path or not

    :param path: a path to check
    :return: true if it's a valid path, false otherwise
    """
    return path.startswith("s3://")
