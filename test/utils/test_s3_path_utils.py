from sparkflowtools.utils import s3_path_utils


test_path_1 = "s3://some-bucket/some-prefix/"
test_path_2 = "s3://some-bucket/some-prefix/some_file.txt"
test_path_3 = "not an s3 path"


def test_is_valid_s3_path():
    """Tests s3_path_utils.is_valid_s3_path function"""
    assert (s3_path_utils.is_valid_s3_path(test_path_1) and s3_path_utils.is_valid_s3_path(test_path_2)
            and not s3_path_utils.is_valid_s3_path(test_path_3))
