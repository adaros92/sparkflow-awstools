rm -rf dist build
python3 setup.py sdist bdist_wheel
twine upload dist/*
rm -rf ./test_dir* ./feature_data_* .pytest_cache *.egg*