from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = ['cloudml-hypertune']

setup(
    name='text_class_hp_tuning',
    version='1.0',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=True,
    description='HP tuning for multiclass text classification using SGD'
    )
