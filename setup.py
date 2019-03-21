# Copyright 2019 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import setuptools
import fink_broker

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fink-broker",
    version=fink_broker.__version__,
    author="JulienPeloton",
    author_email="peloton@lal.in2p3.fr",
    description="Broker for Astronomy based on Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://fink-broker.readthedocs.io/en/latest/",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
    ],
    project_urls={
    'Documentation': "https://fink-broker.readthedocs.io/en/latest/",
    'Source': 'https://github.com/astrolabsoftware/fink-broker'
    },
)
