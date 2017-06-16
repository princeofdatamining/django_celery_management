from distutils.core import setup
from setuptools import find_packages

app_id = 'django_celery_management'
app_name = 'django_celery_management'
app_description = "Django celery utils."
install_requires = [
]

VERSION = __import__(app_id).__version__

CLASSIFIERS = [
    'Framework :: Django',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Topic :: Software Development',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
]

setup(
    name=app_name,
    description=app_description,
    version=VERSION,
    author="CNICG",
    author_email="princeofdatamining@gmail.com",
    license='BSD License',
    platforms=['OS Independent'],
    url="https://github.com/princeofdatamining/"+app_name,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=install_requires,
    classifiers=CLASSIFIERS,
)
